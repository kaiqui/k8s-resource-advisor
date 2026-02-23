#!/usr/bin/env python3
"""
main.py — k8s Resource Advisor CLI

Fluxo principal (sem configuração de apps):
  1. GitHub descobre todos os repos da org com deploy.yaml
  2. Extrai do deploy.yaml: DD_SERVICE, namespace, resources, HPA atual
  3. Extrai do Dockerfile: Python version
  4. Extrai de pyproject.toml/requirements.txt: framework, dependências
  5. Datadog coleta métricas reais das últimas N semanas por serviço
  6. Calcula resources e HPA recomendados
  7. Gera YAMLs comentados + relatório HTML

Uso:
  python main.py run --weeks 4 --output ./output
  python main.py run --filter api --weeks 2        # só repos com "api" no nome
  python main.py run --repo minha-org/minha-api    # repo específico
  python main.py generate                          # regenera YAMLs do cache
  python main.py list                              # lista apps descobertas sem rodar
"""

import dataclasses
import json
import os
import sys
from pathlib import Path
from typing import Optional

try:
    import click
    import yaml
except ImportError:
    print("Instale: pip install click pyyaml rich python-dotenv")
    sys.exit(1)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    from rich.console import Console
    from rich.table import Table
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
    console = Console()
except ImportError:
    class _C:
        def print(self, m, **_): print(m)
    console = _C()
    Table = None
    Progress = None

CONFIG_PATH = Path("src/config/settings.yaml")


# ─────────────────────── Config ────────────────────────────────────────────

def load_config() -> dict:
    if not CONFIG_PATH.exists():
        console.print(
            f"[red]Config não encontrado: {CONFIG_PATH}[/red]\n"
            "  Copie: cp config/settings.example.yaml config/settings.yaml"
        )
        sys.exit(1)
    with open(CONFIG_PATH) as f:
        raw = f.read()
    for k, v in os.environ.items():
        raw = raw.replace(f"${{{k}}}", v)
    return yaml.safe_load(raw)


def apply_overrides(app_config: dict, config: dict) -> dict:
    """Aplica overrides do settings.yaml para um app_config específico."""
    overrides = config.get("overrides", {}) or {}
    app_name = app_config.get("name") or app_config.get("dd_service", "")
    if app_name in overrides and overrides[app_name]:
        app_config = {**app_config, **overrides[app_name]}
    return app_config


# ─────────────────────── Discovery ─────────────────────────────────────────

def discover_apps(config: dict, repo_filter: Optional[str] = None) -> list[dict]:
    """
    Descobre todos os apps da org via GitHub e converte em app_configs.
    Retorna lista ordenada por nome, excluindo repos sem DD_SERVICE.
    """
    from collectors.github import GitHubCollector, github_info_to_app_config

    gh_cfg = config.get("github", {})
    token = gh_cfg.get("token") or os.environ.get("GITHUB_TOKEN")
    org = gh_cfg.get("org", "")
    if not org:
        console.print("[red]github.org não configurado no settings.yaml[/red]")
        sys.exit(1)

    gc = GitHubCollector(
        token=token,
        base_url=gh_cfg.get("base_url"),
    )

    console.print(f"\n[bold]Descobrindo apps em[/bold] [cyan]{org}[/cyan]...")

    apps_info = gc.discover_apps(
        org=org,
        repo_filter=repo_filter or gh_cfg.get("repo_filter"),
        skip_archived=gh_cfg.get("skip_archived", True),
        skip_forks=gh_cfg.get("skip_forks", True),
        max_repos=gh_cfg.get("max_repos", 500),
    )

    app_configs = []
    skipped = []

    for info in apps_info:
        if info.skipped:
            skipped.append(info)
            continue
        cfg = github_info_to_app_config(info)
        cfg = apply_overrides(cfg, config)
        app_configs.append(cfg)

    console.print(
        f"  [green]✓ {len(app_configs)} apps com deploy.yaml + DD_SERVICE[/green]"
    )
    if skipped:
        console.print(
            f"  [yellow]⚠ {len(skipped)} repos pulados (sem DD_SERVICE):[/yellow] "
            + ", ".join(i.repo.split("/")[-1] for i in skipped[:5])
            + ("..." if len(skipped) > 5 else "")
        )

    return sorted(app_configs, key=lambda x: x["name"])


def discover_single_repo(owner: str, repo: str, config: dict) -> dict:
    """Descobre um único repositório por owner/repo."""
    from collectors.github import GitHubCollector, github_info_to_app_config

    gh_cfg = config.get("github", {})
    token = gh_cfg.get("token") or os.environ.get("GITHUB_TOKEN")
    gc = GitHubCollector(token=token, base_url=gh_cfg.get("base_url"))

    console.print(f"\n[bold]Coletando GitHub:[/bold] [cyan]{owner}/{repo}[/cyan]")
    info = gc.collect(owner=owner, repo=repo)

    if not info.dd_service:
        console.print(
            "[red]DD_SERVICE não encontrado no deploy.yaml deste repositório.[/red]"
        )
        sys.exit(1)

    cfg = github_info_to_app_config(info)
    return apply_overrides(cfg, config)


# ─────────────────────── Pipeline ──────────────────────────────────────────

def run_pipeline(
    app_config: dict,
    global_config: dict,
    weeks: int,
    output_dir: str,
    use_cache: bool = True,
) -> tuple:
    from analyzers.profiler import AppProfiler
    from calculators.hpa import HPACalculator
    from calculators.resources import ResourceCalculator
    from collectors.datadog import DatadogCollector
    from reporters.yaml_gen import write_manifests

    collection_cfg = global_config.get("collection", {})
    thresholds = global_config.get("thresholds", {})
    dd_cfg = global_config["datadog"]
    cache_dir = collection_cfg.get("cache_dir", f"{output_dir}/data") if use_cache else None

    # Garante env do Datadog
    if not app_config.get("dd_env"):
        app_config["dd_env"] = global_config.get("datadog", {}).get("env", "production")

    # Coleta Datadog
    collector = DatadogCollector(
        api_key=dd_cfg["api_key"],
        app_key=dd_cfg["app_key"],
        site=dd_cfg.get("site", "datadoghq.com"),
    )
    metrics = collector.collect(
        app_config=app_config,
        weeks=weeks,
        interval_seconds=collection_cfg.get("interval_seconds", 300),
        cache_dir=cache_dir,
    )

    # Sobrescreve resources atuais com valores do deploy.yaml (fonte de verdade)
    for field, attr in [
        ("_current_cpu_request_cores",  "current_cpu_request_cores"),
        ("_current_cpu_limit_cores",    "current_cpu_limit_cores"),
        ("_current_mem_request_bytes",  "current_mem_request_bytes"),
        ("_current_mem_limit_bytes",    "current_mem_limit_bytes"),
    ]:
        if app_config.get(field):
            setattr(metrics, attr, app_config[field])

    # Propaga framework e python_version para as métricas
    metrics.framework     = app_config.get("framework", metrics.framework)
    metrics.python_version = app_config.get("python_version", metrics.python_version)

    # Análise + cálculos
    analyzed  = AppProfiler().analyze(metrics)
    resources = ResourceCalculator(thresholds).calculate(analyzed)
    current_hpa = {
        "min":        app_config.get("_current_hpa_min"),
        "max":        app_config.get("_current_hpa_max"),
        "cpu_target": app_config.get("_current_hpa_cpu_target"),
    }
    hpa = HPACalculator(thresholds).calculate(analyzed, resources, current_hpa=current_hpa)

    # Geração de YAML
    github_info = app_config.get("_github_info")
    write_manifests(analyzed, resources, hpa, output_dir, github_info=github_info)

    return analyzed, resources, hpa


def run_batch(
    app_configs: list[dict],
    config: dict,
    weeks: int,
    output_dir: str,
    use_cache: bool = True,
) -> list[tuple]:
    """Executa o pipeline para uma lista de apps com progress bar."""
    results = []
    failed = []

    for i, app_cfg in enumerate(app_configs, 1):
        name = app_cfg["name"]
        console.print(
            f"\n[bold blue][{i}/{len(app_configs)}] {name}[/bold blue] "
            f"[dim]({app_cfg.get('dd_service')} | "
            f"py{app_cfg.get('python_version','?')} | "
            f"{app_cfg.get('framework','?')})[/dim]"
        )
        try:
            result = run_pipeline(app_cfg, config, weeks, output_dir, use_cache)
            analyzed, resources, hpa = result
            console.print(
                f"  [green]✓[/green] "
                f"cpu {resources.cpu_request_str}/{resources.cpu_limit_str}  "
                f"mem {resources.mem_request_str}/{resources.mem_limit_str}  "
                f"hpa {hpa.min_replicas}/{hpa.max_replicas}@{hpa.cpu_target_utilization}%"
            )
            if analyzed.warnings:
                for w in analyzed.warnings:
                    c = "red" if w.severity == "critical" else "yellow"
                    console.print(f"  [{c}]⚠ {w.code}[/{c}] {w.message}")
            results.append(result)
        except InsufficientDataError as exc:
            console.print(f"  [red]✗ Dados insuficientes:[/red]")
            for line in str(exc).splitlines():
                console.print(f"    [yellow]{line}[/yellow]")
            failed.append(name)
        except Exception as exc:
            console.print(f"  [red]✗ Erro inesperado: {exc}[/red]")
            import traceback; traceback.print_exc()
            failed.append(name)

    if failed:
        console.print(f"\n[red]Falhas ({len(failed)}):[/red] {', '.join(failed)}")

    return results


# ─────────────────────── CLI ───────────────────────────────────────────────

@click.group()
def cli():
    """k8s Resource Advisor — recursos e HPA via GitHub + Datadog, sem configuração manual."""
    pass


@cli.command()
@click.option("--weeks",  default=4,  show_default=True, help="Janela de coleta em semanas")
@click.option("--output", default="./output", show_default=True)
@click.option("--filter", "repo_filter", default=None,
              help="Filtra repos pelo nome (substring). Ex: --filter api")
@click.option("--repo",   default=None,
              help="Analisa um repo específico: owner/repo ou só repo (usa org do config)")
@click.option("--no-cache", is_flag=True, help="Ignora cache e re-coleta tudo")
def run(weeks, output, repo_filter, repo, no_cache):
    """Pipeline completo: GitHub discovery → Datadog → análise → YAML + relatório."""
    config = load_config()

    if repo:
        # Repo específico
        if "/" in repo:
            owner, repo_name = repo.split("/", 1)
        else:
            owner = config.get("github", {}).get("org", "")
            repo_name = repo
        app_configs = [discover_single_repo(owner, repo_name, config)]
    else:
        # Descoberta automática da org
        app_configs = discover_apps(config, repo_filter=repo_filter)

    if not app_configs:
        console.print("[yellow]Nenhuma app encontrada.[/yellow]")
        sys.exit(0)

    console.print(
        f"\n[bold]Iniciando análise:[/bold] "
        f"{len(app_configs)} apps | {weeks} semanas\n"
    )

    results = run_batch(app_configs, config, weeks, output, use_cache=not no_cache)

    if results:
        from reporters.report import generate_report
        report_path = Path(output) / "report.html"
        generate_report(results, str(report_path), weeks=weeks)
        console.print(f"\n[bold green]Relatório:[/bold green] {report_path}")
        _print_summary(results)


@cli.command()
@click.option("--output", default="./output", show_default=True)
@click.option("--filter", "repo_filter", default=None)
def list(output, repo_filter):
    """Lista apps que seriam descobertas, sem coletar métricas."""
    config = load_config()
    app_configs = discover_apps(config, repo_filter=repo_filter)

    if Table:
        table = Table(title=f"\n{len(app_configs)} apps descobertas", header_style="bold")
        table.add_column("Service (DD_SERVICE)")
        table.add_column("Namespace")
        table.add_column("Python")
        table.add_column("Framework")
        table.add_column("CPU atual")
        table.add_column("Mem atual")
        table.add_column("HPA min/max")
        table.add_column("Repo")

        for c in app_configs:
            gh = c.get("_github_info")
            table.add_row(
                c["dd_service"],
                c.get("namespace", "-"),
                c.get("python_version", "-"),
                c.get("framework", "-"),
                f"{c.get('_current_cpu_request','-')}/{c.get('_current_cpu_limit','-')}",
                f"{c.get('_current_mem_request','-')}/{c.get('_current_mem_limit','-')}",
                f"{c.get('_current_hpa_min','-')}/{c.get('_current_hpa_max','-')}",
                gh.repo if gh else "-",
            )
        console.print(table)
    else:
        for c in app_configs:
            print(f"{c['dd_service']:30} py={c.get('python_version','?'):5} fw={c.get('framework','?')}")


@cli.command()
@click.option("--output", default="./output", show_default=True)
@click.option("--filter", "name_filter", default=None, help="Filtra por nome de app")
def generate(output, name_filter):
    """Regenera YAMLs e relatório a partir do cache — sem re-coletar."""
    from collectors.datadog import AppMetrics
    from collectors.github import GitHubAppInfo, enrich_app_config
    from analyzers.profiler import AppProfiler
    from calculators.hpa import HPACalculator
    from calculators.resources import ResourceCalculator
    from reporters.yaml_gen import write_manifests
    from reporters.report import generate_report

    config = load_config()
    thresholds = config.get("thresholds", {})
    cache_dir = Path(output) / "data"

    cache_files = sorted(cache_dir.glob("*-metrics.json"))
    if not cache_files:
        console.print(f"[red]Nenhum cache em {cache_dir}. Rode 'run' primeiro.[/red]")
        sys.exit(1)

    results = []
    for metrics_path in cache_files:
        name = metrics_path.stem.replace("-metrics", "")
        if name_filter and name_filter not in name:
            continue

        with open(metrics_path) as f:
            metrics = AppMetrics(**json.load(f))

        app_cfg = {"name": name, "dd_service": metrics.dd_service or name,
                   "dd_env": metrics.dd_env}

        # Carrega cache do GitHub se existir
        gh_path = cache_dir / f"{name}-github.json"
        github_info = None
        if gh_path.exists():
            with open(gh_path) as f:
                raw = json.load(f)
            valid = set(GitHubAppInfo.__dataclass_fields__)
            github_info = GitHubAppInfo(**{k: v for k, v in raw.items() if k in valid})
            app_cfg = enrich_app_config(app_cfg, github_info)
            for field, attr in [
                ("_current_cpu_request_cores",  "current_cpu_request_cores"),
                ("_current_cpu_limit_cores",    "current_cpu_limit_cores"),
                ("_current_mem_request_bytes",  "current_mem_request_bytes"),
                ("_current_mem_limit_bytes",    "current_mem_limit_bytes"),
            ]:
                if app_cfg.get(field):
                    setattr(metrics, attr, app_cfg[field])

        app_cfg = apply_overrides(app_cfg, config)
        analyzed  = AppProfiler().analyze(metrics)
        resources = ResourceCalculator(thresholds).calculate(analyzed)
        current_hpa = {
            "min":        app_cfg.get("_current_hpa_min"),
            "max":        app_cfg.get("_current_hpa_max"),
            "cpu_target": app_cfg.get("_current_hpa_cpu_target"),
        }
        hpa = HPACalculator(thresholds).calculate(analyzed, resources, current_hpa=current_hpa)
        write_manifests(analyzed, resources, hpa, output, github_info=github_info)
        results.append((analyzed, resources, hpa))
        console.print(f"[green]✓[/green] {name}")

    if results:
        report_path = Path(output) / "report.html"
        generate_report(results, str(report_path))
        console.print(f"\n[bold green]Relatório:[/bold green] {report_path}")


def _print_summary(results):
    if not Table:
        for a, r, h in results:
            print(f"{a.app_name}: {r.cpu_request_str}/{r.cpu_limit_str} {r.mem_request_str}/{r.mem_limit_str} hpa={h.min_replicas}/{h.max_replicas}")
        return

    t = Table(title="\nResumo", header_style="bold", show_lines=False)
    t.add_column("App",       style="bold", no_wrap=True)
    t.add_column("Perfil",    style="dim")
    t.add_column("CPU req→",  justify="right")
    t.add_column("CPU lim→",  justify="right")
    t.add_column("Mem req→",  justify="right")
    t.add_column("Mem lim→",  justify="right")
    t.add_column("HPA",       justify="center")
    t.add_column("Δ cpu_req", justify="right")
    t.add_column("Δ mem_req", justify="right")
    t.add_column("Status")

    for analyzed, resources, hpa in results:
        delta_cpu = f"{resources.cpu_request_delta_pct:+.0f}%"
        delta_mem = f"{resources.mem_request_delta_pct:+.0f}%"
        status = " ".join(
            f"[{'red' if w.severity=='critical' else 'yellow'}]{w.code}[/]"
            for w in analyzed.warnings
        ) or "[green]OK[/green]"
        t.add_row(
            analyzed.app_name,
            analyzed.app_profile.value.replace("_", " "),
            resources.cpu_request_str,
            resources.cpu_limit_str,
            resources.mem_request_str,
            resources.mem_limit_str,
            f"{hpa.min_replicas}/{hpa.max_replicas}@{hpa.cpu_target_utilization}%",
            delta_cpu,
            delta_mem,
            status,
        )
    console.print(t)


if __name__ == "__main__":
    cli()