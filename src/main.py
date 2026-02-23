#!/usr/bin/env python3
"""
k8s-audit — auditoria de configurações Kubernetes via GitHub

Uso:
  python main.py scan                          # toda a org
  python main.py scan --filter payments        # filtra por substring
  python main.py scan --repo service-billing   # um repo específico
  python main.py scan --no-cache               # ignora cache JSON
"""

import json
import os
import sys
from pathlib import Path

try:
    import click
    import yaml
except ImportError:
    print("pip install click pyyaml rich")
    sys.exit(1)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    from rich.console import Console
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, MofNCompleteColumn
    console = Console()
except ImportError:
    class _C:
        def print(self, m, **_): print(m)
    console = _C()
    Progress = None

CONFIG_PATH = Path("config/settings.yaml")


# ── Config ────────────────────────────────────────────────────────────────────

def load_config() -> dict:
    if not CONFIG_PATH.exists():
        console.print(f"[red]Config não encontrado: {CONFIG_PATH}[/red]")
        console.print("  Copie: cp config/settings.example.yaml config/settings.yaml")
        sys.exit(1)
    with open(CONFIG_PATH) as f:
        raw = f.read()
    for k, v in os.environ.items():
        raw = raw.replace(f"${{{k}}}", v)
    return yaml.safe_load(raw)


# ── CLI ───────────────────────────────────────────────────────────────────────

@click.group()
def cli():
    """k8s-audit — auditoria de configurações K8s via GitHub."""
    pass


@cli.command()
@click.option("--filter", "repo_filter", default=None,
              help="Filtra repos por substring no nome")
@click.option("--repo", default=None,
              help="Analisa apenas este repo (nome ou org/nome)")
@click.option("--output", default="./output/report.html", show_default=True)
@click.option("--cache-dir", default="./output/cache", show_default=True)
@click.option("--no-cache", is_flag=True, help="Ignora cache e re-coleta do GitHub")
def scan(repo_filter, repo, output, cache_dir, no_cache):
    """Varre o GitHub, coleta configurações K8s e gera relatório HTML."""
    from collectors.github import GitHubAuditCollector, ServiceAudit
    from analyzers.stats import analyze
    from reporters.html_report import generate_report

    config  = load_config()
    gh_cfg  = config.get("github", {})
    token   = gh_cfg.get("token") or os.environ.get("GITHUB_TOKEN")
    org     = gh_cfg.get("org", "")

    if not org:
        console.print("[red]github.org não configurado em config/settings.yaml[/red]")
        sys.exit(1)

    cache_path_all = Path(cache_dir) / "audits.json"
    audits: list[ServiceAudit] = []

    # ── Tenta carregar cache ──────────────────────────────────────────────
    if not no_cache and not repo and cache_path_all.exists():
        console.print(f"[cyan]↩ Usando cache:[/cyan] {cache_path_all}")
        raw = json.loads(cache_path_all.read_text())
        audits = [_dict_to_audit(d) for d in raw]

    else:
        gc = GitHubAuditCollector(
            token=token,
            base_url=gh_cfg.get("base_url"),
        )

        # ── Coleta ───────────────────────────────────────────────────────
        if repo:
            if "/" in repo:
                owner, rname = repo.split("/", 1)
            else:
                owner, rname = org, repo
            console.print(f"\n[bold]Coletando:[/bold] [cyan]{owner}/{rname}[/cyan]")
            audits = [gc.scan_one(owner, rname)]

        else:
            console.print(f"\n[bold]Varrendo org:[/bold] [cyan]{org}[/cyan]")
            console.print(f"  Filtro: [dim]{repo_filter or 'todos service-*'}[/dim]\n")

            if Progress:
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    MofNCompleteColumn(),
                    console=console,
                    transient=True,
                ) as progress:
                    task = progress.add_task("Coletando repositórios...", total=None)

                    def on_repo(name, i, total):
                        progress.update(task, description=f"[cyan]{name}[/cyan]", total=total, completed=i)

                    audits = _scan_with_callback(gc, org, repo_filter, gh_cfg, on_repo)
            else:
                audits = gc.scan_org(
                    org=org,
                    repo_filter=repo_filter,
                    skip_archived=gh_cfg.get("skip_archived", True),
                    skip_forks=gh_cfg.get("skip_forks", True),
                    max_repos=gh_cfg.get("max_repos", 500),
                )

        # ── Salva cache ───────────────────────────────────────────────────
        if not repo:
            Path(cache_dir).mkdir(parents=True, exist_ok=True)
            cache_path_all.write_text(json.dumps([_audit_to_dict(a) for a in audits], indent=2))
            console.print(f"  [dim]Cache salvo: {cache_path_all}[/dim]")

    # ── Sumário de coleta ─────────────────────────────────────────────────
    scanned = [a for a in audits if not a.skipped]
    skipped = [a for a in audits if a.skipped]
    console.print(f"\n  [green]✓ {len(scanned)} serviços analisados[/green]  "
                  f"[dim]{len(skipped)} pulados[/dim]")

    if not scanned:
        console.print("[yellow]Nenhum serviço válido encontrado.[/yellow]")
        sys.exit(0)

    # ── Análise ───────────────────────────────────────────────────────────
    console.print("\n[bold]Calculando estatísticas...[/bold]")
    stats = analyze(audits)

    # ── Relatório ─────────────────────────────────────────────────────────
    console.print(f"[bold]Gerando relatório:[/bold] [cyan]{output}[/cyan]")
    generate_report(audits, stats, output, org=org)

    # ── Sumário no terminal ───────────────────────────────────────────────
    console.print()
    _print_summary(stats)
    console.print(f"\n[bold green]✓ Relatório gerado:[/bold green] {output}")


def _scan_with_callback(gc, org, repo_filter, gh_cfg, on_repo):
    """Wrapper que emite callbacks de progresso durante o scan."""
    results = []
    checked = 0
    for repo_meta in gc._iter_repos(
        org,
        gh_cfg.get("skip_archived", True),
        gh_cfg.get("skip_forks", True),
    ):
        if checked >= gh_cfg.get("max_repos", 500):
            break
        name = repo_meta["name"]
        if not name.startswith(gc.REPO_PREFIX):
            continue
        if repo_filter and repo_filter.lower() not in name.lower():
            continue
        checked += 1
        on_repo(name, checked, None)
        branch = repo_meta.get("default_branch", "main")
        audit  = gc._scan_repo(org, name, branch)
        results.append(audit)
        import time; time.sleep(0.05)
    return results


def _print_summary(stats):
    from rich.table import Table
    t = Table(show_header=True, header_style="bold dim", box=None, padding=(0, 2))
    t.add_column("Métrica")
    t.add_column("Valor", justify="right", style="bold")
    t.add_column("Status", justify="right")

    def row(label, val, ok_threshold=None, bad_is_high=True):
        pct = f"{val}%"
        if ok_threshold is None:
            return label, pct, ""
        if bad_is_high:
            color = "red" if val > ok_threshold * 1.5 else ("yellow" if val > ok_threshold else "green")
        else:
            color = "green" if val >= ok_threshold else ("yellow" if val >= ok_threshold * 0.6 else "red")
        icon = "●"
        return label, pct, f"[{color}]{icon}[/{color}]"

    t.add_row(*row("Sem resources",      stats.pct_no_resources,   10))
    t.add_row(*row("Sem HPA",            stats.pct_no_hpa,         20))
    t.add_row(*row("Sem readiness probe",stats.pct_no_readiness,   30))
    t.add_row(*row("Resources completos",stats.pct_full_resources,  80, bad_is_high=False))
    t.add_row(*row("Sem issues críticos",stats.pct_fully_ok,        70, bad_is_high=False))
    console.print(t)

    if stats.most_critical:
        console.print("\n[red bold]Top issues críticos:[/red bold]")
        for name, n in stats.most_critical[:5]:
            console.print(f"  [dim]•[/dim] {name}  [red]{n} crítico(s)[/red]")


# ── Serialização ──────────────────────────────────────────────────────────────

def _audit_to_dict(a) -> dict:
    import dataclasses
    d = dataclasses.asdict(a)
    d["issues"] = [dataclasses.asdict(i) for i in a.issues]
    return d


def _dict_to_audit(d: dict):
    from collectors.github import ServiceAudit, Issue
    issues = [Issue(**i) for i in d.pop("issues", [])]
    # Remove campos temporários que não são do dataclass
    d.pop("_reqs_path", None)
    valid = set(ServiceAudit.__dataclass_fields__)
    a = ServiceAudit(**{k: v for k, v in d.items() if k in valid})
    a.issues = issues
    return a


if __name__ == "__main__":
    cli()