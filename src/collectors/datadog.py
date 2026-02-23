"""
collectors/datadog.py

Coleta métricas do Datadog para uma app durante um período de tempo.
Retorna tudo que os calculadores precisam: CPU, memória, RPS, startup time, OOMKills.

Métricas coletadas:
  - CPU usage por pod (rate de nanosegundos)
  - CPU throttling (detecta limit muito baixo)
  - Memória working set por pod
  - OOMKill count (detecta limit de memória estourado)
  - HTTP requests por segundo (se instrumentado)
  - Pod startup duration (detects slow cold starts)
  - Réplicas ativas ao longo do tempo

Comportamento quando não há dados:
  - CPU e memória são métricas obrigatórias.
  - Se ambas retornarem vazias, levanta InsufficientDataError — o pipeline
    aborta essa app com mensagem clara em vez de gerar recomendações com
    valores mínimos default que seriam inválidos.
  - As demais métricas (RPS, réplicas, throttle) são opcionais: ausência
    apenas reduz a qualidade das recomendações, com warning no relatório.
"""

import json
import os
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import numpy as np

try:
    from datadog_api_client import ApiClient, Configuration
    from datadog_api_client.v1.api.metrics_api import MetricsApi
    HAS_DATADOG = True
except ImportError:
    HAS_DATADOG = False

try:
    from rich.console import Console
    console = Console()
except ImportError:
    class _FallbackConsole:
        def print(self, msg, **_): print(msg)
    console = _FallbackConsole()


class InsufficientDataError(Exception):
    """
    Levantada quando os dados do Datadog são insuficientes para gerar
    recomendações válidas. O pipeline deve abortar essa app e informar o usuário
    em vez de prosseguir com valores default que seriam enganosos.
    """
    pass


@dataclass
class AppMetrics:
    """Resultado bruto da coleta para uma app."""
    app_name: str
    namespace: str
    dd_service: str
    dd_env: str
    framework: str
    python_version: str
    collection_start: str
    collection_end: str
    weeks_collected: int

    # Séries temporais (lista de valores float, amostras a cada interval_seconds)
    cpu_usage_cores: list[float] = field(default_factory=list)
    cpu_throttle_ratio: list[float] = field(default_factory=list)
    memory_bytes: list[float] = field(default_factory=list)
    rps: list[float] = field(default_factory=list)
    replica_count: list[float] = field(default_factory=list)

    # Métricas pontuais
    oom_kill_count: int = 0
    startup_p50_seconds: float = 0.0
    startup_p95_seconds: float = 0.0
    current_cpu_request_cores: float = 0.0
    current_cpu_limit_cores: float = 0.0
    current_mem_request_bytes: float = 0.0
    current_mem_limit_bytes: float = 0.0

    # Flags extras
    has_pandas: bool = False
    sla_latency_p95_ms: float = 500.0
    peak_traffic_multiplier: float = 3.0


class DatadogCollector:
    """
    Coleta métricas do Datadog usando a API v1 de timeseries.

    As queries usam as métricas padrão do Kubernetes com o Datadog Agent:
      kubernetes.cpu.usage.total    → nanosegundos, dividimos por 1e9 para cores
      kubernetes.memory.working_set → bytes
      kubernetes.containers.restarts (com reason=OOMKilled)
    """

    def __init__(self, api_key: str, app_key: str, site: str = "datadoghq.com"):
        if not HAS_DATADOG:
            raise ImportError(
                "datadog-api-client não instalado. "
                "Rode: pip install datadog-api-client"
            )
        config = Configuration()
        config.api_key["apiKeyAuth"] = api_key
        config.api_key["appKeyAuth"] = app_key
        config.server_variables["site"] = site
        self._config = config

    # ── Compatibilidade de API ────────────────────────────────────────────

    @staticmethod
    def _call_query_metrics(api, start: int, end: int, query: str):
        """
        Chama api.query_metrics() com a assinatura correta para a versão instalada.

        O datadog-api-client alterou a assinatura entre versões:
          < 2.x   → query_metrics(start=, end=, query=)
          >= 2.x  → argumentos posicionais (start, end, query)

        Tenta cada variante em ordem.
        """
        try:
            return api.query_metrics(start, end, query)
        except TypeError:
            pass
        try:
            return api.query_metrics(start=start, end=end, query=query)
        except TypeError:
            pass
        return api.query_metrics(_from=start, to=end, query=query)

    @staticmethod
    def _point_value(point) -> Optional[float]:
        """
        Extrai o valor numérico de um ponto da série temporal.

        O formato mudou entre versões do cliente:
          Versão antiga: pointlist é lista de [timestamp, value]  → p[1]
          Versão nova:   pointlist é lista de objetos Point       → p.value
        """
        # Tenta acesso por índice (formato lista [ts, value])
        try:
            v = point[1]
            if v is not None:
                return float(v)
            return None
        except (TypeError, KeyError, IndexError):
            pass

        # Tenta atributo .value (objeto Point)
        try:
            v = point.value
            if v is not None:
                return float(v)
            return None
        except AttributeError:
            pass

        # Tenta atributo .y (algumas versões usam x/y)
        try:
            v = point.y
            if v is not None:
                return float(v)
            return None
        except AttributeError:
            pass

        return None

    # ── Queries ───────────────────────────────────────────────────────────

    def _query_metric(
        self, api, query: str, start: int, end: int
    ) -> list[float]:
        """Executa uma query e retorna lista plana de valores de todas as séries."""
        try:
            resp = self._call_query_metrics(api, start, end, query)
            all_values = []
            if resp.series:
                for series in resp.series:
                    for p in series.pointlist:
                        v = self._point_value(p)
                        if v is not None:
                            all_values.append(v)
            return all_values
        except Exception as exc:
            console.print(f"    [yellow]⚠ Query falhou: {exc}[/yellow]")
            return []

    def _query_metric_avg_series(
        self, api, query: str, start: int, end: int
    ) -> list[float]:
        """
        Retorna a série temporal com a média de todos os pods por timestamp.
        Útil para CPU e memória onde queremos o comportamento médio por pod,
        não a soma do cluster.
        """
        try:
            resp = self._call_query_metrics(api, start, end, query)
            if not resp.series:
                return []

            all_series = []
            for series in resp.series:
                vals = []
                for p in series.pointlist:
                    v = self._point_value(p)
                    vals.append(v if v is not None else float("nan"))
                all_series.append(vals)

            if not all_series:
                return []

            max_len = max(len(s) for s in all_series)
            result = []
            for i in range(max_len):
                pts = [s[i] for s in all_series if i < len(s) and not np.isnan(s[i])]
                result.append(float(np.mean(pts)) if pts else float("nan"))

            return [v for v in result if not np.isnan(v)]
        except Exception as exc:
            console.print(f"    [yellow]⚠ Query série falhou: {exc}[/yellow]")
            return []

    # ── Coleta principal ──────────────────────────────────────────────────

    def collect(
        self,
        app_config: dict,
        weeks: int = 4,
        interval_seconds: int = 300,
        cache_dir: Optional[str] = None,
    ) -> AppMetrics:
        """
        Coleta todas as métricas de uma app.

        Levanta InsufficientDataError se CPU e memória retornarem vazias —
        isso indica que as queries falharam ou a instrumentação está ausente,
        e prosseguir geraria recomendações com valores mínimos inválidos.
        """
        app_name = app_config["name"]

        # Verifica cache
        if cache_dir:
            cache_path = Path(cache_dir) / f"{app_name}-metrics.json"
            if cache_path.exists():
                console.print(f"  [cyan]↩ Usando cache:[/cyan] {cache_path}")
                with open(cache_path) as f:
                    data = json.load(f)
                return AppMetrics(**data)

        now = int(time.time())
        start = now - (weeks * 7 * 24 * 3600)
        env = app_config.get("dd_env", "production")
        svc = app_config["dd_service"]
        ns = app_config.get("namespace", "default")

        metrics = AppMetrics(
            app_name=app_name,
            namespace=ns,
            dd_service=svc,
            dd_env=env,
            framework=app_config.get("framework", "wsgi"),
            python_version=app_config.get("python_version", "3.x"),
            collection_start=datetime.fromtimestamp(start, tz=timezone.utc).isoformat(),
            collection_end=datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
            weeks_collected=weeks,
            has_pandas=app_config.get("has_pandas", False),
            sla_latency_p95_ms=app_config.get("sla_latency_p95_ms", 500.0),
            peak_traffic_multiplier=app_config.get("peak_traffic_multiplier", 3.0),
        )

        with ApiClient(self._config) as client:
            api = MetricsApi(client)
            console.print(f"  [bold]Coletando:[/bold] {app_name}")

            # ── CPU usage ────────────────────────────────────────────────
            console.print("    CPU usage...")
            metrics.cpu_usage_cores = self._query_metric_avg_series(
                api,
                f"avg:kubernetes.cpu.usage.total{{service:{svc},kube_namespace:{ns}}} by {{pod_name}} / 1e9",
                start, now,
            )

            # ── CPU throttle ─────────────────────────────────────────────
            console.print("    CPU throttle...")
            metrics.cpu_throttle_ratio = self._query_metric(
                api,
                f"avg:kubernetes.cpu.throttled.time{{service:{svc},kube_namespace:{ns}}} / "
                f"(avg:kubernetes.cpu.usage.total{{service:{svc},kube_namespace:{ns}}} + 1)",
                start, now,
            )

            # ── Memória ──────────────────────────────────────────────────
            console.print("    Memória...")
            metrics.memory_bytes = self._query_metric_avg_series(
                api,
                f"avg:kubernetes.memory.working_set{{service:{svc},kube_namespace:{ns}}} by {{pod_name}}",
                start, now,
            )

            # ── Dados obrigatórios: CPU e memória ─────────────────────────
            # Se ambas vieram vazias, algo está fundamentalmente errado:
            # service tag errada, namespace errado, agent não instalado,
            # ou a aplicação não tem dados no período solicitado.
            # Continuar geraria recomendações com valores mínimos default
            # (50m CPU, 64Mi memória) que não refletem nada real.
            if not metrics.cpu_usage_cores and not metrics.memory_bytes:
                raise InsufficientDataError(
                    f"Nenhum dado de CPU ou memória retornado pelo Datadog para '{svc}' "
                    f"(namespace={ns}, env={env}, janela={weeks} semanas).\n"
                    f"  Verifique:\n"
                    f"  • O DD_SERVICE '{svc}' corresponde exatamente à tag service: no Datadog?\n"
                    f"  • O namespace '{ns}' está correto?\n"
                    f"  • O Datadog Agent está instalado no cluster e coletando métricas kubernetes.*?\n"
                    f"  • A aplicação existia nesse período? Tente --weeks 1 ou --weeks 2."
                )

            # ── RPS ───────────────────────────────────────────────────────
            console.print("    RPS (APM)...")
            rps_vals = self._query_metric(
                api,
                f"sum:trace.web.request.hits{{service:{svc},env:{env}}}.as_rate()",
                start, now,
            )
            if not rps_vals:
                console.print("    RPS (nginx fallback)...")
                rps_vals = self._query_metric(
                    api,
                    f"sum:nginx.net.request_per_s{{service:{svc}}}",
                    start, now,
                )
            metrics.rps = rps_vals

            # ── OOMKills ──────────────────────────────────────────────────
            console.print("    OOMKills...")
            oom_vals = self._query_metric(
                api,
                f"sum:kubernetes.containers.restarts{{service:{svc},kube_namespace:{ns},reason:oomkilled}}.as_count()",
                start, now,
            )
            metrics.oom_kill_count = int(sum(oom_vals))

            # ── Réplicas ──────────────────────────────────────────────────
            console.print("    Réplicas...")
            metrics.replica_count = self._query_metric(
                api,
                f"avg:kubernetes.deployments.replicas_available{{kube_deployment:{app_name},kube_namespace:{ns}}}",
                start, now,
            )

            # ── Resources atuais via Datadog (se deploy.yaml não forneceu) ─
            console.print("    Resources atuais...")
            for query, attr in [
                (f"avg:kubernetes.cpu.requests{{service:{svc},kube_namespace:{ns}}}",  "current_cpu_request_cores"),
                (f"avg:kubernetes.memory.requests{{service:{svc},kube_namespace:{ns}}}", "current_mem_request_bytes"),
                (f"avg:kubernetes.cpu.limits{{service:{svc},kube_namespace:{ns}}}",     "current_cpu_limit_cores"),
                (f"avg:kubernetes.memory.limits{{service:{svc},kube_namespace:{ns}}}",  "current_mem_limit_bytes"),
            ]:
                vals = self._query_metric(api, query, start, now)
                if vals and getattr(metrics, attr) == 0.0:
                    setattr(metrics, attr, float(np.mean(vals)))

            # ── Startup time ──────────────────────────────────────────────
            console.print("    Startup time...")
            startup_vals = self._query_metric(
                api,
                f"histogram_quantile(0.50, sum(rate(kubernetes_pod_start_duration_seconds_bucket"
                f"{{service:{svc}}}[5m])) by (le))",
                start, now,
            )
            if startup_vals:
                metrics.startup_p50_seconds = float(np.percentile(startup_vals, 50))
                metrics.startup_p95_seconds = float(np.percentile(startup_vals, 95))
            else:
                metrics.startup_p50_seconds, metrics.startup_p95_seconds = _estimate_startup(app_config)

        # Salva cache — só salva se passou da validação de dados
        if cache_dir:
            Path(cache_dir).mkdir(parents=True, exist_ok=True)
            cache_path = Path(cache_dir) / f"{app_name}-metrics.json"
            with open(cache_path, "w") as f:
                json.dump(asdict(metrics), f, indent=2)
            console.print(f"  [green]✓ Cache salvo:[/green] {cache_path}")

        return metrics


def _estimate_startup(app_config: dict) -> tuple[float, float]:
    """Estimativa de startup time quando não há métrica disponível."""
    framework = app_config.get("framework", "wsgi").lower()
    py_ver = app_config.get("python_version", "3.x")
    estimates = {
        "fastapi":  (3.0,  8.0),
        "aiohttp":  (2.0,  6.0),
        "flask":    (4.0, 12.0),
        "django":   (8.0, 20.0),
        "wsgi":     (5.0, 15.0),
        "async":    (3.0,  8.0),
    }
    p50, p95 = estimates.get(framework, (5.0, 15.0))
    if py_ver.startswith("2."):
        p50 *= 1.5
        p95 *= 1.5
    return p50, p95