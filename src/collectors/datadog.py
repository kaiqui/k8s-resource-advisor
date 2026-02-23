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
    cpu_usage_cores: list[float] = field(default_factory=list)       # cores em uso
    cpu_throttle_ratio: list[float] = field(default_factory=list)    # 0.0 a 1.0
    memory_bytes: list[float] = field(default_factory=list)          # bytes working set
    rps: list[float] = field(default_factory=list)                   # req/s por pod
    replica_count: list[float] = field(default_factory=list)         # replicas ativas

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
      kubernetes.cpu.usage.total   → nanosegundos, dividimos por 1e9 para cores
      kubernetes.memory.working_set → bytes
      kubernetes.containers.restarts (com reason=OOMKilled)
      container.uptime             → startup proxy

    Para RPS, tenta métricas de trace (trace.web.request) e APM.
    """

    # Queries Datadog por métrica
    QUERIES = {
        "cpu_usage": (
            "avg:kubernetes.cpu.usage.total{{{filters}}} by {{pod_name}} / 1e9"
        ),
        "cpu_throttle": (
            "avg:kubernetes.cpu.throttled.time{{{filters}}} by {{pod_name}} / "
            "avg:kubernetes.cpu.usage.total{{{filters}}} by {{pod_name}}"
        ),
        "memory": (
            "avg:kubernetes.memory.working_set{{{filters}}} by {{pod_name}}"
        ),
        "rps_apm": (
            "sum:trace.web.request.hits{{{filters}},env:{env}}.as_rate()"
        ),
        "rps_nginx": (
            "sum:nginx.net.request_per_s{{{filters}}}"
        ),
        "oom_kills": (
            "sum:kubernetes.containers.restarts{{{filters}},reason:oomkilled}.as_count()"
        ),
        "replicas": (
            "avg:kubernetes.deployments.replicas_available{{{filters}}}"
        ),
        "cpu_request": (
            "avg:kubernetes.cpu.requests{{{filters}}} by {{pod_name}}"
        ),
        "mem_request": (
            "avg:kubernetes.memory.requests{{{filters}}} by {{pod_name}}"
        ),
        "cpu_limit": (
            "avg:kubernetes.cpu.limits{{{filters}}} by {{pod_name}}"
        ),
        "mem_limit": (
            "avg:kubernetes.memory.limits{{{filters}}} by {{pod_name}}"
        ),
    }

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

    def _build_filters(self, app_config: dict) -> str:
        """Monta a string de filtros para as queries."""
        service = app_config["dd_service"]
        env = app_config.get("dd_env", "production")
        namespace = app_config.get("namespace", "default")
        filters = f"service:{service},env:{env},kube_namespace:{namespace}"
        return filters

    @staticmethod
    def _call_query_metrics(api, start: int, end: int, query: str):
        """
        Chama api.query_metrics() com a assinatura correta para a versão instalada.

        O datadog-api-client renomeou os parâmetros ao longo das versões:
          < 2.x   → query_metrics(start=, end=, query=)
          >= 2.x  → query_metrics(_from, to, query)  (posicionais)

        Tenta cada variante em ordem; retorna na primeira que funcionar.
        """
        # versão >= 2.x: posicionais
        try:
            return api.query_metrics(start, end, query)
        except TypeError:
            pass
        # versão antiga: kwargs start/end
        try:
            return api.query_metrics(start=start, end=end, query=query)
        except TypeError:
            pass
        # variante com _from/to
        return api.query_metrics(_from=start, to=end, query=query)

    def _query_metric(
        self,
        api,
        query: str,
        start: int,
        end: int,
    ) -> list[float]:
        """Executa uma query e retorna lista de valores (média de todas as series)."""
        try:
            resp = self._call_query_metrics(api, start, end, query)
            all_values = []
            if resp.series:
                for series in resp.series:
                    vals = [p[1] for p in series.pointlist if p[1] is not None]
                    all_values.extend(vals)
            return all_values
        except Exception as exc:
            console.print(f"    [yellow]⚠ Query falhou: {exc}[/yellow]")
            return []

    def _query_metric_avg_series(
        self,
        api,
        query: str,
        start: int,
        end: int,
    ) -> list[float]:
        """
        Retorna a série temporal média entre todos os pods.
        Útil para CPU/memória onde queremos o comportamento médio por pod.
        """
        try:
            resp = self._call_query_metrics(api, start, end, query)
            if not resp.series:
                return []

            # Alinhar todas as series pelo mesmo índice de tempo
            all_series = []
            for series in resp.series:
                vals = [p[1] if p[1] is not None else float("nan") for p in series.pointlist]
                all_series.append(vals)

            if not all_series:
                return []

            # Média por timestamp (ignora NaN)
            max_len = max(len(s) for s in all_series)
            result = []
            for i in range(max_len):
                pts = [s[i] for s in all_series if i < len(s) and not np.isnan(s[i])]
                result.append(float(np.mean(pts)) if pts else float("nan"))

            return [v for v in result if not np.isnan(v)]
        except Exception as exc:
            console.print(f"    [yellow]⚠ Query série falhou: {exc}[/yellow]")
            return []

    def collect(
        self,
        app_config: dict,
        weeks: int = 4,
        interval_seconds: int = 300,
        cache_dir: Optional[str] = None,
    ) -> AppMetrics:
        """
        Ponto de entrada principal. Coleta todas as métricas de uma app.

        Args:
            app_config: dicionário com campos do settings.yaml
            weeks: janela de coleta em semanas
            interval_seconds: granularidade das métricas
            cache_dir: se fornecido, salva/carrega cache JSON

        Returns:
            AppMetrics preenchido
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
        filters = self._build_filters(app_config)
        env = app_config.get("dd_env", "production")

        metrics = AppMetrics(
            app_name=app_name,
            namespace=app_config.get("namespace", "default"),
            dd_service=app_config["dd_service"],
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

            # CPU usage (cores médio por pod)
            console.print("    CPU usage...")
            cpu_query = f"avg:kubernetes.cpu.usage.total{{service:{app_config['dd_service']},kube_namespace:{metrics.namespace}}} by {{pod_name}} / 1e9"
            metrics.cpu_usage_cores = self._query_metric_avg_series(api, cpu_query, start, now)

            # CPU throttle
            console.print("    CPU throttle...")
            throttle_q = f"avg:kubernetes.cpu.throttled.time{{service:{app_config['dd_service']},kube_namespace:{metrics.namespace}}} / (avg:kubernetes.cpu.usage.total{{service:{app_config['dd_service']},kube_namespace:{metrics.namespace}}} + 1)"
            metrics.cpu_throttle_ratio = self._query_metric(api, throttle_q, start, now)

            # Memória
            console.print("    Memória...")
            mem_query = f"avg:kubernetes.memory.working_set{{service:{app_config['dd_service']},kube_namespace:{metrics.namespace}}} by {{pod_name}}"
            metrics.memory_bytes = self._query_metric_avg_series(api, mem_query, start, now)

            # RPS — tenta APM primeiro, depois nginx
            console.print("    RPS (APM)...")
            rps_query = f"sum:trace.web.request.hits{{service:{app_config['dd_service']},env:{env}}}.as_rate()"
            rps_vals = self._query_metric(api, rps_query, start, now)
            if not rps_vals:
                console.print("    RPS (nginx fallback)...")
                rps_query2 = f"sum:nginx.net.request_per_s{{service:{app_config['dd_service']}}}"
                rps_vals = self._query_metric(api, rps_query2, start, now)
            # Normalizar por réplicas (RPS por pod)
            metrics.rps = rps_vals  # divisão por réplicas no analyzer

            # OOMKills
            console.print("    OOMKills...")
            oom_query = f"sum:kubernetes.containers.restarts{{service:{app_config['dd_service']},kube_namespace:{metrics.namespace},reason:oomkilled}}.as_count()"
            oom_vals = self._query_metric(api, oom_query, start, now)
            metrics.oom_kill_count = int(sum(oom_vals))

            # Réplicas
            console.print("    Réplicas...")
            replica_query = f"avg:kubernetes.deployments.replicas_available{{kube_deployment:{app_name},kube_namespace:{metrics.namespace}}}"
            metrics.replica_count = self._query_metric(api, replica_query, start, now)

            # Resources atuais (média histórica)
            console.print("    Resources atuais...")
            cpu_req_q = f"avg:kubernetes.cpu.requests{{service:{app_config['dd_service']},kube_namespace:{metrics.namespace}}}"
            cpu_req_vals = self._query_metric(api, cpu_req_q, start, now)
            if cpu_req_vals:
                metrics.current_cpu_request_cores = float(np.mean(cpu_req_vals))

            mem_req_q = f"avg:kubernetes.memory.requests{{service:{app_config['dd_service']},kube_namespace:{metrics.namespace}}}"
            mem_req_vals = self._query_metric(api, mem_req_q, start, now)
            if mem_req_vals:
                metrics.current_mem_request_bytes = float(np.mean(mem_req_vals))

            cpu_lim_q = f"avg:kubernetes.cpu.limits{{service:{app_config['dd_service']},kube_namespace:{metrics.namespace}}}"
            cpu_lim_vals = self._query_metric(api, cpu_lim_q, start, now)
            if cpu_lim_vals:
                metrics.current_cpu_limit_cores = float(np.mean(cpu_lim_vals))

            mem_lim_q = f"avg:kubernetes.memory.limits{{service:{app_config['dd_service']},kube_namespace:{metrics.namespace}}}"
            mem_lim_vals = self._query_metric(api, mem_lim_q, start, now)
            if mem_lim_vals:
                metrics.current_mem_limit_bytes = float(np.mean(mem_lim_vals))

            # Startup time — usa container uptime proxy
            # Se disponível via custom metric ou kube_pod start_time
            console.print("    Startup time...")
            startup_query = f"histogram_quantile(0.50, sum(rate(kubernetes_pod_start_duration_seconds_bucket{{service:{app_config['dd_service']}}}[5m])) by (le))"
            startup_vals = self._query_metric(api, startup_query, start, now)
            if startup_vals:
                metrics.startup_p50_seconds = float(np.percentile(startup_vals, 50))
                metrics.startup_p95_seconds = float(np.percentile(startup_vals, 95))
            else:
                # fallback: estimativa por framework
                metrics.startup_p50_seconds, metrics.startup_p95_seconds = _estimate_startup(app_config)

        # Salva cache
        if cache_dir:
            Path(cache_dir).mkdir(parents=True, exist_ok=True)
            with open(cache_path, "w") as f:
                json.dump(asdict(metrics), f, indent=2)
            console.print(f"  [green]✓ Cache salvo:[/green] {cache_path}")

        return metrics


def _estimate_startup(app_config: dict) -> tuple[float, float]:
    """
    Estimativa de startup time quando não há métrica disponível.
    Baseado em benchmarks típicos por framework.
    """
    framework = app_config.get("framework", "wsgi").lower()
    py_ver = app_config.get("python_version", "3.x")

    estimates = {
        "fastapi": (3.0, 8.0),
        "aiohttp": (2.0, 6.0),
        "flask": (4.0, 12.0),
        "django": (8.0, 20.0),
        "wsgi": (5.0, 15.0),
        "async": (3.0, 8.0),
    }

    p50, p95 = estimates.get(framework, (5.0, 15.0))

    # Python 2.7 é mais lento no import
    if py_ver.startswith("2."):
        p50 *= 1.5
        p95 *= 1.5

    return p50, p95