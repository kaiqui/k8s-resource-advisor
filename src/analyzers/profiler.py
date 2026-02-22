from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import numpy as np

from collectors.datadog import AppMetrics


class AppProfile(str, Enum):
    CPU_BOUND = "cpu_bound"       
    IO_BOUND = "io_bound"          
    MEMORY_BOUND = "memory_bound"  
    MIXED = "mixed"  


class FrameworkType(str, Enum):
    WSGI = "wsgi"       
    ASYNC = "async"     

@dataclass
class PercentileStats:
    p50: float
    p75: float
    p95: float
    p99: float
    max: float
    mean: float
    std: float

    def to_dict(self) -> dict:
        return {
            "p50": round(self.p50, 4),
            "p75": round(self.p75, 4),
            "p95": round(self.p95, 4),
            "p99": round(self.p99, 4),
            "max": round(self.max, 4),
            "mean": round(self.mean, 4),
            "std": round(self.std, 4),
        }


@dataclass
class HealthWarning:
    severity: str   # "critical" | "warning" | "info"
    code: str
    message: str


@dataclass
class AnalyzedApp:
    app_name: str
    namespace: str
    framework_type: FrameworkType
    app_profile: AppProfile
    python_version: str

    # Percentis calculados
    cpu_stats: PercentileStats          # em cores (ex: 0.25 = 250m)
    mem_stats: PercentileStats          # em bytes

    # Métricas derivadas
    avg_replicas: float
    peak_replicas: float
    rps_per_pod_p95: float             # RPS por pod no pico
    cpu_throttle_p95: float            # ratio de throttle (0..1)
    oom_kill_count: int
    startup_p50_seconds: float
    startup_p95_seconds: float

    # Configuração atual
    current_cpu_request_cores: float
    current_cpu_limit_cores: float
    current_mem_request_bytes: float
    current_mem_limit_bytes: float

    # Flags
    has_pandas: bool
    sla_latency_p95_ms: float
    peak_traffic_multiplier: float
    weeks_collected: int

    # Diagnósticos
    warnings: list[HealthWarning] = field(default_factory=list)
    memory_leak_detected: bool = False
    cpu_knee_point_cores: float = 0.0   # ponto onde CPU satura por pod
    data_quality_score: float = 1.0    # 0..1, quão confiáveis são os dados


class AppProfiler:

    WSGI_FRAMEWORKS = {"wsgi", "flask", "django", "gunicorn", "uwsgi"}
    ASYNC_FRAMEWORKS = {"async", "fastapi", "aiohttp", "tornado", "starlette"}

    def analyze(self, metrics: AppMetrics) -> AnalyzedApp:
        framework_type = self._detect_framework_type(metrics.framework)
        cpu_stats = self._compute_stats(metrics.cpu_usage_cores)
        mem_stats = self._compute_stats(metrics.memory_bytes)

        avg_replicas = float(np.mean(metrics.replica_count)) if metrics.replica_count else 1.0
        peak_replicas = float(np.max(metrics.replica_count)) if metrics.replica_count else 1.0

        # RPS por pod normalizado
        rps_per_pod_p95 = self._compute_rps_per_pod(
            metrics.rps, metrics.replica_count
        )

        cpu_throttle_p95 = 0.0
        if metrics.cpu_throttle_ratio:
            clean = [v for v in metrics.cpu_throttle_ratio if 0 <= v <= 1]
            if clean:
                cpu_throttle_p95 = float(np.percentile(clean, 95))

        profile = self._classify_profile(metrics, cpu_stats, mem_stats, framework_type)
        memory_leak = self._detect_memory_leak(metrics.memory_bytes)
        knee_point = self._estimate_cpu_knee_point(metrics, cpu_stats)
        data_quality = self._assess_data_quality(metrics)

        analyzed = AnalyzedApp(
            app_name=metrics.app_name,
            namespace=metrics.namespace,
            framework_type=framework_type,
            app_profile=profile,
            python_version=metrics.python_version,
            cpu_stats=cpu_stats,
            mem_stats=mem_stats,
            avg_replicas=avg_replicas,
            peak_replicas=peak_replicas,
            rps_per_pod_p95=rps_per_pod_p95,
            cpu_throttle_p95=cpu_throttle_p95,
            oom_kill_count=metrics.oom_kill_count,
            startup_p50_seconds=metrics.startup_p50_seconds,
            startup_p95_seconds=metrics.startup_p95_seconds,
            current_cpu_request_cores=metrics.current_cpu_request_cores,
            current_cpu_limit_cores=metrics.current_cpu_limit_cores,
            current_mem_request_bytes=metrics.current_mem_request_bytes,
            current_mem_limit_bytes=metrics.current_mem_limit_bytes,
            has_pandas=metrics.has_pandas,
            sla_latency_p95_ms=metrics.sla_latency_p95_ms,
            peak_traffic_multiplier=metrics.peak_traffic_multiplier,
            weeks_collected=metrics.weeks_collected,
            memory_leak_detected=memory_leak,
            cpu_knee_point_cores=knee_point,
            data_quality_score=data_quality,
        )

        self._generate_warnings(analyzed)
        return analyzed


    def _detect_framework_type(self, framework: str) -> FrameworkType:
        fw = framework.lower()
        if fw in self.WSGI_FRAMEWORKS:
            return FrameworkType.WSGI
        if fw in self.ASYNC_FRAMEWORKS:
            return FrameworkType.ASYNC
        # Default conservador
        return FrameworkType.WSGI

    def _compute_stats(self, values: list[float]) -> PercentileStats:
        if not values:
            return PercentileStats(0, 0, 0, 0, 0, 0, 0)
        arr = np.array([v for v in values if v is not None and not np.isnan(v) and v >= 0])
        if len(arr) == 0:
            return PercentileStats(0, 0, 0, 0, 0, 0, 0)
        return PercentileStats(
            p50=float(np.percentile(arr, 50)),
            p75=float(np.percentile(arr, 75)),
            p95=float(np.percentile(arr, 95)),
            p99=float(np.percentile(arr, 99)),
            max=float(np.max(arr)),
            mean=float(np.mean(arr)),
            std=float(np.std(arr)),
        )

    def _compute_rps_per_pod(
        self, rps_total: list[float], replicas: list[float]
    ) -> float:
        if not rps_total:
            return 0.0
        if not replicas:
            return float(np.percentile(rps_total, 95))

        # Normaliza RPS total pelo número de réplicas no mesmo instante
        n = min(len(rps_total), len(replicas))
        per_pod = []
        for i in range(n):
            r = replicas[i] if replicas[i] > 0 else 1.0
            per_pod.append(rps_total[i] / r)

        return float(np.percentile(per_pod, 95))

    def _classify_profile(
        self,
        metrics: AppMetrics,
        cpu_stats: PercentileStats,
        mem_stats: PercentileStats,
        framework_type: FrameworkType,
    ) -> AppProfile:
        cpu_limit = metrics.current_cpu_limit_cores or 1.0
        cpu_utilization_p99 = cpu_stats.p99 / cpu_limit if cpu_limit > 0 else 0

        is_cpu_heavy = cpu_utilization_p99 > 0.6
        has_throttling = (
            len(metrics.cpu_throttle_ratio) > 0
            and float(np.percentile(metrics.cpu_throttle_ratio, 95)) > 0.10
        )
        is_memory_growing = self._detect_memory_leak(metrics.memory_bytes)
        is_cpu_low = cpu_stats.mean < 0.3 and cpu_stats.p95 < 0.5

        if is_cpu_heavy or has_throttling:
            return AppProfile.CPU_BOUND
        if is_memory_growing and is_cpu_low:
            return AppProfile.MEMORY_BOUND
        if framework_type == FrameworkType.ASYNC and is_cpu_low:
            return AppProfile.IO_BOUND
        return AppProfile.MIXED

    def _detect_memory_leak(self, memory_bytes: list[float]) -> bool:
        if len(memory_bytes) < 100:
            return False
        arr = np.array(memory_bytes)
        x = np.arange(len(arr))
        slope, _ = np.polyfit(x, arr, 1)
        mean_val = np.mean(arr)
        if mean_val == 0:
            return False
        # slope em bytes por sample, multiplicado pelo número de samples em 1 semana
        samples_per_week = (7 * 24 * 3600) / 300
        weekly_growth_ratio = (slope * samples_per_week) / mean_val
        return weekly_growth_ratio > 0.05  # crescimento > 5% por semana

    def _estimate_cpu_knee_point(
        self, metrics: AppMetrics, cpu_stats: PercentileStats
    ) -> float:
        if (
            metrics.current_cpu_limit_cores > 0
            and len(metrics.cpu_throttle_ratio) > 0
            and float(np.mean(metrics.cpu_throttle_ratio)) > 0.05
        ):
            return metrics.current_cpu_limit_cores * 0.9

        # Caso geral: P99 * multiplicador de pico
        return cpu_stats.p99 * metrics.peak_traffic_multiplier

    def _assess_data_quality(self, metrics: AppMetrics) -> float:
        score = 1.0
        if len(metrics.cpu_usage_cores) < 50:
            score -= 0.3   # poucos pontos
        if len(metrics.memory_bytes) < 50:
            score -= 0.3
        if metrics.weeks_collected < 2:
            score -= 0.2   # janela muito curta
        if not metrics.rps:
            score -= 0.1   # sem RPS, estimativa de réplicas é menos confiável
        return max(0.0, score)

    def _generate_warnings(self, app: AnalyzedApp) -> None:
        w = app.warnings

        if app.oom_kill_count > 0:
            w.append(HealthWarning(
                severity="critical",
                code="OOM_KILLS",
                message=f"{app.oom_kill_count} OOMKills detectados — memory limit muito baixo ou leak.",
            ))

        if app.cpu_throttle_p95 > 0.20:
            w.append(HealthWarning(
                severity="critical",
                code="CPU_THROTTLE_HIGH",
                message=f"CPU throttle P95 em {app.cpu_throttle_p95:.0%} — limit de CPU muito baixo, latência impactada.",
            ))
        elif app.cpu_throttle_p95 > 0.10:
            w.append(HealthWarning(
                severity="warning",
                code="CPU_THROTTLE_MODERATE",
                message=f"CPU throttle P95 em {app.cpu_throttle_p95:.0%} — considere aumentar o limit.",
            ))

        if app.memory_leak_detected:
            w.append(HealthWarning(
                severity="warning",
                code="MEMORY_LEAK",
                message="Tendência crescente de memória detectada — possível memory leak. Investigue antes de aumentar limits.",
            ))

        if app.python_version.startswith("2."):
            w.append(HealthWarning(
                severity="warning",
                code="PYTHON_EOL",
                message="Python 2.7 chegou ao EOL em Jan/2020. Planeje a migração para 3.x.",
            ))

        if app.data_quality_score < 0.6:
            w.append(HealthWarning(
                severity="info",
                code="LOW_DATA_QUALITY",
                message=f"Score de qualidade de dados: {app.data_quality_score:.0%}. Recomendações menos precisas — colete por mais tempo ou valide instrumentação.",
            ))

        if app.startup_p95_seconds > 60:
            w.append(HealthWarning(
                severity="warning",
                code="SLOW_STARTUP",
                message=f"Startup P95 de {app.startup_p95_seconds:.0f}s é lento — configure readinessProbe adequada e considere minReplicas mais alto para absorver picos sem scale-up demorado.",
            ))
