from dataclasses import dataclass

from analyzers.profiler import AnalyzedApp, FrameworkType


@dataclass
class ResourceRecommendation:
    app_name: str

    # Em cores (float) e bytes (int)
    cpu_request_cores: float
    cpu_limit_cores: float
    mem_request_bytes: int
    mem_limit_bytes: int

    # Forma legível para YAML
    cpu_request_str: str    # ex: "250m"
    cpu_limit_str: str
    mem_request_str: str    # ex: "256Mi"
    mem_limit_str: str

    # Comparação com atual
    cpu_request_delta_pct: float
    cpu_limit_delta_pct: float
    mem_request_delta_pct: float
    mem_limit_delta_pct: float

    # Justificativas
    rationale: dict[str, str]


def _cores_to_str(cores: float) -> str:
    """Converte float de cores para string Kubernetes (m ou cores)."""
    millis = round(cores * 1000)
    if millis < 1000:
        return f"{millis}m"
    return f"{round(cores, 1)}"


def _bytes_to_mi(b: int) -> str:
    """Converte bytes para MiB ou GiB."""
    mib = b / (1024 ** 2)
    if mib >= 1024:
        return f"{mib / 1024:.1f}Gi"
    return f"{round(mib)}Mi"


def _delta_pct(new: float, old: float) -> float:
    if old == 0:
        return 0.0
    return round((new - old) / old * 100, 1)


class ResourceCalculator:
    """
    Regras de cálculo:

    CPU Request  = P75(cpu_usage) * cpu_request_headroom
    CPU Limit    = CPU_Request * burst_factor(framework)
    Mem Request  = P80(mem_bytes) * mem_request_headroom * lib_factor
    Mem Limit    = Mem_Request * mem_limit_headroom(python_version)

    Burst factors:
      WSGI:   2.0  (processo bloqueante, pode puxar 100% de 1 core)
      ASYNC:  1.5  (coroutines distribuem melhor a CPU)

    Lib factors (memória):
      pandas/numpy presentes: +20%
      Python 2.7: +20% no limit (GC fragmenta mais)
    """

    def __init__(self, thresholds: dict | None = None):
        th = thresholds or {}
        self.cpu_req_headroom = th.get("cpu_request_headroom", 1.20)
        self.cpu_burst_wsgi = th.get("cpu_limit_headroom_wsgi", 2.0)
        self.cpu_burst_async = th.get("cpu_limit_headroom_async", 1.5)
        self.mem_req_headroom = th.get("mem_request_headroom", 1.30)
        self.mem_limit_headroom = th.get("mem_limit_headroom", 1.50)
        self.mem_limit_headroom_legacy = th.get("mem_limit_headroom_legacy", 1.70)
        self.mem_extra_heavy_libs = th.get("mem_extra_heavy_libs", 1.20)

    def calculate(self, app: AnalyzedApp) -> ResourceRecommendation:
        rationale = {}

        # ── CPU REQUEST ──────────────────────────────────────────────────
        # Base: P75 de CPU (carga normal, não o pico)
        cpu_req_base = app.cpu_stats.p75
        cpu_request = cpu_req_base * self.cpu_req_headroom

        # Throttling detectado → request subestimado, sobe para P95
        if app.cpu_throttle_p95 > 0.15:
            cpu_request = app.cpu_stats.p95 * self.cpu_req_headroom
            rationale["cpu_request"] = (
                f"P95({app.cpu_stats.p95:.3f} cores) usado por throttling detectado "
                f"({app.cpu_throttle_p95:.0%}); headroom {self.cpu_req_headroom}x"
            )
        else:
            rationale["cpu_request"] = (
                f"P75({app.cpu_stats.p75:.3f} cores) * headroom {self.cpu_req_headroom}x"
            )

        # Mínimo razoável: 50m
        cpu_request = max(cpu_request, 0.05)

        # ── CPU LIMIT ────────────────────────────────────────────────────
        burst_factor = (
            self.cpu_burst_wsgi
            if app.framework_type == FrameworkType.WSGI
            else self.cpu_burst_async
        )
        cpu_limit = cpu_request * burst_factor
        rationale["cpu_limit"] = (
            f"Request * burst_factor {burst_factor}x "
            f"({'WSGI bloqueante' if app.framework_type == FrameworkType.WSGI else 'async'})"
        )

        # Se knee point estimado é maior que o limit calculado, usa knee
        if app.cpu_knee_point_cores > cpu_limit:
            cpu_limit = app.cpu_knee_point_cores
            rationale["cpu_limit"] += f"; ajustado para knee point estimado {cpu_limit:.3f} cores"

        # ── MEMÓRIA REQUEST ──────────────────────────────────────────────
        mem_req_base = app.mem_stats.p75
        lib_factor = self.mem_extra_heavy_libs if app.has_pandas else 1.0
        mem_request = int(mem_req_base * self.mem_req_headroom * lib_factor)

        rationale["mem_request"] = (
            f"P75({mem_req_base / 1024**2:.0f}Mi) * headroom {self.mem_req_headroom}x"
        )
        if app.has_pandas:
            rationale["mem_request"] += f" * lib_factor {lib_factor}x (pandas/numpy detectado)"

        # Mínimo: 64Mi
        mem_request = max(mem_request, 64 * 1024 * 1024)

        # ── MEMÓRIA LIMIT ─────────────────────────────────────────────────
        is_legacy = app.python_version.startswith("2.")
        mem_headroom = (
            self.mem_limit_headroom_legacy if is_legacy else self.mem_limit_headroom
        )

        # Se leak detectado, usa P99 como base em vez de P75
        if app.memory_leak_detected:
            mem_base = app.mem_stats.p99
            mem_limit = int(mem_base * mem_headroom)
            rationale["mem_limit"] = (
                f"P99({mem_base / 1024**2:.0f}Mi) usado por memory leak detectado "
                f"* {mem_headroom}x"
            )
        else:
            mem_limit = int(mem_request * mem_headroom)
            rationale["mem_limit"] = (
                f"Request * {mem_headroom}x"
                + (" (Python 2.7: GC mais agressivo)" if is_legacy else "")
            )

        # OOMKills → aumenta limit em 30% extra
        if app.oom_kill_count > 0:
            old = mem_limit
            mem_limit = int(mem_limit * 1.30)
            rationale["mem_limit"] += (
                f"; +30% extra por {app.oom_kill_count} OOMKill(s) detectados"
            )

        # Mínimo: 128Mi
        mem_limit = max(mem_limit, 128 * 1024 * 1024)

        # ── DELTAS vs ATUAL ───────────────────────────────────────────────
        return ResourceRecommendation(
            app_name=app.app_name,
            cpu_request_cores=cpu_request,
            cpu_limit_cores=cpu_limit,
            mem_request_bytes=mem_request,
            mem_limit_bytes=mem_limit,
            cpu_request_str=_cores_to_str(cpu_request),
            cpu_limit_str=_cores_to_str(cpu_limit),
            mem_request_str=_bytes_to_mi(mem_request),
            mem_limit_str=_bytes_to_mi(mem_limit),
            cpu_request_delta_pct=_delta_pct(cpu_request, app.current_cpu_request_cores),
            cpu_limit_delta_pct=_delta_pct(cpu_limit, app.current_cpu_limit_cores),
            mem_request_delta_pct=_delta_pct(mem_request, app.current_mem_request_bytes),
            mem_limit_delta_pct=_delta_pct(mem_limit, app.current_mem_limit_bytes),
            rationale=rationale,
        )
