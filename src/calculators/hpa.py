from dataclasses import dataclass

from analyzers.profiler import AnalyzedApp, FrameworkType
from calculators.resources import ResourceRecommendation


@dataclass
class HPABehaviorPolicy:
    type: str           # "Pods" ou "Percent"
    value: int
    period_seconds: int


@dataclass
class HPABehavior:
    scale_up_stabilization_seconds: int
    scale_down_stabilization_seconds: int
    scale_up_policies: list[HPABehaviorPolicy]
    scale_down_policies: list[HPABehaviorPolicy]


@dataclass
class HPARecommendation:
    app_name: str
    min_replicas: int
    max_replicas: int
    cpu_target_utilization: int     # percentual do limit (0-100)
    mem_target_utilization: int
    behavior: HPABehavior
    rationale: dict[str, str]

    # Flags extras
    keda_recommended: bool = False
    keda_reason: str = ""


class HPACalculator:

    def __init__(self, thresholds: dict | None = None):
        th = thresholds or {}
        self.safety_margin = th.get("hpa_cpu_safety_margin", 0.70)
        self.mem_target = th.get("hpa_mem_target", 75)
        self.scaleup_window = th.get("hpa_scaleup_window_seconds", 30)
        self.scaledown_window = th.get("hpa_scaledown_window_seconds", 300)

    def calculate(
        self,
        app: AnalyzedApp,
        resources: ResourceRecommendation,
        current_hpa: dict | None = None,
    ) -> HPARecommendation:
        rationale = {}
        current_hpa = current_hpa or {}

        # ── MIN REPLICAS ─────────────────────────────────────────────────
        # P10 de réplicas históricas (carga baixa mas não mínima)
        import numpy as np
        min_from_history = 2
        if app.avg_replicas > 0:
            min_from_history = max(2, int(np.percentile(
                [app.avg_replicas * 0.5, app.avg_replicas], 10
            )))

        # Startup lento → mínimo maior para não depender de scale-up
        startup_bump = 0
        if app.startup_p95_seconds > 60:
            startup_bump = 2
        elif app.startup_p95_seconds > 30:
            startup_bump = 1

        min_replicas = max(2, min_from_history + startup_bump)

        # Respeita override do usuário
        if hasattr(app, "_min_replicas_override") and app._min_replicas_override:
            min_replicas = max(min_replicas, app._min_replicas_override)

        rationale["min_replicas"] = (
            f"Base: {min_from_history} (histórico P10); "
            f"startup bump: +{startup_bump} (startup P95={app.startup_p95_seconds:.0f}s)"
        )

        # ── MAX REPLICAS ─────────────────────────────────────────────────
        # Quantas réplicas para absorver o pico projetado?
        # pico_rps / rps_por_pod_no_target_cpu
        #
        # Se não temos RPS: usamos réplicas históricas * multiplicador de pico
        if app.rps_per_pod_p95 > 0 and app.avg_replicas > 0:
            rps_total_peak = app.rps_per_pod_p95 * app.avg_replicas * app.peak_traffic_multiplier
            # Por pod no pico = rps_per_pod_p95 (esse é o limite que queremos manter)
            max_from_rps = int(np.ceil(rps_total_peak / max(app.rps_per_pod_p95, 1)))
            rationale["max_replicas"] = (
                f"RPS pico projetado: {rps_total_peak:.0f}/s "
                f"/ {app.rps_per_pod_p95:.1f} RPS/pod = {max_from_rps} réplicas"
            )
        else:
            max_from_rps = int(np.ceil(app.peak_replicas * app.peak_traffic_multiplier))
            rationale["max_replicas"] = (
                f"Réplicas pico históricas ({app.peak_replicas:.0f}) "
                f"* multiplicador {app.peak_traffic_multiplier}x = {max_from_rps}"
            )

        max_replicas = max(min_replicas + 2, max_from_rps)

        # ── CPU TARGET ───────────────────────────────────────────────────
        # Queremos escalar ANTES de atingir o ponto de saturação.
        # target = (knee_point / cpu_limit) * safety_margin * 100
        #
        # Se não temos knee point confiável, usamos P95/limit * safety_margin.
        cpu_limit = resources.cpu_limit_cores
        if app.cpu_knee_point_cores > 0 and cpu_limit > 0:
            raw_target = (app.cpu_knee_point_cores / cpu_limit) * self.safety_margin
        elif cpu_limit > 0:
            raw_target = (app.cpu_stats.p95 / cpu_limit) * self.safety_margin
        else:
            raw_target = 0.60  # fallback 60%

        # Clipa entre 40% e 80%
        cpu_target = int(max(40, min(80, raw_target * 100)))
        rationale["cpu_target"] = (
            f"knee({app.cpu_knee_point_cores:.3f}) / limit({cpu_limit:.3f}) "
            f"* safety_margin({self.safety_margin}) = {raw_target:.0%} → clampado em {cpu_target}%"
        )

        # ── MEMORY TARGET ─────────────────────────────────────────────────
        # Memória no k8s não tem throttle — OOM mata o pod.
        # Target conservador: 75% do limit.
        # Se leak detectado: 65% (espaço para GC não devolver antes de escalar)
        mem_target = 65 if app.memory_leak_detected else self.mem_target
        rationale["mem_target"] = (
            f"{mem_target}%"
            + (" (reduzido por memory leak detectado)" if app.memory_leak_detected else "")
        )

        # ── BEHAVIOR ─────────────────────────────────────────────────────
        behavior = self._compute_behavior(app)

        # ── KEDA? ────────────────────────────────────────────────────────
        keda_recommended = False
        keda_reason = ""
        if app.startup_p95_seconds > 90:
            keda_recommended = True
            keda_reason = (
                f"Startup P95 de {app.startup_p95_seconds:.0f}s é muito lento para "
                "reatividade de HPA padrão. KEDA com ScaledObject permite scale-to-zero "
                "e integração direta com métricas de fila/RPS do Datadog."
            )
        elif app.rps_per_pod_p95 == 0 and app.app_profile.value == "cpu_bound":
            keda_recommended = True
            keda_reason = (
                "App CPU-bound sem instrumentação de RPS. "
                "KEDA com métrica customizada do Datadog (APM ou queue length) "
                "dará targets mais precisos que CPU alone."
            )

        return HPARecommendation(
            app_name=app.app_name,
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            cpu_target_utilization=cpu_target,
            mem_target_utilization=mem_target,
            behavior=behavior,
            rationale=rationale,
            keda_recommended=keda_recommended,
            keda_reason=keda_reason,
        )

    def _compute_behavior(self, app: AnalyzedApp) -> HPABehavior:
        """
        Scale-up rápido e scale-down conservador.
        Startup lento → adiciona mais pods de uma vez no scale-up.
        """
        # Scale-up: quantos pods adicionar por vez?
        # Startup rápido (<30s): 2 pods por ciclo
        # Startup médio (30-60s): 4 pods por ciclo (compensa espera)
        # Startup lento (>60s): 20% dos pods por ciclo
        if app.startup_p95_seconds > 60:
            scale_up_policies = [
                HPABehaviorPolicy(type="Percent", value=20, period_seconds=60),
                HPABehaviorPolicy(type="Pods", value=4, period_seconds=60),
            ]
            scale_up_stab = 0  # reage imediatamente
        elif app.startup_p95_seconds > 30:
            scale_up_policies = [
                HPABehaviorPolicy(type="Pods", value=4, period_seconds=30),
            ]
            scale_up_stab = 15
        else:
            scale_up_policies = [
                HPABehaviorPolicy(type="Pods", value=2, period_seconds=30),
            ]
            scale_up_stab = self.scaleup_window

        # Scale-down: sempre conservador
        # WSGI: mais conservador (conexões longas)
        # Async: pode ser um pouco mais agressivo
        if app.framework_type == FrameworkType.WSGI:
            scale_down_stab = max(self.scaledown_window, 300)  # mínimo 5min
        else:
            scale_down_stab = self.scaledown_window

        scale_down_policies = [
            HPABehaviorPolicy(type="Percent", value=10, period_seconds=60),
        ]

        return HPABehavior(
            scale_up_stabilization_seconds=scale_up_stab,
            scale_down_stabilization_seconds=scale_down_stab,
            scale_up_policies=scale_up_policies,
            scale_down_policies=scale_down_policies,
        )
