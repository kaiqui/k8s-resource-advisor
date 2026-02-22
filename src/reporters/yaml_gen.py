import textwrap
from pathlib import Path

from analyzers.profiler import AnalyzedApp
from calculators.hpa import HPARecommendation
from calculators.resources import ResourceRecommendation


def _comment_block(lines: list[str], indent: int = 0) -> str:
    pad = " " * indent
    return "\n".join(f"{pad}# {line}" for line in lines)


def generate_resources_patch(
    app: AnalyzedApp,
    resources: ResourceRecommendation,
    github_info=None,
) -> str:
    github_block = ""
    if github_info:
        gh_lines = [f"Fonte: GitHub {github_info.repo} ({github_info.default_branch})"]
        if github_info.python_version:
            gh_lines.append(f"Python detectado: {github_info.python_version} (Dockerfile)")
        if github_info.framework:
            gh_lines.append(f"Framework detectado: {github_info.framework}")
        if github_info.dependency_manager:
            gh_lines.append(f"Gerenciador de deps: {github_info.dependency_manager}")
        if github_info.cpu_request:
            gh_lines.append(
                f"Resources anteriores (deploy.yaml): "
                f"cpu={github_info.cpu_request}/{github_info.cpu_limit} "
                f"mem={github_info.mem_request}/{github_info.mem_limit}"
            )
        if github_info.hpa_min_replicas is not None:
            gh_lines.append(
                f"HPA anterior (deploy.yaml): "
                f"min={github_info.hpa_min_replicas} max={github_info.hpa_max_replicas} "
                f"cpu_target={github_info.hpa_cpu_target}%"
            )
        github_block = _comment_block(gh_lines) + "\n"

    warnings_block = ""
    if app.warnings:
        warning_lines = ["ATENÇÃO — problemas detectados na análise:"]
        for w in app.warnings:
            warning_lines.append(f"  [{w.severity.upper()}] {w.code}: {w.message}")
        warnings_block = _comment_block(warning_lines) + "\n"

    rationale_lines = ["Racional dos valores:"]
    for k, v in resources.rationale.items():
        rationale_lines.append(f"  {k}: {v}")
    rationale_block = _comment_block(rationale_lines)

    delta_lines = []
    if resources.cpu_request_delta_pct != 0:
        delta_lines.append(f"  cpu_request: {resources.cpu_request_delta_pct:+.0f}% vs atual")
    if resources.cpu_limit_delta_pct != 0:
        delta_lines.append(f"  cpu_limit:   {resources.cpu_limit_delta_pct:+.0f}% vs atual")
    if resources.mem_request_delta_pct != 0:
        delta_lines.append(f"  mem_request: {resources.mem_request_delta_pct:+.0f}% vs atual")
    if resources.mem_limit_delta_pct != 0:
        delta_lines.append(f"  mem_limit:   {resources.mem_limit_delta_pct:+.0f}% vs atual")

    delta_block = ""
    if delta_lines:
        delta_block = _comment_block(["Variação vs configuração atual:"] + delta_lines) + "\n"

    return textwrap.dedent(f"""\
        {github_block}{warnings_block}\
        {_comment_block([
            f"Resources Patch — {app.app_name}",
            f"Framework: {app.framework_type.value} | Python: {app.python_version}",
            f"Perfil: {app.app_profile.value} | Dados: {app.weeks_collected} semanas",
            f"Qualidade dos dados: {app.data_quality_score:.0%}",
            "",
            f"Aplique com:",
            f"  kubectl patch deployment {app.app_name} -n {app.namespace} --patch-file {app.app_name}-resources-patch.yaml",
        ])}
        {rationale_block}
        {delta_block}\
        ---
        apiVersion: apps/v1
        kind: Deployment
        metadata:
          name: {app.app_name}
          namespace: {app.namespace}
        spec:
          template:
            spec:
              containers:
              - name: {app.app_name}
                resources:
                  requests:
                    cpu: "{resources.cpu_request_str}"      # P75 real + headroom
                    memory: "{resources.mem_request_str}"   # P75 real + headroom
                  limits:
                    cpu: "{resources.cpu_limit_str}"        # burst {
                        'WSGI 2x' if app.framework_type.value == 'wsgi' else 'async 1.5x'
                    }
                    memory: "{resources.mem_limit_str}"     # {"limite conservador — OOMKills detectados" if app.oom_kill_count > 0 else "1.5x request"}
    """)


def generate_hpa_manifest(
    app: AnalyzedApp,
    hpa: HPARecommendation,
    resources: ResourceRecommendation,
    github_info=None,
) -> str:
    """Gera o HPA v2 completo."""

    rationale_lines = ["Racional do HPA:"]
    for k, v in hpa.rationale.items():
        rationale_lines.append(f"  {k}: {v}")

    keda_block = ""
    if hpa.keda_recommended:
        keda_block = _comment_block([
            "",
            "RECOMENDAÇÃO: Considere KEDA para esta app.",
            hpa.keda_reason,
        ]) + "\n"

    # Gera as policies de scale-up
    up_policies_yaml = ""
    for p in hpa.behavior.scale_up_policies:
        up_policies_yaml += (
            f"          - type: {p.type}\n"
            f"            value: {p.value}\n"
            f"            periodSeconds: {p.period_seconds}\n"
        )

    down_policies_yaml = ""
    for p in hpa.behavior.scale_down_policies:
        down_policies_yaml += (
            f"          - type: {p.type}\n"
            f"            value: {p.value}\n"
            f"            periodSeconds: {p.period_seconds}\n"
        )

    burst_label = 'WSGI 2x' if app.framework_type.value == 'wsgi' else 'async 1.5x'
    oom_comment = "limite conservador — OOMKills detectados" if app.oom_kill_count > 0 else "1.5x request"
    leak_comment = "conservador: leak detectado" if app.memory_leak_detected else "target padrão"

    hpa_yaml = f"""\
{_comment_block([
    f"HPA v2 — {app.app_name}",
    f"Framework: {app.framework_type.value} | Startup P95: {app.startup_p95_seconds:.0f}s",
    f"OOMKills: {app.oom_kill_count} | Throttle P95: {app.cpu_throttle_p95:.0%}",
    "",
    f"Aplique com:",
    f"  kubectl apply -f {app.app_name}-hpa.yaml",
])}
{_comment_block(rationale_lines)}
{keda_block}---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: {app.app_name}
  namespace: {app.namespace}
  annotations:
    k8s-resource-advisor/generated: "true"
    k8s-resource-advisor/framework: "{app.framework_type.value}"
    k8s-resource-advisor/data-weeks: "{app.weeks_collected}"
    k8s-resource-advisor/data-quality: "{app.data_quality_score:.0%}"
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: {app.app_name}
  minReplicas: {hpa.min_replicas}   # historico P10 + startup bump
  maxReplicas: {hpa.max_replicas}   # pico projetado com multiplicador {app.peak_traffic_multiplier}x
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: {hpa.cpu_target_utilization}   # knee / limit * safety_margin
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: {hpa.mem_target_utilization}   # {leak_comment}
  behavior:
    scaleUp:
      stabilizationWindowSeconds: {hpa.behavior.scale_up_stabilization_seconds}
      selectPolicy: Max
      policies:
{up_policies_yaml.rstrip()}
    scaleDown:
      stabilizationWindowSeconds: {hpa.behavior.scale_down_stabilization_seconds}   # conservador
      selectPolicy: Min
      policies:
{down_policies_yaml.rstrip()}
"""
    return hpa_yaml


def write_manifests(
    app: AnalyzedApp,
    resources: ResourceRecommendation,
    hpa: HPARecommendation,
    output_dir: str,
    github_info=None,
) -> tuple[Path, Path]:
    """Escreve os dois arquivos YAML e retorna os paths."""
    out = Path(output_dir) / "manifests"
    out.mkdir(parents=True, exist_ok=True)

    patch_path = out / f"{app.app_name}-resources-patch.yaml"
    hpa_path = out / f"{app.app_name}-hpa.yaml"

    patch_path.write_text(generate_resources_patch(app, resources, github_info=github_info))
    hpa_path.write_text(generate_hpa_manifest(app, hpa, resources, github_info=github_info))

    return patch_path, hpa_path
