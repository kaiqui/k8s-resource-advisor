"""
reporters/report.py

Gera relatório HTML com:
  - Resumo executivo por app
  - Tabela comparativa (atual vs recomendado)
  - Gráficos de CPU e memória (distribuição e série temporal)
  - Warnings e diagnósticos
  - Links para os YAMLs gerados
"""

from __future__ import annotations

import json
from pathlib import Path

from jinja2 import Template

from analyzers.profiler import AnalyzedApp
from calculators.hpa import HPARecommendation
from calculators.resources import ResourceRecommendation


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>k8s Resource Advisor — Relatório</title>
<style>
  :root {
    --blue: #2563eb; --green: #16a34a; --red: #dc2626;
    --yellow: #ca8a04; --gray: #6b7280; --light: #f9fafb;
    --border: #e5e7eb;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: #f3f4f6; color: #111827; line-height: 1.5; }
  .container { max-width: 1280px; margin: 0 auto; padding: 24px; }
  h1 { font-size: 1.875rem; font-weight: 700; margin-bottom: 4px; }
  h2 { font-size: 1.25rem; font-weight: 600; margin: 24px 0 12px; }
  h3 { font-size: 1rem; font-weight: 600; margin-bottom: 8px; color: var(--gray); }
  .subtitle { color: var(--gray); font-size: 0.875rem; margin-bottom: 24px; }
  .card { background: white; border-radius: 8px; border: 1px solid var(--border);
          padding: 20px; margin-bottom: 20px; }
  .app-header { display: flex; align-items: center; gap: 12px; margin-bottom: 16px; }
  .badge { font-size: 0.75rem; padding: 2px 10px; border-radius: 9999px; font-weight: 500; }
  .badge-blue { background: #dbeafe; color: #1e40af; }
  .badge-green { background: #dcfce7; color: #166534; }
  .badge-yellow { background: #fef9c3; color: #854d0e; }
  .badge-red { background: #fee2e2; color: #991b1b; }
  .badge-gray { background: #f3f4f6; color: #374151; }
  table { width: 100%; border-collapse: collapse; font-size: 0.875rem; }
  th { text-align: left; padding: 8px 12px; background: var(--light);
       border-bottom: 1px solid var(--border); font-weight: 600; color: var(--gray); }
  td { padding: 8px 12px; border-bottom: 1px solid var(--border); }
  tr:last-child td { border-bottom: none; }
  .delta-pos { color: var(--green); font-weight: 500; }
  .delta-neg { color: var(--red); font-weight: 500; }
  .delta-neu { color: var(--gray); }
  .warn-critical { border-left: 3px solid var(--red); padding: 8px 12px;
                   background: #fff5f5; border-radius: 0 4px 4px 0; margin-bottom: 8px; }
  .warn-warning { border-left: 3px solid var(--yellow); padding: 8px 12px;
                  background: #fffbeb; border-radius: 0 4px 4px 0; margin-bottom: 8px; }
  .warn-info { border-left: 3px solid var(--blue); padding: 8px 12px;
               background: #eff6ff; border-radius: 0 4px 4px 0; margin-bottom: 8px; }
  .warn-label { font-size: 0.75rem; font-weight: 700; text-transform: uppercase;
                letter-spacing: 0.05em; margin-bottom: 2px; }
  .warn-critical .warn-label { color: var(--red); }
  .warn-warning .warn-label { color: var(--yellow); }
  .warn-info .warn-label { color: var(--blue); }
  .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
  .yaml-cmd { background: #1e293b; color: #94a3b8; padding: 12px 16px;
              border-radius: 6px; font-family: monospace; font-size: 0.8rem; margin-top: 8px; }
  .yaml-cmd span { color: #38bdf8; }
  .stats-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; margin-bottom: 16px; }
  .stat-box { background: var(--light); border-radius: 6px; padding: 10px 14px; }
  .stat-label { font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.05em;
                color: var(--gray); margin-bottom: 2px; }
  .stat-value { font-size: 1.25rem; font-weight: 700; }
  .keda-box { background: #f0f9ff; border: 1px solid #bae6fd; border-radius: 6px;
              padding: 12px 16px; margin-top: 12px; }
  .keda-box h3 { color: #0369a1; margin-bottom: 4px; }
  summary { cursor: pointer; font-weight: 600; color: var(--blue); padding: 4px 0; }
  details { margin-top: 8px; }
  .rationale { font-size: 0.8rem; color: var(--gray); font-family: monospace;
               background: var(--light); padding: 8px; border-radius: 4px; }
</style>
</head>
<body>
<div class="container">
  <h1>k8s Resource Advisor</h1>
  <p class="subtitle">
    Gerado em {{ generated_at }} &nbsp;|&nbsp;
    {{ apps|length }} aplicações analisadas &nbsp;|&nbsp;
    Janela: {{ weeks }} semanas
  </p>

  {% for item in apps %}
  <div class="card">
    <div class="app-header">
      <h2 style="margin:0">{{ item.app.app_name }}</h2>
      <span class="badge badge-{{ 'blue' if item.app.framework_type.value == 'async' else 'gray' }}">
        {{ item.app.framework_type.value.upper() }}
      </span>
      <span class="badge badge-gray">Python {{ item.app.python_version }}</span>
      <span class="badge badge-{{ 'red' if item.app.app_profile.value == 'cpu_bound' else 'blue' if item.app.app_profile.value == 'io_bound' else 'yellow' }}">
        {{ item.app.app_profile.value.replace('_', ' ').upper() }}
      </span>
      {% if item.app.oom_kill_count > 0 %}
      <span class="badge badge-red">⚠ {{ item.app.oom_kill_count }} OOMKills</span>
      {% endif %}
      {% if item.app.cpu_throttle_p95 > 0.1 %}
      <span class="badge badge-yellow">⚠ Throttle {{ "%.0f"|format(item.app.cpu_throttle_p95*100) }}%</span>
      {% endif %}
    </div>

    <!-- Stats rápidos -->
    <div class="stats-grid">
      <div class="stat-box">
        <div class="stat-label">CPU P95</div>
        <div class="stat-value">{{ "%.0f"|format(item.app.cpu_stats.p95 * 1000) }}m</div>
      </div>
      <div class="stat-box">
        <div class="stat-label">Mem P95</div>
        <div class="stat-value">{{ "%.0f"|format(item.app.mem_stats.p95 / 1024 / 1024) }}Mi</div>
      </div>
      <div class="stat-box">
        <div class="stat-label">Réplicas avg/max</div>
        <div class="stat-value">{{ "%.1f"|format(item.app.avg_replicas) }} / {{ "%.0f"|format(item.app.peak_replicas) }}</div>
      </div>
      <div class="stat-box">
        <div class="stat-label">Startup P95</div>
        <div class="stat-value">{{ "%.0f"|format(item.app.startup_p95_seconds) }}s</div>
      </div>
    </div>

    <!-- Warnings -->
    {% for w in item.app.warnings %}
    <div class="warn-{{ w.severity }}">
      <div class="warn-label">{{ w.severity }} — {{ w.code }}</div>
      {{ w.message }}
    </div>
    {% endfor %}

    <!-- Tabela comparativa -->
    <div class="grid-2" style="margin-top:16px">
      <div>
        <h3>CPU & Memória — Resources</h3>
        <table>
          <tr>
            <th>Parâmetro</th><th>Atual</th><th>Recomendado</th><th>Δ</th>
          </tr>
          <tr>
            <td>cpu.request</td>
            <td>{{ "%.0f"|format(item.app.current_cpu_request_cores * 1000) }}m</td>
            <td><strong>{{ item.resources.cpu_request_str }}</strong></td>
            <td class="{{ 'delta-pos' if item.resources.cpu_request_delta_pct > 0 else 'delta-neg' if item.resources.cpu_request_delta_pct < 0 else 'delta-neu' }}">
              {{ "%+.0f"|format(item.resources.cpu_request_delta_pct) }}%
            </td>
          </tr>
          <tr>
            <td>cpu.limit</td>
            <td>{{ "%.0f"|format(item.app.current_cpu_limit_cores * 1000) }}m</td>
            <td><strong>{{ item.resources.cpu_limit_str }}</strong></td>
            <td class="{{ 'delta-pos' if item.resources.cpu_limit_delta_pct > 0 else 'delta-neg' if item.resources.cpu_limit_delta_pct < 0 else 'delta-neu' }}">
              {{ "%+.0f"|format(item.resources.cpu_limit_delta_pct) }}%
            </td>
          </tr>
          <tr>
            <td>memory.request</td>
            <td>{{ "%.0f"|format(item.app.current_mem_request_bytes / 1024 / 1024) }}Mi</td>
            <td><strong>{{ item.resources.mem_request_str }}</strong></td>
            <td class="{{ 'delta-pos' if item.resources.mem_request_delta_pct > 0 else 'delta-neg' if item.resources.mem_request_delta_pct < 0 else 'delta-neu' }}">
              {{ "%+.0f"|format(item.resources.mem_request_delta_pct) }}%
            </td>
          </tr>
          <tr>
            <td>memory.limit</td>
            <td>{{ "%.0f"|format(item.app.current_mem_limit_bytes / 1024 / 1024) }}Mi</td>
            <td><strong>{{ item.resources.mem_limit_str }}</strong></td>
            <td class="{{ 'delta-pos' if item.resources.mem_limit_delta_pct > 0 else 'delta-neg' if item.resources.mem_limit_delta_pct < 0 else 'delta-neu' }}">
              {{ "%+.0f"|format(item.resources.mem_limit_delta_pct) }}%
            </td>
          </tr>
        </table>
      </div>

      <div>
        <h3>HPA</h3>
        <table>
          <tr><th>Parâmetro</th><th>Recomendado</th></tr>
          <tr><td>minReplicas</td><td><strong>{{ item.hpa.min_replicas }}</strong></td></tr>
          <tr><td>maxReplicas</td><td><strong>{{ item.hpa.max_replicas }}</strong></td></tr>
          <tr><td>CPU target</td><td><strong>{{ item.hpa.cpu_target_utilization }}%</strong></td></tr>
          <tr><td>Mem target</td><td><strong>{{ item.hpa.mem_target_utilization }}%</strong></td></tr>
          <tr><td>scaleUp window</td><td>{{ item.hpa.behavior.scale_up_stabilization_seconds }}s</td></tr>
          <tr><td>scaleDown window</td><td>{{ item.hpa.behavior.scale_down_stabilization_seconds }}s</td></tr>
        </table>
      </div>
    </div>

    <!-- KEDA -->
    {% if item.hpa.keda_recommended %}
    <div class="keda-box">
      <h3>💡 KEDA Recomendado</h3>
      <p style="font-size:0.875rem">{{ item.hpa.keda_reason }}</p>
    </div>
    {% endif %}

    <!-- Comandos YAML -->
    <details style="margin-top:16px">
      <summary>Ver comandos de aplicação</summary>
      <div class="yaml-cmd">
        <span>kubectl</span> patch deployment {{ item.app.app_name }} -n {{ item.app.namespace }} \\<br>
        &nbsp;&nbsp;--patch-file <span>manifests/{{ item.app.app_name }}-resources-patch.yaml</span><br><br>
        <span>kubectl</span> apply -f <span>manifests/{{ item.app.app_name }}-hpa.yaml</span>
      </div>
    </details>

    <!-- Racional detalhado -->
    <details style="margin-top:8px">
      <summary>Ver racional dos cálculos</summary>
      <div class="rationale" style="margin-top:8px">
        <strong>Resources:</strong><br>
        {% for k, v in item.resources.rationale.items() %}
        &nbsp;&nbsp;{{ k }}: {{ v }}<br>
        {% endfor %}
        <br>
        <strong>HPA:</strong><br>
        {% for k, v in item.hpa.rationale.items() %}
        &nbsp;&nbsp;{{ k }}: {{ v }}<br>
        {% endfor %}
      </div>
    </details>
  </div>
  {% endfor %}
</div>
</body>
</html>
"""


def generate_report(
    results: list[tuple[AnalyzedApp, ResourceRecommendation, HPARecommendation]],
    output_path: str,
    weeks: int = 4,
) -> Path:
    from datetime import datetime, timezone

    items = [
        {"app": app, "resources": res, "hpa": hpa}
        for app, res, hpa in results
    ]

    html = Template(HTML_TEMPLATE).render(
        apps=items,
        generated_at=datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        weeks=weeks,
    )

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html)
    return out