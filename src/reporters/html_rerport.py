"""
reporters/html_report.py — Relatório HTML da auditoria K8s

Seções:
  1. Header + KPI cards
  2. Gráficos: donuts SVG (pizza) para cobertura + barras para distribuições
  3. Por framework — tabela comparativa
  4. Distribuição global de issues
  5. Rankings — top mais críticos
  6. Alertas por app — seção dedicada com expand/collapse por app
  7. Tabela filtrável — todos os serviços
"""

from __future__ import annotations
import math
from datetime import datetime, timezone
from pathlib import Path

from analyzers.stats import OrgStats
from collectors.github import ServiceAudit

SEVERITY_COLOR = {"critical": "#ef4444", "warning": "#f59e0b", "info": "#60a5fa"}

ISSUE_META: dict[str, tuple[str, str]] = {
    "NO_RESOURCES":       ("Sem nenhum resource configurado",                           "critical"),
    "NO_CPU_REQUEST":     ("CPU request não definido",                                  "warning"),
    "NO_CPU_LIMIT":       ("CPU limit não definido",                                    "warning"),
    "NO_MEM_REQUEST":     ("Memory request não definido",                               "warning"),
    "NO_MEM_LIMIT":       ("Memory limit não definido — risco de OOMKill",              "critical"),
    "HIGH_CPU_BURST":     ("CPU limit > 4× request — request pode estar subconfigurado","warning"),
    "MEM_NO_HEADROOM":    ("Mem limit ≤ request — qualquer pico causa OOMKill",         "critical"),
    "NO_HPA":             ("Sem HPA — réplicas fixas, sem auto-escalonamento",          "warning"),
    "HPA_USELESS":        ("minReplicas ≥ maxReplicas — HPA nunca escala",              "warning"),
    "SPOF":               ("minReplicas = 1 — single point of failure",                 "critical"),
    "HPA_NO_TARGET":      ("HPA sem target de CPU ou memória — nunca escalará",         "warning"),
    "HPA_TARGET_HIGH":    ("CPU target > 85% — HPA escala tarde demais",                "warning"),
    "NO_READINESS_PROBE": ("Sem readiness probe — tráfego roteado a pods não-prontos",  "warning"),
    "NO_LIVENESS_PROBE":  ("Sem liveness probe — pods travados não reiniciam",          "info"),
    "UNKNOWN_PYTHON":     ("Versão Python não detectada (imagem custom)",               "info"),
}

FW_COLORS = ["#6366f1","#0ea5e9","#10b981","#f59e0b","#ef4444","#8b5cf6","#ec4899","#14b8a6","#f97316","#a3e635"]


# ── Ponto de entrada ──────────────────────────────────────────────────────────

def generate_report(
    audits: list[ServiceAudit],
    stats: OrgStats,
    output_path: str,
    org: str = "",
) -> str:
    now  = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    html = _build(audits, stats, org, now)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text(html, encoding="utf-8")
    return output_path


# ── Builder principal ─────────────────────────────────────────────────────────

def _build(audits: list[ServiceAudit], s: OrgStats, org: str, now: str) -> str:
    active = [a for a in audits if not a.skipped]
    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>K8s Audit — {org}</title>
<style>{_CSS}</style>
</head>
<body>
{_header(org, now, s)}
{_kpi_cards(s)}
{_charts_row(s)}
{_framework_table(s)}
{_issues_distribution(s)}
{_rankings(s)}
{_app_alerts(active)}
{_services_table(s)}
{_footer(now)}
<script>{_JS}</script>
</body>
</html>"""


# ── 1. Header ─────────────────────────────────────────────────────────────────

def _header(org: str, now: str, s: OrgStats) -> str:
    return f"""
<header class="hdr">
  <div>
    <div class="hdr-eyebrow">Kubernetes Configuration Audit</div>
    <h1 class="hdr-title">{org or "Organization"}</h1>
  </div>
  <div class="hdr-meta">
    <div>Gerado em <b>{now}</b></div>
    <div><b>{s.total}</b> repos service-* · <b>{s.scanned}</b> analisados · <b>{s.skipped}</b> pulados</div>
  </div>
</header>"""


# ── 2. KPI cards ──────────────────────────────────────────────────────────────

def _kpi_cards(s: OrgStats) -> str:
    def card(label, value, sub, variant=""):
        return f"""<div class="kpi kpi-{variant}">
      <div class="kpi-value">{value}</div>
      <div class="kpi-label">{label}</div>
      <div class="kpi-sub">{sub}</div>
    </div>"""

    no_res_v = "crit" if s.pct_no_resources > 30 else ("warn" if s.pct_no_resources > 10 else "ok")
    no_hpa_v = "warn" if s.pct_no_hpa > 20 else "ok"
    no_rd_v  = "warn" if s.pct_no_readiness > 30 else "ok"
    ok_v     = "ok"  if s.pct_fully_ok >= 70 else ("warn" if s.pct_fully_ok >= 40 else "crit")

    n = s.scanned
    return f"""<div class="kpi-strip">
  {card("Serviços analisados", s.scanned,              f"{s.skipped} pulados",                        "white")}
  {card("Sem resources",       f"{s.pct_no_resources}%", f"{int(n*s.pct_no_resources/100)} apps",    no_res_v)}
  {card("Sem HPA",             f"{s.pct_no_hpa}%",       f"{int(n*s.pct_no_hpa/100)} apps",          no_hpa_v)}
  {card("Sem readiness probe", f"{s.pct_no_readiness}%", f"{int(n*s.pct_no_readiness/100)} apps",    no_rd_v)}
  {card("Resources completos", f"{s.pct_full_resources}%","cpu+mem req+lim",                          "accent")}
  {card("Sem issues críticos", f"{s.pct_fully_ok}%",     f"{int(n*s.pct_fully_ok/100)} apps",        ok_v)}
</div>"""


# ── 3. Gráficos ───────────────────────────────────────────────────────────────

def _charts_row(s: OrgStats) -> str:
    n = s.scanned

    # Donut 1 — 4 anéis concêntricos de cobertura de resources
    donut1 = _donut_rings(
        "Cobertura de Resources",
        [
            ("CPU Request",  s.pct_has_cpu_request,  "#6366f1"),
            ("CPU Limit",    s.pct_has_cpu_limit,     "#0ea5e9"),
            ("Mem Request",  s.pct_has_mem_request,   "#10b981"),
            ("Mem Limit",    s.pct_has_mem_limit,     "#f59e0b"),
        ],
        center_label="Cobertura",
        center_value=f"{s.pct_full_resources:.0f}%",
    )

    # Donut 2 — pizza HPA (tem / não tem)
    has_hpa = int(n * s.pct_has_hpa / 100)
    no_hpa  = n - has_hpa
    donut2 = _donut_pie(
        "HPA",
        [("Com HPA", has_hpa, "#10b981"), ("Sem HPA", no_hpa, "#ef4444")],
        n,
        center_label="HPA",
        center_value=f"{s.pct_has_hpa:.0f}%",
    )

    # Donut 3 — pizza Readiness probe
    has_rd = n - int(n * s.pct_no_readiness / 100)
    no_rd  = n - has_rd
    donut3 = _donut_pie(
        "Readiness Probe",
        [("Com probe", has_rd, "#0ea5e9"), ("Sem probe", no_rd, "#f59e0b")],
        n,
        center_label="Probe",
        center_value=f"{100-s.pct_no_readiness:.0f}%",
    )

    # Barras — frameworks
    fw_bars = _bar_chart_card("Frameworks", s.by_framework, n, FW_COLORS)

    # Barras — Python version
    py_colors = ["#6366f1","#8b5cf6","#a78bfa","#c4b5fd","#ddd6fe","#ede9fe"]
    py_bars = _bar_chart_card("Python version", s.by_python, n, py_colors)

    return f"""
<section class="sec">
  <h2 class="sec-title">Visão geral</h2>
  <div class="charts-row">
    {donut1}
    <div class="charts-donuts-pair">
      {donut2}
      {donut3}
    </div>
    {fw_bars}
    {py_bars}
  </div>
</section>"""


# ── SVG helpers ───────────────────────────────────────────────────────────────

def _arc_path(cx, cy, r_out, r_in, start_angle, end_angle) -> str:
    """Gera path SVG de um arco (anel) de start_angle até end_angle (radianos)."""
    def pt(a, r):
        return cx + r * math.cos(a), cy + r * math.sin(a)

    x1o, y1o = pt(start_angle, r_out)
    x2o, y2o = pt(end_angle,   r_out)
    x2i, y2i = pt(end_angle,   r_in)
    x1i, y1i = pt(start_angle, r_in)
    large = 1 if (end_angle - start_angle) > math.pi else 0

    return (f"M {x1o:.3f} {y1o:.3f} "
            f"A {r_out:.1f} {r_out:.1f} 0 {large} 1 {x2o:.3f} {y2o:.3f} "
            f"L {x2i:.3f} {y2i:.3f} "
            f"A {r_in:.1f} {r_in:.1f} 0 {large} 0 {x1i:.3f} {y1i:.3f} Z")


def _donut_rings(title: str, items: list[tuple], center_label: str, center_value: str) -> str:
    """
    Donut com N anéis concêntricos — cada anel representa % independente.
    Anel mais externo = primeiro item.
    """
    cx, cy  = 80, 80
    r_start = 68
    ring_w  = 10
    gap     = 4
    TOP     = -math.pi / 2

    shapes = ""
    legend = ""
    for i, (label, pct, color) in enumerate(items):
        r_out = r_start - i * (ring_w + gap)
        r_in  = r_out - ring_w
        frac  = max(0.001, min(0.9999, pct / 100))

        # Track (background)
        shapes += f'<circle cx="{cx}" cy="{cy}" r="{(r_out+r_in)/2:.1f}" fill="none" stroke="#1a1a2e" stroke-width="{ring_w}"/>'
        # Fill arc
        path = _arc_path(cx, cy, r_out, r_in, TOP, TOP + 2*math.pi*frac)
        shapes += f'<path d="{path}" fill="{color}"/>'

        legend += f"""<div class="dl-row">
      <span class="dl-dot" style="background:{color}"></span>
      <span class="dl-name">{label}</span>
      <span class="dl-val" style="color:{color}">{pct}%</span>
    </div>"""

    return f"""<div class="chart-card">
  <div class="chart-title">{title}</div>
  <div class="donut-wrap">
    <svg viewBox="0 0 160 160" width="160" height="160">
      {shapes}
      <text x="80" y="76" text-anchor="middle" class="dc-label">{center_label}</text>
      <text x="80" y="92" text-anchor="middle" class="dc-value">{center_value}</text>
    </svg>
    <div class="dl">{legend}</div>
  </div>
</div>"""


def _donut_pie(title: str, slices: list[tuple], total: int, center_label: str, center_value: str) -> str:
    """
    Pizza/donut clássica com fatias proporcionais.
    slices = [(label, count, color), ...]
    """
    cx, cy  = 70, 70
    r_out   = 62
    r_in    = 40
    TOP     = -math.pi / 2

    shapes = ""
    legend = ""
    angle  = TOP
    for label, count, color in slices:
        frac = max(0.001, count / total) if total else 0.001
        frac = min(frac, 0.9999)
        end  = angle + 2 * math.pi * frac
        path = _arc_path(cx, cy, r_out, r_in, angle, end)
        shapes += f'<path d="{path}" fill="{color}"/>'
        angle = end
        pct = round(count / total * 100, 1) if total else 0
        legend += f"""<div class="dl-row">
      <span class="dl-dot" style="background:{color}"></span>
      <span class="dl-name">{label}</span>
      <span class="dl-val" style="color:{color}">{count} ({pct}%)</span>
    </div>"""

    return f"""<div class="chart-card chart-card-sm">
  <div class="chart-title">{title}</div>
  <div class="donut-wrap">
    <svg viewBox="0 0 140 140" width="140" height="140">
      {shapes}
      <text x="70" y="66" text-anchor="middle" class="dc-label">{center_label}</text>
      <text x="70" y="81" text-anchor="middle" class="dc-value">{center_value}</text>
    </svg>
    <div class="dl">{legend}</div>
  </div>
</div>"""


def _bar_chart_card(title: str, data: dict, total: int, palette: list) -> str:
    if not data:
        return f'<div class="chart-card"><div class="chart-title">{title}</div><span class="muted">Sem dados</span></div>'
    items  = list(data.items())[:9]
    max_v  = max(v for _, v in items)
    rows   = ""
    for i, (label, val) in enumerate(items):
        color = palette[i % len(palette)]
        w     = val / max_v * 100
        pct   = round(val / total * 100, 1) if total else 0
        rows += f"""<div class="brow">
      <div class="blabel" title="{label}">{label}</div>
      <div class="btrack"><div class="bfill" style="width:{w:.0f}%;background:{color}"></div></div>
      <div class="bval">{val} <span class="muted">({pct}%)</span></div>
    </div>"""
    return f"""<div class="chart-card">
  <div class="chart-title">{title}</div>
  <div class="bar-list">{rows}</div>
</div>"""


# ── 4. Por framework ──────────────────────────────────────────────────────────

def _framework_table(s: OrgStats) -> str:
    if not s.framework_details:
        return ""

    fw_keys = list(s.framework_details.keys())

    def pc(val, warn, crit, hi_bad=True):
        if hi_bad:
            c = "#ef4444" if val >= crit else ("#f59e0b" if val >= warn else "#10b981")
        else:
            c = "#10b981" if val >= crit else ("#f59e0b" if val >= warn else "#ef4444")
        return f'<span class="mono" style="color:{c}">{val}%</span>'

    rows = ""
    for fw, fs in sorted(s.framework_details.items(), key=lambda x: -x[1].count):
        idx   = fw_keys.index(fw) % len(FW_COLORS)
        color = FW_COLORS[idx]
        ir    = s.framework_issue_rate.get(fw, 0)
        cpu_s = f"{fs.avg_cpu_request_m:.0f}m"   if fs.avg_cpu_request_m  else "—"
        mem_s = f"{fs.avg_mem_request_mi:.0f}Mi"  if fs.avg_mem_request_mi else "—"
        hpa_s = f"{fs.avg_hpa_min:.0f}↔{fs.avg_hpa_max:.0f}" if fs.avg_hpa_min else "—"
        rows += f"""<tr>
      <td><span class="fw-badge" style="background:{color}22;color:{color};border-color:{color}55">{fw}</span></td>
      <td class="r mono">{fs.count}</td>
      <td class="r">{pc(fs.pct_no_resources, 10, 30)}</td>
      <td class="r">{pc(fs.pct_no_hpa, 20, 50)}</td>
      <td class="r">{pc(fs.pct_no_readiness, 30, 60)}</td>
      <td class="r">{pc(fs.pct_no_mem_limit, 10, 30)}</td>
      <td class="r mono muted">{cpu_s}</td>
      <td class="r mono muted">{mem_s}</td>
      <td class="r mono muted">{hpa_s}</td>
      <td class="r">{pc(ir, 40, 80)}</td>
    </tr>"""

    return f"""
<section class="sec">
  <h2 class="sec-title">Por framework</h2>
  <div class="tbl-wrap">
  <table class="gtable">
    <thead><tr>
      <th>Framework</th><th class="r">Apps</th>
      <th class="r">Sem resources</th><th class="r">Sem HPA</th>
      <th class="r">Sem readiness</th><th class="r">Sem mem limit</th>
      <th class="r">CPU req médio</th><th class="r">Mem req médio</th>
      <th class="r">HPA min↔max</th><th class="r">Taxa issues</th>
    </tr></thead>
    <tbody>{rows}</tbody>
  </table>
  </div>
</section>"""


# ── 5. Distribuição de issues ─────────────────────────────────────────────────

def _issues_distribution(s: OrgStats) -> str:
    if not s.issues_by_code:
        return ""

    sev_pills = " ".join(
        f'<span class="badge sev-{sev}">{cnt} {sev}</span>'
        for sev in ["critical", "warning", "info"]
        if (cnt := s.issues_by_severity.get(sev, 0))
    )

    max_c = max(s.issues_by_code.values())
    rows  = ""
    for code, count in s.issues_by_code.items():
        desc, sev = ISSUE_META.get(code, (code, "info"))
        pct       = round(count / s.scanned * 100, 1)
        w         = count / max_c * 100
        color     = SEVERITY_COLOR.get(sev, "#6b6b8a")
        rows += f"""<div class="irow">
      <div class="icode"><span class="badge sev-{sev}">{code}</span></div>
      <div class="idesc muted">{desc}</div>
      <div class="ibar"><div class="ibar-fill" style="width:{w:.0f}%;background:{color}"></div></div>
      <div class="inum mono">{count}</div>
      <div class="ipct mono" style="color:{color}">{pct}%</div>
    </div>"""

    return f"""
<section class="sec">
  <h2 class="sec-title">Distribuição de issues <span class="sec-meta">{sev_pills}</span></h2>
  <div class="issue-dist">{rows}</div>
</section>"""


# ── 6. Rankings ───────────────────────────────────────────────────────────────

def _rankings(s: OrgStats) -> str:
    def rank_block(items, badge_cls, suffix, empty_msg):
        if not items:
            return f'<p class="muted" style="padding:12px 0">{empty_msg}</p>'
        html = ""
        for i, (name, n) in enumerate(items[:8], 1):
            html += f"""<div class="rank-row">
          <span class="rank-n">{i}</span>
          <span class="rank-name">{name}</span>
          <span class="badge {badge_cls}">{n} {suffix}</span>
        </div>"""
        return html

    return f"""
<section class="sec">
  <h2 class="sec-title">Rankings</h2>
  <div class="grid2">
    <div>
      <div class="subsec-title">Mais issues críticos</div>
      {rank_block(s.most_critical, "sev-critical", "crítico(s)", "Nenhum serviço com issues críticos ✓")}
    </div>
    <div>
      <div class="subsec-title">Mais issues no total</div>
      {rank_block(s.most_issues, "sev-warning", "issue(s)", "Nenhum serviço com issues ✓")}
    </div>
  </div>
</section>"""


# ── 7. Alertas por app ────────────────────────────────────────────────────────

def _app_alerts(active: list[ServiceAudit]) -> str:
    """
    Seção dedicada: uma entrada por app, ordenada por criticidade.
    Cada entrada mostra os chips de issues e expande para detalhes ao clicar.
    Apps sem issues ficam agrupadas numa lista colapsada ao final.
    """
    with_issues = sorted(
        [a for a in active if a.issues],
        key=lambda a: (
            -sum(1 for i in a.issues if i.severity == "critical"),
            -sum(1 for i in a.issues if i.severity == "warning"),
            -len(a.issues),
        ),
    )
    without = [a for a in active if not a.issues]

    cards = ""
    for a in with_issues:
        crits = [i for i in a.issues if i.severity == "critical"]
        warns = [i for i in a.issues if i.severity == "warning"]
        infos = [i for i in a.issues if i.severity == "info"]

        # Severity pill no header
        pills = ""
        if crits: pills += f'<span class="sev-pill sev-critical">{len(crits)}C</span>'
        if warns: pills += f'<span class="sev-pill sev-warning">{len(warns)}W</span>'
        if infos: pills += f'<span class="sev-pill sev-info">{len(infos)}I</span>'

        # Chips de código (max 5 visíveis)
        visible = (crits + warns + infos)[:5]
        chips   = "".join(f'<span class="badge sev-{i.severity}" title="{ISSUE_META.get(i.code,("",""))[0]}">{i.code}</span>' for i in visible)
        extra   = len(a.issues) - len(visible)
        if extra > 0:
            chips += f'<span class="badge sev-info">+{extra}</span>'

        # Linhas de detalhe (expandível)
        detail = ""
        for i in a.issues:
            desc, _ = ISSUE_META.get(i.code, (i.message, i.severity))
            detail += f"""<div class="detail-row">
          <span class="badge sev-{i.severity}">{i.code}</span>
          <span class="detail-msg">{desc}</span>
        </div>"""

        # Badges de contexto
        ctx = f'<span class="ctx-badge ctx-fw">{a.framework or "?"}</span>'
        ctx += f'<span class="ctx-badge ctx-py">py{a.python_version or "?"}</span>'
        if a.k8s_namespace:
            ctx += f'<span class="ctx-badge ctx-ns">{a.k8s_namespace}</span>'

        cards += f"""<div class="alert-card" onclick="toggleCard(this)">
      <div class="alert-hdr">
        <div class="alert-left">
          <div class="alert-pills">{pills}</div>
          <span class="alert-name">{a.name}</span>
          <div class="alert-ctx">{ctx}</div>
        </div>
        <div class="alert-chips">{chips}</div>
        <span class="alert-chevron">▾</span>
      </div>
      <div class="alert-body" hidden>
        <div class="detail-list">{detail}</div>
      </div>
    </div>"""

    # Apps sem issues
    ok_block = ""
    if without:
        chips_ok = "".join(f'<span class="ok-chip">{a.name}</span>' for a in without)
        ok_block = f"""<div class="ok-block">
      <div class="ok-header">
        <span class="badge sev-ok">✓ {len(without)} sem issues</span>
      </div>
      <div class="ok-chips">{chips_ok}</div>
    </div>"""

    if not with_issues and not ok_block:
        return ""

    meta = f"{len(with_issues)} apps com issues · {len(without)} OK"
    return f"""
<section class="sec">
  <h2 class="sec-title">Alertas por app <span class="sec-meta">{meta}</span></h2>
  <div class="alerts-list">{cards}</div>
  {ok_block}
</section>"""


# ── 8. Tabela filtrável ───────────────────────────────────────────────────────

def _services_table(s: OrgStats) -> str:
    rows = ""
    for a in s.services:
        cpu_s = f"{a.cpu_request}/{a.cpu_limit}" if (a.cpu_request or a.cpu_limit) else None
        mem_s = f"{a.mem_request}/{a.mem_limit}" if (a.mem_request or a.mem_limit) else None
        hpa_s = None
        if a.hpa_min_replicas is not None:
            tgt   = f"@{a.hpa_cpu_target}%" if a.hpa_cpu_target else ""
            hpa_s = f"{a.hpa_min_replicas}/{a.hpa_max_replicas}{tgt}"

        crits = sum(1 for i in a.issues if i.severity == "critical")
        warns = sum(1 for i in a.issues if i.severity == "warning")
        chips = ""
        if crits: chips += f'<span class="badge sev-critical">{crits}C</span>'
        if warns: chips += f'<span class="badge sev-warning">{warns}W</span>'
        if not a.issues: chips = '<span class="badge sev-ok">OK</span>'

        probes = ("R" if a.has_readiness_probe else "·") + ("L" if a.has_liveness_probe else "·")
        probe_color = "#10b981" if (a.has_readiness_probe and a.has_liveness_probe) else ("#f59e0b" if (a.has_readiness_probe or a.has_liveness_probe) else "#ef4444")
        nd = '<span class="muted">—</span>'

        rows += f"""<tr
      data-name="{a.name}"
      data-fw="{a.framework or ''}"
      data-crits="{crits}"
      data-issues="{len(a.issues)}"
      data-hpa="{0 if a.hpa_min_replicas is None else 1}"
      data-res="{0 if not a.cpu_request else 1}">
      <td class="t-name">{a.name}</td>
      <td><span class="fw-badge" style="background:{FW_COLORS[0]}22;color:{FW_COLORS[0]};border-color:{FW_COLORS[0]}55">{a.framework or '—'}</span></td>
      <td class="mono">{a.python_version or '—'}</td>
      <td class="mono muted">{a.k8s_namespace or '—'}</td>
      <td class="mono">{cpu_s or nd}</td>
      <td class="mono">{mem_s or nd}</td>
      <td class="mono">{hpa_s or nd}</td>
      <td class="mono" style="color:{probe_color}">{probes}</td>
      <td><div class="chip-row">{chips}</div></td>
    </tr>"""

    return f"""
<section class="sec">
  <h2 class="sec-title">Todos os serviços <span class="sec-meta">{s.scanned} apps</span></h2>
  <div class="tbl-controls">
    <input id="tsearch" class="tsearch" placeholder="Filtrar por nome ou framework…" oninput="filterTable()">
    <button class="fbtn active" onclick="setF('all',this)">Todos</button>
    <button class="fbtn" onclick="setF('crit',this)">Com críticos</button>
    <button class="fbtn" onclick="setF('nores',this)">Sem resources</button>
    <button class="fbtn" onclick="setF('nohpa',this)">Sem HPA</button>
    <button class="fbtn" onclick="setF('ok',this)">Sem issues</button>
  </div>
  <div class="tbl-scroll">
    <table class="gtable" id="stbl">
      <thead><tr>
        <th>Serviço</th><th>Framework</th><th>Python</th><th>Namespace</th>
        <th>CPU req/lim</th><th>Mem req/lim</th><th>HPA</th><th>Probes</th><th>Issues</th>
      </tr></thead>
      <tbody id="stbody">{rows}</tbody>
    </table>
  </div>
</section>"""


# ── Footer ────────────────────────────────────────────────────────────────────

def _footer(now: str) -> str:
    return f'<footer class="ftr">k8s-audit · {now} · GitHub → deploy.yaml · Dockerfile · pyproject.toml</footer>'


# ══════════════════════════════════════════════════════════════════════════════
# CSS
# ══════════════════════════════════════════════════════════════════════════════

_CSS = """
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=Syne:wght@400;600;700;800&display=swap');

:root {
  --bg:     #08080f;
  --surf:   #0f0f1a;
  --surf2:  #14141f;
  --bdr:    #1a1a2e;
  --bdr2:   #252540;
  --txt:    #d4d4e8;
  --muted:  #5a5a7a;
  --accent: #6366f1;
  --ok:     #10b981;
  --warn:   #f59e0b;
  --crit:   #ef4444;
  --info:   #60a5fa;
  --mono:   'IBM Plex Mono', monospace;
  --sans:   'Syne', sans-serif;
}
*,*::before,*::after { box-sizing: border-box; margin: 0; padding: 0; }
html { scroll-behavior: smooth; }
body {
  background: var(--bg);
  color: var(--txt);
  font-family: var(--sans);
  font-size: 14px;
  line-height: 1.55;
}

/* ── Header ── */
.hdr {
  padding: 36px 48px 28px;
  border-bottom: 1px solid var(--bdr);
  display: flex;
  justify-content: space-between;
  align-items: flex-end;
  gap: 24px;
}
.hdr-eyebrow {
  font-family: var(--mono);
  font-size: 10px;
  letter-spacing: .22em;
  text-transform: uppercase;
  color: var(--accent);
  margin-bottom: 6px;
}
.hdr-title {
  font-size: 30px;
  font-weight: 800;
  letter-spacing: -.03em;
  color: #fff;
}
.hdr-meta {
  font-family: var(--mono);
  font-size: 11px;
  color: var(--muted);
  text-align: right;
  line-height: 2;
}
.hdr-meta b { color: var(--txt); }

/* ── KPI strip ── */
.kpi-strip {
  display: grid;
  grid-template-columns: repeat(6,1fr);
  border-bottom: 1px solid var(--bdr);
}
.kpi {
  padding: 22px 20px;
  border-right: 1px solid var(--bdr);
}
.kpi:last-child { border-right: none; }
.kpi-value {
  font-family: var(--mono);
  font-size: 34px;
  font-weight: 600;
  line-height: 1;
  margin-bottom: 5px;
}
.kpi-label {
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: .1em;
  color: var(--muted);
}
.kpi-sub {
  font-family: var(--mono);
  font-size: 10px;
  color: var(--muted);
  margin-top: 3px;
}
.kpi-white  .kpi-value { color: #fff; }
.kpi-crit   .kpi-value { color: var(--crit); }
.kpi-warn   .kpi-value { color: var(--warn); }
.kpi-ok     .kpi-value { color: var(--ok); }
.kpi-accent .kpi-value { color: var(--accent); }

/* ── Sections ── */
.sec {
  padding: 36px 48px;
  border-bottom: 1px solid var(--bdr);
}
.sec-title {
  font-family: var(--mono);
  font-size: 10px;
  letter-spacing: .2em;
  text-transform: uppercase;
  color: var(--accent);
  margin-bottom: 24px;
  display: flex;
  align-items: center;
  gap: 12px;
}
.sec-title::after { content: ''; flex: 1; height: 1px; background: var(--bdr); }
.sec-meta {
  color: var(--muted);
  font-weight: 400;
  letter-spacing: 0;
  text-transform: none;
  font-size: 11px;
  display: flex;
  align-items: center;
  gap: 6px;
  flex-shrink: 0;
}
.subsec-title {
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: .1em;
  color: var(--muted);
  margin-bottom: 12px;
  font-family: var(--mono);
}

/* ── Charts ── */
.charts-row {
  display: grid;
  grid-template-columns: 260px 220px 1fr 1fr;
  gap: 16px;
  align-items: start;
}
.charts-donuts-pair {
  display: flex;
  flex-direction: column;
  gap: 12px;
}
.chart-card {
  background: var(--surf);
  border: 1px solid var(--bdr);
  border-radius: 6px;
  padding: 18px;
}
.chart-card-sm { padding: 14px 18px; }
.chart-title {
  font-family: var(--mono);
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: .1em;
  color: var(--muted);
  margin-bottom: 14px;
}
.donut-wrap { display: flex; align-items: center; gap: 14px; }
.dc-label { fill: var(--muted); font-family: var(--mono); font-size: 9px; }
.dc-value { fill: #fff; font-family: var(--mono); font-size: 13px; font-weight: 600; }
.dl { flex: 1; display: flex; flex-direction: column; gap: 6px; min-width: 0; }
.dl-row { display: flex; align-items: center; gap: 6px; font-size: 11px; min-width: 0; }
.dl-dot { width: 7px; height: 7px; border-radius: 50%; flex-shrink: 0; }
.dl-name { flex: 1; color: var(--txt); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.dl-val { font-family: var(--mono); font-size: 11px; white-space: nowrap; }

/* Bars */
.bar-list { display: flex; flex-direction: column; gap: 8px; }
.brow { display: grid; grid-template-columns: 72px 1fr 72px; gap: 8px; align-items: center; }
.blabel { font-family: var(--mono); font-size: 11px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.btrack { background: var(--bdr2); border-radius: 2px; height: 5px; overflow: hidden; }
.bfill { height: 100%; border-radius: 2px; }
.bval { font-family: var(--mono); font-size: 11px; text-align: right; }

/* ── Generic table ── */
.tbl-wrap { overflow-x: auto; }
.tbl-scroll {
  max-height: 580px;
  overflow-y: auto;
  border: 1px solid var(--bdr);
  border-radius: 6px;
}
.tbl-scroll::-webkit-scrollbar { width: 5px; }
.tbl-scroll::-webkit-scrollbar-thumb { background: var(--bdr2); border-radius: 3px; }
.gtable { width: 100%; border-collapse: collapse; font-size: 12px; }
.gtable th {
  font-family: var(--mono);
  font-size: 10px;
  letter-spacing: .08em;
  text-transform: uppercase;
  color: var(--muted);
  padding: 9px 13px;
  border-bottom: 1px solid var(--bdr2);
  text-align: left;
  white-space: nowrap;
  position: sticky;
  top: 0;
  background: var(--bg);
  z-index: 1;
}
.gtable th.r, .gtable td.r { text-align: right; }
.gtable td {
  padding: 10px 13px;
  border-bottom: 1px solid var(--bdr);
  vertical-align: middle;
}
.gtable tr:last-child td { border-bottom: none; }
.gtable tr:hover td { background: rgba(99,102,241,.04); }
.t-name { font-weight: 700; font-size: 13px; color: #fff; max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }

/* ── Issue distribution ── */
.issue-dist { display: flex; flex-direction: column; gap: 5px; }
.irow {
  display: grid;
  grid-template-columns: 210px 1fr 180px 48px 60px;
  gap: 10px;
  align-items: center;
  padding: 4px 0;
}
.idesc { font-size: 11px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.ibar { background: var(--bdr2); border-radius: 2px; height: 5px; overflow: hidden; }
.ibar-fill { height: 100%; border-radius: 2px; }
.inum { font-size: 11px; text-align: right; }
.ipct { font-size: 11px; text-align: right; }

/* ── Rankings ── */
.grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 32px; }
.rank-row {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 9px 12px;
  background: var(--surf);
  border: 1px solid var(--bdr);
  border-radius: 4px;
  margin-bottom: 6px;
}
.rank-n { font-family: var(--mono); font-size: 10px; color: var(--muted); width: 18px; text-align: right; flex-shrink: 0; }
.rank-name { flex: 1; font-size: 13px; font-weight: 600; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }

/* ── App alerts ── */
.alerts-list { display: flex; flex-direction: column; gap: 5px; margin-bottom: 16px; }
.alert-card {
  background: var(--surf);
  border: 1px solid var(--bdr);
  border-radius: 5px;
  overflow: hidden;
  cursor: pointer;
  transition: border-color .15s;
}
.alert-card:hover { border-color: var(--bdr2); }
.alert-card.open  { border-color: var(--accent); }
.alert-hdr {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 11px 14px;
}
.alert-left {
  display: flex;
  align-items: center;
  gap: 8px;
  flex: 1;
  min-width: 0;
  overflow: hidden;
}
.alert-pills { display: flex; gap: 4px; flex-shrink: 0; }
.alert-name  { font-size: 13px; font-weight: 700; color: #fff; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.alert-ctx   { display: flex; gap: 4px; flex-shrink: 0; }
.alert-chips { display: flex; flex-wrap: wrap; gap: 4px; flex-shrink: 0; max-width: 340px; justify-content: flex-end; }
.alert-chevron { color: var(--muted); font-size: 13px; flex-shrink: 0; transition: transform .2s; }
.alert-card.open .alert-chevron { transform: rotate(180deg); }
.alert-body { border-top: 1px solid var(--bdr); }
.detail-list { padding: 8px 14px; display: flex; flex-direction: column; gap: 0; }
.detail-row {
  display: flex;
  align-items: flex-start;
  gap: 10px;
  padding: 8px 0;
  border-bottom: 1px solid var(--bdr);
}
.detail-row:last-child { border-bottom: none; }
.detail-msg { font-size: 12px; color: var(--txt); padding-top: 2px; }

/* Severity pills (compact, no border) */
.sev-pill {
  font-family: var(--mono);
  font-size: 10px;
  font-weight: 700;
  padding: 2px 5px;
  border-radius: 3px;
  flex-shrink: 0;
}
.sev-pill.sev-critical { background: #450a0a; color: #ef4444; }
.sev-pill.sev-warning  { background: #451a03; color: #f59e0b; }
.sev-pill.sev-info     { background: #0c1a2e; color: #60a5fa; }

/* OK block */
.ok-block {
  background: var(--surf);
  border: 1px solid var(--bdr);
  border-radius: 5px;
  padding: 14px 16px;
}
.ok-header { margin-bottom: 10px; }
.ok-chips  { display: flex; flex-wrap: wrap; gap: 6px; }
.ok-chip {
  font-family: var(--mono);
  font-size: 10px;
  background: #052e16;
  color: var(--ok);
  border: 1px solid #064e3b;
  padding: 2px 7px;
  border-radius: 3px;
}

/* Badges (with border) */
.badge {
  display: inline-flex;
  align-items: center;
  padding: 2px 7px;
  border-radius: 3px;
  font-family: var(--mono);
  font-size: 10px;
  font-weight: 600;
  letter-spacing: .04em;
  border: 1px solid transparent;
  white-space: nowrap;
}
.sev-critical { background: #450a0a; color: #ef4444; border-color: #7f1d1d; }
.sev-warning  { background: #451a03; color: #f59e0b; border-color: #78350f; }
.sev-info     { background: #0c1a2e; color: #60a5fa; border-color: #1e3a5f; }
.sev-ok       { background: #052e16; color: #10b981; border-color: #064e3b; }

/* Context badges */
.ctx-badge {
  font-family: var(--mono);
  font-size: 10px;
  padding: 1px 6px;
  border-radius: 3px;
  border: 1px solid;
  white-space: nowrap;
}
.ctx-fw { background: rgba(99,102,241,.12); color: #a5b4fc; border-color: rgba(99,102,241,.3); }
.ctx-py { background: rgba(14,165,233,.1);  color: #7dd3fc; border-color: rgba(14,165,233,.25); }
.ctx-ns { background: rgba(107,114,128,.08); color: var(--muted); border-color: var(--bdr2); }

/* Framework badge (custom color via inline) */
.fw-badge {
  display: inline-block;
  font-family: var(--mono);
  font-size: 10px;
  font-weight: 600;
  padding: 2px 7px;
  border-radius: 3px;
  border: 1px solid;
  white-space: nowrap;
}

/* Filter controls */
.tbl-controls { display: flex; gap: 10px; margin-bottom: 14px; align-items: center; flex-wrap: wrap; }
.tsearch {
  background: var(--surf);
  border: 1px solid var(--bdr2);
  border-radius: 4px;
  padding: 7px 13px;
  color: var(--txt);
  font-family: var(--mono);
  font-size: 12px;
  width: 260px;
  outline: none;
}
.tsearch:focus { border-color: var(--accent); }
.tsearch::placeholder { color: var(--muted); }
.fbtn {
  background: var(--surf);
  border: 1px solid var(--bdr2);
  border-radius: 4px;
  padding: 6px 13px;
  color: var(--muted);
  font-family: var(--mono);
  font-size: 11px;
  cursor: pointer;
  letter-spacing: .05em;
  transition: all .12s;
}
.fbtn:hover, .fbtn.active {
  border-color: var(--accent);
  color: var(--accent);
  background: rgba(99,102,241,.1);
}

/* Chips row */
.chip-row { display: flex; flex-wrap: wrap; gap: 4px; }

/* Utils */
.mono  { font-family: var(--mono); }
.muted { color: var(--muted); }
.r     { text-align: right; }

/* Footer */
.ftr {
  padding: 20px 48px;
  border-top: 1px solid var(--bdr);
  font-family: var(--mono);
  font-size: 11px;
  color: var(--muted);
}

/* Responsive */
@media (max-width: 1300px) {
  .charts-row { grid-template-columns: 1fr 1fr; }
  .charts-donuts-pair { flex-direction: row; }
  .irow { grid-template-columns: 170px 1fr 100px 40px 54px; }
}
@media (max-width: 900px) {
  .kpi-strip { grid-template-columns: repeat(3,1fr); }
  .grid2 { grid-template-columns: 1fr; }
  .hdr { flex-direction: column; align-items: flex-start; }
  .charts-row { grid-template-columns: 1fr 1fr; }
}
@media (max-width: 600px) {
  .kpi-strip { grid-template-columns: repeat(2,1fr); }
  .charts-row { grid-template-columns: 1fr; }
  .sec { padding: 24px 20px; }
  .hdr { padding: 24px 20px; }
}
"""

# ══════════════════════════════════════════════════════════════════════════════
# JS
# ══════════════════════════════════════════════════════════════════════════════

_JS = """
// Expand/collapse alert cards
function toggleCard(el) {
  el.classList.toggle('open');
  var body = el.querySelector('.alert-body');
  if (body) body.hidden = !body.hidden;
}

// Table filter
var _activeFilter = 'all';
function filterTable() {
  var q = document.getElementById('tsearch').value.toLowerCase();
  document.querySelectorAll('#stbody tr').forEach(function(r) {
    var name   = (r.getAttribute('data-name') || '').toLowerCase();
    var fw     = (r.getAttribute('data-fw')   || '').toLowerCase();
    var crits  = parseInt(r.getAttribute('data-crits')  || '0');
    var issues = parseInt(r.getAttribute('data-issues') || '0');
    var hpa    = parseInt(r.getAttribute('data-hpa')    || '0');
    var res    = parseInt(r.getAttribute('data-res')    || '0');

    var matchSearch = !q || name.includes(q) || fw.includes(q);
    var matchFilter = true;
    if      (_activeFilter === 'crit')  matchFilter = crits  >  0;
    else if (_activeFilter === 'nores') matchFilter = res    === 0;
    else if (_activeFilter === 'nohpa') matchFilter = hpa    === 0;
    else if (_activeFilter === 'ok')    matchFilter = issues === 0;

    r.style.display = (matchSearch && matchFilter) ? '' : 'none';
  });
}
function setF(f, btn) {
  _activeFilter = f;
  document.querySelectorAll('.fbtn').forEach(function(b) { b.classList.remove('active'); });
  btn.classList.add('active');
  filterTable();
}
"""