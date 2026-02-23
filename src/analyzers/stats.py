"""
analyzers/stats.py

Processa a lista de ServiceAudit e produz todas as estatísticas
para o relatório: KPIs, distribuições, rankings, comparações por framework.
"""

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Optional

from collectors.github import ServiceAudit, Issue, _parse_cores, _parse_bytes


@dataclass
class OrgStats:
    """Resultado completo da análise de todos os serviços."""

    total: int = 0
    scanned: int = 0          # com deploy.yaml + Dockerfile
    skipped: int = 0

    # ── KPIs principais ──────────────────────────────────────────────────
    pct_no_resources: float = 0.0
    pct_no_hpa: float = 0.0
    pct_no_readiness: float = 0.0
    pct_has_both: float = 0.0       # resources + HPA configurados
    pct_fully_ok: float = 0.0       # sem nenhum issue critical

    # ── Distribuições ────────────────────────────────────────────────────
    by_framework: dict = field(default_factory=dict)        # framework -> count
    by_python: dict = field(default_factory=dict)           # "3.11" -> count
    by_dep_manager: dict = field(default_factory=dict)      # "poetry"|"pip" -> count
    by_namespace: dict = field(default_factory=dict)        # namespace -> count

    # Resources
    pct_has_cpu_request: float = 0.0
    pct_has_cpu_limit: float = 0.0
    pct_has_mem_request: float = 0.0
    pct_has_mem_limit: float = 0.0
    pct_full_resources: float = 0.0  # todos os 4 campos

    # HPA
    pct_has_hpa: float = 0.0
    hpa_cpu_target_dist: dict = field(default_factory=dict)   # bucket -> count
    hpa_replicas_dist: dict = field(default_factory=dict)     # min_max -> count

    # Issues por severidade
    issues_by_code: dict = field(default_factory=dict)    # code -> count
    issues_by_severity: dict = field(default_factory=dict)

    # ── Rankings ─────────────────────────────────────────────────────────
    most_critical: list = field(default_factory=list)     # [(name, n_critical)]
    most_issues: list = field(default_factory=list)       # [(name, n_issues)]
    framework_issue_rate: dict = field(default_factory=dict)  # framework -> % com issues

    # ── Detalhes por framework ────────────────────────────────────────────
    framework_details: dict = field(default_factory=dict)   # framework -> FrameworkStats

    # Lista completa de serviços (para tabela)
    services: list = field(default_factory=list)


@dataclass
class FrameworkStats:
    name: str
    count: int = 0
    pct_no_resources: float = 0.0
    pct_no_hpa: float = 0.0
    pct_no_readiness: float = 0.0
    pct_no_mem_limit: float = 0.0
    avg_cpu_request_m: Optional[float] = None
    avg_mem_request_mi: Optional[float] = None
    avg_hpa_min: Optional[float] = None
    avg_hpa_max: Optional[float] = None


def analyze(audits: list[ServiceAudit]) -> OrgStats:
    """Calcula todas as estatísticas a partir da lista de ServiceAudit."""
    stats = OrgStats()

    all_audits = audits
    stats.total   = len(all_audits)
    stats.skipped = sum(1 for a in all_audits if a.skipped)

    active = [a for a in all_audits if not a.skipped]
    stats.scanned = len(active)

    if not active:
        return stats

    n = stats.scanned

    def pct(count): return round(count / n * 100, 1)

    # ── Resources ────────────────────────────────────────────────────────
    no_resources  = [a for a in active if _all_none(a.cpu_request, a.cpu_limit, a.mem_request, a.mem_limit)]
    has_cpu_req   = [a for a in active if a.cpu_request]
    has_cpu_lim   = [a for a in active if a.cpu_limit]
    has_mem_req   = [a for a in active if a.mem_request]
    has_mem_lim   = [a for a in active if a.mem_limit]
    full_resources= [a for a in active if all([a.cpu_request, a.cpu_limit, a.mem_request, a.mem_limit])]

    stats.pct_no_resources   = pct(len(no_resources))
    stats.pct_has_cpu_request = pct(len(has_cpu_req))
    stats.pct_has_cpu_limit   = pct(len(has_cpu_lim))
    stats.pct_has_mem_request = pct(len(has_mem_req))
    stats.pct_has_mem_limit   = pct(len(has_mem_lim))
    stats.pct_full_resources  = pct(len(full_resources))

    # ── HPA ──────────────────────────────────────────────────────────────
    has_hpa  = [a for a in active if a.hpa_min_replicas is not None]
    no_hpa   = [a for a in active if a.hpa_min_replicas is None]

    stats.pct_has_hpa = pct(len(has_hpa))
    stats.pct_no_hpa  = pct(len(no_hpa))

    # HPA CPU target distribution
    target_buckets = Counter()
    for a in has_hpa:
        if a.hpa_cpu_target is not None:
            t = a.hpa_cpu_target
            if t <= 50:   bucket = "≤50%"
            elif t <= 70: bucket = "51–70%"
            elif t <= 85: bucket = "71–85%"
            else:         bucket = ">85%"
            target_buckets[bucket] += 1
    stats.hpa_cpu_target_dist = dict(target_buckets)

    # ── Probes ────────────────────────────────────────────────────────────
    no_readiness  = [a for a in active if not a.has_readiness_probe]
    stats.pct_no_readiness = pct(len(no_readiness))

    # ── KPIs compostos ───────────────────────────────────────────────────
    has_both  = [a for a in active if a.cpu_request and a.hpa_min_replicas is not None]
    no_critical = [a for a in active if not any(i.severity == "critical" for i in a.issues)]

    stats.pct_has_both  = pct(len(has_both))
    stats.pct_fully_ok  = pct(len(no_critical))

    # ── Distribuições categóricas ─────────────────────────────────────────
    stats.by_framework   = _count_dist(active, lambda a: a.framework or "unknown")
    stats.by_python      = _count_dist(active, lambda a: _major_minor(a.python_version))
    stats.by_dep_manager = _count_dist(active, lambda a: a.dependency_manager or "unknown")
    stats.by_namespace   = _count_dist(active, lambda a: a.k8s_namespace or "unknown")

    # ── Issues ────────────────────────────────────────────────────────────
    code_counter     = Counter()
    severity_counter = Counter()
    for a in active:
        for issue in a.issues:
            code_counter[issue.code] += 1
            severity_counter[issue.severity] += 1

    stats.issues_by_code     = dict(code_counter.most_common())
    stats.issues_by_severity = dict(severity_counter)

    # ── Rankings ─────────────────────────────────────────────────────────
    by_critical = sorted(
        [(a.name, sum(1 for i in a.issues if i.severity == "critical")) for a in active],
        key=lambda x: -x[1],
    )
    stats.most_critical = [x for x in by_critical if x[1] > 0][:10]

    by_issues = sorted(
        [(a.name, len(a.issues)) for a in active],
        key=lambda x: -x[1],
    )
    stats.most_issues = [x for x in by_issues if x[1] > 0][:10]

    # ── Por framework ─────────────────────────────────────────────────────
    fw_groups = defaultdict(list)
    for a in active:
        fw_groups[a.framework or "unknown"].append(a)

    fw_details = {}
    fw_issue_rate = {}
    for fw, group in fw_groups.items():
        ng = len(group)
        fs = FrameworkStats(name=fw, count=ng)
        fs.pct_no_resources = round(sum(1 for a in group if _all_none(a.cpu_request, a.cpu_limit, a.mem_request, a.mem_limit)) / ng * 100, 1)
        fs.pct_no_hpa       = round(sum(1 for a in group if a.hpa_min_replicas is None) / ng * 100, 1)
        fs.pct_no_readiness = round(sum(1 for a in group if not a.has_readiness_probe) / ng * 100, 1)
        fs.pct_no_mem_limit = round(sum(1 for a in group if not a.mem_limit) / ng * 100, 1)

        cpu_vals = [_parse_cores(a.cpu_request) * 1000 for a in group if a.cpu_request]
        mem_vals = [_parse_bytes(a.mem_request) / 1024**2 for a in group if a.mem_request]
        hpa_mins = [a.hpa_min_replicas for a in group if a.hpa_min_replicas is not None]
        hpa_maxs = [a.hpa_max_replicas for a in group if a.hpa_max_replicas is not None]

        if cpu_vals: fs.avg_cpu_request_m  = round(sum(cpu_vals) / len(cpu_vals), 0)
        if mem_vals: fs.avg_mem_request_mi = round(sum(mem_vals) / len(mem_vals), 0)
        if hpa_mins: fs.avg_hpa_min        = round(sum(hpa_mins) / len(hpa_mins), 1)
        if hpa_maxs: fs.avg_hpa_max        = round(sum(hpa_maxs) / len(hpa_maxs), 1)

        fw_details[fw] = fs
        fw_issue_rate[fw] = round(sum(1 for a in group if a.issues) / ng * 100, 1)

    stats.framework_details   = fw_details
    stats.framework_issue_rate = fw_issue_rate

    # ── Lista de serviços (ordenada por criticidade) ───────────────────────
    def svc_score(a):
        crit = sum(1 for i in a.issues if i.severity == "critical")
        warn = sum(1 for i in a.issues if i.severity == "warning")
        return -(crit * 100 + warn)

    stats.services = sorted(active, key=svc_score)

    return stats


# ── Helpers ───────────────────────────────────────────────────────────────────

def _all_none(*vals) -> bool:
    return all(v is None for v in vals)


def _count_dist(items, key_fn) -> dict:
    c = Counter(key_fn(i) for i in items)
    return dict(c.most_common())


def _major_minor(ver: Optional[str]) -> str:
    if not ver:
        return "unknown"
    parts = str(ver).split(".")
    if len(parts) >= 2:
        return f"{parts[0]}.{parts[1]}"
    return ver