"""
collectors/github.py

Descobre todos os repositórios service-* da organização GitHub e extrai
o estado atual das configurações Kubernetes de cada um:
  - Resources (cpu/mem request + limit)
  - HPA (min/max replicas, cpu/mem target)
  - Liveness/readiness probes
  - Framework Python detectado
  - Python version

Regras de descoberta (idênticas ao projeto anterior):
  - Nome começa com "service-"
  - Tem Dockerfile na raiz
  - Tem manifests/kubernetes/main/deploy.yaml
"""

import base64
import re
import time
from dataclasses import dataclass, field
from typing import Optional, Iterator

try:
    import requests as _requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    import yaml as _yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False


# ── Resultado por serviço ────────────────────────────────────────────────────

@dataclass
class ServiceAudit:
    """Tudo que foi coletado de um repositório service-*."""

    # Identificação
    repo: str                           # "org/service-eligibility"
    name: str                           # "service-eligibility"
    default_branch: str = "main"
    dd_service: Optional[str] = None
    k8s_namespace: Optional[str] = None

    # Runtime detectado
    python_version: Optional[str] = None
    framework: Optional[str] = None
    dependency_manager: Optional[str] = None   # "poetry" | "pip"
    has_pandas: bool = False
    has_celery: bool = False

    # Resources — None = não configurado
    cpu_request: Optional[str] = None
    cpu_limit: Optional[str] = None
    mem_request: Optional[str] = None
    mem_limit: Optional[str] = None

    # HPA — None = sem HPA
    hpa_min_replicas: Optional[int] = None
    hpa_max_replicas: Optional[int] = None
    hpa_cpu_target: Optional[int] = None
    hpa_mem_target: Optional[int] = None

    # Probes
    has_liveness_probe: bool = False
    has_readiness_probe: bool = False
    has_startup_probe: bool = False

    # Réplicas fixas no Deployment
    replicas: Optional[int] = None

    # Diagnósticos / issues encontrados
    issues: list = field(default_factory=list)       # lista de Issue (code, severity, message)
    files_found: list = field(default_factory=list)
    files_missing: list = field(default_factory=list)

    # Metadados
    skipped: bool = False
    skip_reason: str = ""
    scan_ok: bool = True


@dataclass
class Issue:
    code: str
    severity: str   # "critical" | "warning" | "info"
    message: str


# ── Collector ────────────────────────────────────────────────────────────────

class GitHubAuditCollector:
    """Coleta estado atual das configs K8s de repositórios service-* via GitHub API."""

    BASE_URL        = "https://api.github.com"
    DEPLOY_PATH     = "manifests/kubernetes/main/deploy.yaml"
    DOCKERFILE_PATH = "Dockerfile"
    PYPROJECT_PATH  = "pyproject.toml"
    REPO_PREFIX     = "service-"

    def __init__(self, token: Optional[str] = None, base_url: Optional[str] = None):
        if not HAS_REQUESTS:
            raise ImportError("pip install requests")
        import os
        self._token = token or os.environ.get("GITHUB_TOKEN")
        self._base  = (base_url or self.BASE_URL).rstrip("/")
        self._session = _requests.Session()
        self._session.headers.update({
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "k8s-audit/1.0",
        })
        if self._token:
            self._session.headers["Authorization"] = f"Bearer {self._token}"

    # ── Descoberta ────────────────────────────────────────────────────────

    def scan_org(
        self,
        org: str,
        repo_filter: Optional[str] = None,
        skip_archived: bool = True,
        skip_forks: bool = True,
        max_repos: int = 500,
    ) -> list[ServiceAudit]:
        results = []
        checked = 0

        for repo_meta in self._iter_repos(org, skip_archived, skip_forks):
            if checked >= max_repos:
                break
            name = repo_meta["name"]
            if not name.startswith(self.REPO_PREFIX):
                continue
            checked += 1
            if repo_filter and repo_filter.lower() not in name.lower():
                continue

            branch = repo_meta.get("default_branch", "main")
            audit  = self._scan_repo(org, name, branch)
            results.append(audit)
            time.sleep(0.05)

        return results

    def scan_one(self, owner: str, repo: str) -> ServiceAudit:
        branch = self._get_default_branch(owner, repo)
        return self._scan_repo(owner, repo, branch)

    # ── Coleta por repo ───────────────────────────────────────────────────

    def _scan_repo(self, org: str, name: str, branch: str) -> ServiceAudit:
        audit = ServiceAudit(repo=f"{org}/{name}", name=name, default_branch=branch)

        # 1. Dockerfile (obrigatório)
        dockerfile = self._get_file(org, name, self.DOCKERFILE_PATH, branch)
        if not dockerfile:
            audit.skipped    = True
            audit.skip_reason = "Sem Dockerfile na raiz"
            audit.files_missing.append(self.DOCKERFILE_PATH)
            return audit
        audit.files_found.append(self.DOCKERFILE_PATH)
        self._parse_dockerfile(dockerfile, audit)

        # 2. deploy.yaml (obrigatório)
        deploy = self._get_file(org, name, self.DEPLOY_PATH, branch)
        if not deploy:
            audit.skipped     = True
            audit.skip_reason = f"Sem {self.DEPLOY_PATH}"
            audit.files_missing.append(self.DEPLOY_PATH)
            return audit
        audit.files_found.append(self.DEPLOY_PATH)
        self._parse_deploy(deploy, audit)

        # 3. Dependências
        pyproject = self._get_file(org, name, self.PYPROJECT_PATH, branch)
        if pyproject:
            audit.files_found.append(self.PYPROJECT_PATH)
            self._parse_pyproject(pyproject, audit)
        else:
            reqs_path = audit._reqs_path if hasattr(audit, "_reqs_path") else None
            reqs = None
            for candidate in ([reqs_path] if reqs_path else []) + [
                "requirements.txt", "requirements/base.txt",
                "requirements/production.txt", "requirements/prod.txt",
            ]:
                if not candidate:
                    continue
                reqs = self._get_file(org, name, candidate, branch)
                if reqs:
                    audit.files_found.append(candidate)
                    self._parse_requirements(reqs, audit)
                    break

        # 4. Fallback Python version
        if not audit.python_version:
            audit.python_version = "2.7"
            audit.framework = audit.framework or "wsgi"
            audit.issues.append(Issue(
                code="UNKNOWN_PYTHON",
                severity="info",
                message="Versão do Python não detectada no Dockerfile (imagem custom). Assumindo 2.7.",
            ))

        # 5. Validações de qualidade
        self._validate(audit)

        return audit

    # ── Parsers ───────────────────────────────────────────────────────────

    def _parse_dockerfile(self, content: str, audit: ServiceAudit) -> None:
        req_candidates = []
        for line in content.splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue

            # Python version
            m = re.match(
                r"^FROM\s+(?:[\w.\-/]+/)?python:([\d.]+)(?:[-\w.]*)?(?:\s+AS\s+\S+)?",
                s, re.IGNORECASE,
            )
            if m and not audit.python_version:
                raw   = m.group(1)
                parts = raw.split(".")
                audit.python_version = f"{parts[0]}.{parts[1]}" if len(parts) >= 2 else raw

            # Requirements path
            cm = re.match(r"^(?:COPY|ADD)\s+(?:--\S+\s+)*(.+)$", s, re.IGNORECASE)
            if cm:
                tokens  = cm.group(1).strip().split()
                sources = tokens[:-1] if len(tokens) > 1 else tokens
                for src in sources:
                    src = src.strip("'\"")
                    if re.search(r"req[\w\-]*\.txt|requirements[\w/.\-]*\.txt", src, re.IGNORECASE):
                        req_candidates.append(src)

        if req_candidates:
            ranked = sorted(req_candidates, key=lambda p: (
                0 if re.search(r"prod|production|base|common", p, re.IGNORECASE) else 1,
                len(p),
            ))
            audit._reqs_path = ranked[0]

    def _parse_deploy(self, content: str, audit: ServiceAudit) -> None:
        if not HAS_YAML:
            return
        try:
            docs = list(_yaml.safe_load_all(content))
        except Exception:
            return
        for doc in docs:
            if not isinstance(doc, dict):
                continue
            kind = doc.get("kind", "")
            if kind == "Deployment":
                self._extract_deployment(doc, audit)
            elif kind == "HorizontalPodAutoscaler":
                self._extract_hpa(doc, audit)

    def _extract_deployment(self, doc: dict, audit: ServiceAudit) -> None:
        meta = doc.get("metadata") or {}
        audit.k8s_namespace = meta.get("namespace")

        spec = doc.get("spec") or {}
        if spec.get("replicas") is not None:
            audit.replicas = int(spec["replicas"])

        pod_tmpl  = spec.get("template") or {}
        pod_meta  = pod_tmpl.get("metadata") or {}
        pod_labels= pod_meta.get("labels") or {}
        pod_ann   = pod_meta.get("annotations") or {}
        pod_spec  = pod_tmpl.get("spec") or {}
        containers= pod_spec.get("containers") or []

        if not containers:
            return
        c = containers[0]

        # Resources
        res  = c.get("resources") or {}
        reqs = res.get("requests") or {}
        lims = res.get("limits") or {}
        if reqs.get("cpu"):    audit.cpu_request = str(reqs["cpu"])
        if reqs.get("memory"): audit.mem_request = str(reqs["memory"])
        if lims.get("cpu"):    audit.cpu_limit   = str(lims["cpu"])
        if lims.get("memory"): audit.mem_limit   = str(lims["memory"])

        # Probes
        audit.has_liveness_probe  = bool(c.get("livenessProbe"))
        audit.has_readiness_probe = bool(c.get("readinessProbe"))
        audit.has_startup_probe   = bool(c.get("startupProbe"))

        # DD_SERVICE
        env_vars = {}
        for e in (c.get("env") or []):
            n = e.get("name", "")
            if e.get("value"):
                env_vars[n] = str(e["value"])
            elif (e.get("valueFrom") or {}).get("fieldRef"):
                fp = (e["valueFrom"]["fieldRef"] or {}).get("fieldPath", "")
                lm = re.search(r"labels\['([^']+)'\]", fp)
                if lm and lm.group(1) in pod_labels:
                    env_vars[n] = str(pod_labels[lm.group(1)])

        audit.dd_service = (
            env_vars.get("DD_SERVICE")
            or pod_labels.get("tags.datadoghq.com/service")
            or pod_ann.get("tags.datadoghq.com/service")
            or pod_labels.get("service")
            or pod_labels.get("app.kubernetes.io/name")
            or pod_labels.get("app")
            or audit.name
        )

    def _extract_hpa(self, doc: dict, audit: ServiceAudit) -> None:
        spec = doc.get("spec") or {}
        if spec.get("minReplicas") is not None:
            audit.hpa_min_replicas = int(spec["minReplicas"])
        if spec.get("maxReplicas") is not None:
            audit.hpa_max_replicas = int(spec["maxReplicas"])
        if spec.get("targetCPUUtilizationPercentage") is not None:
            audit.hpa_cpu_target = int(spec["targetCPUUtilizationPercentage"])
            return
        for m in (spec.get("metrics") or []):
            if m.get("type") != "Resource":
                continue
            res = m.get("resource") or {}
            avg = (res.get("target") or {}).get("averageUtilization")
            if avg is None:
                continue
            if res.get("name") == "cpu":    audit.hpa_cpu_target = int(avg)
            elif res.get("name") == "memory": audit.hpa_mem_target = int(avg)

    def _parse_pyproject(self, content: str, audit: ServiceAudit) -> None:
        audit.dependency_manager = "poetry"
        py = re.search(r'^python\s*=\s*["\']([^"\']+)["\']', content, re.MULTILINE) \
          or re.search(r'requires-python\s*=\s*["\']([^"\']+)["\']', content)
        if py:
            vm = re.search(r'(\d+\.\d+)', py.group(1))
            if vm:
                audit.python_version = vm.group(1)
        self._detect_libs(content.lower(), audit)

    def _parse_requirements(self, content: str, audit: ServiceAudit) -> None:
        audit.dependency_manager = "pip"
        lines = []
        for line in content.splitlines():
            s = line.strip().lower()
            if not s or s.startswith("#") or s.startswith("-"):
                continue
            lines.append(re.split(r'[\[>=<!;@]', s)[0].strip())
        self._detect_libs(" ".join(lines), audit)

    def _detect_libs(self, text: str, audit: ServiceAudit) -> None:
        for name, patterns in [
            ("fastapi",   ["fastapi"]),
            ("aiohttp",   ["aiohttp"]),
            ("django",    ["django"]),
            ("flask",     ["flask"]),
            ("tornado",   ["tornado"]),
            ("starlette", ["starlette"]),
        ]:
            if any(p in text for p in patterns):
                if not audit.framework:
                    audit.framework = name
                break
        audit.has_pandas = bool(re.search(r'\bpandas\b', text))
        audit.has_celery = bool(re.search(r'\bcelery\b', text))

    # ── Validações ────────────────────────────────────────────────────────

    def _validate(self, audit: ServiceAudit) -> None:
        """Gera issues de qualidade para uma ServiceAudit preenchida."""
        has_cpu_req = audit.cpu_request is not None
        has_cpu_lim = audit.cpu_limit is not None
        has_mem_req = audit.mem_request is not None
        has_mem_lim = audit.mem_limit is not None
        has_hpa     = audit.hpa_min_replicas is not None

        # Ausência total de resources
        if not any([has_cpu_req, has_cpu_lim, has_mem_req, has_mem_lim]):
            audit.issues.append(Issue(
                code="NO_RESOURCES",
                severity="critical",
                message="Nenhum resource (cpu/mem request ou limit) configurado.",
            ))
        else:
            # Ausências parciais
            if not has_cpu_req:
                audit.issues.append(Issue("NO_CPU_REQUEST", "warning", "CPU request não definido."))
            if not has_cpu_lim:
                audit.issues.append(Issue("NO_CPU_LIMIT", "warning", "CPU limit não definido — sem proteção de throttle."))
            if not has_mem_req:
                audit.issues.append(Issue("NO_MEM_REQUEST", "warning", "Memory request não definido."))
            if not has_mem_lim:
                audit.issues.append(Issue("NO_MEM_LIMIT", "critical", "Memory limit não definido — pod pode ser OOMKilled sem aviso."))

            # Burst factor de CPU suspeito
            if has_cpu_req and has_cpu_lim:
                req_m = _parse_cores(audit.cpu_request)
                lim_m = _parse_cores(audit.cpu_limit)
                if req_m > 0 and lim_m > 0:
                    burst = lim_m / req_m
                    if burst > 4:
                        audit.issues.append(Issue(
                            "HIGH_CPU_BURST",
                            "warning",
                            f"CPU limit é {burst:.1f}x o request (>{4}x é suspeito — "
                            "pode indicar request subconfigurado).",
                        ))

            # Mem limit = mem request (sem headroom)
            if has_mem_req and has_mem_lim:
                req_b = _parse_bytes(audit.mem_request)
                lim_b = _parse_bytes(audit.mem_limit)
                if req_b > 0 and lim_b > 0 and lim_b <= req_b:
                    audit.issues.append(Issue(
                        "MEM_NO_HEADROOM",
                        "critical",
                        "Memory limit ≤ request — qualquer pico de memória causa OOMKill.",
                    ))

        # HPA ausente
        if not has_hpa:
            audit.issues.append(Issue(
                "NO_HPA",
                "warning",
                "Sem HPA configurado — réplicas fixas, sem escalonamento automático.",
            ))
        else:
            # HPA inútil
            if (audit.hpa_min_replicas is not None
                    and audit.hpa_max_replicas is not None
                    and audit.hpa_min_replicas >= audit.hpa_max_replicas):
                audit.issues.append(Issue(
                    "HPA_USELESS",
                    "warning",
                    f"minReplicas ({audit.hpa_min_replicas}) ≥ maxReplicas ({audit.hpa_max_replicas}) — HPA nunca escala.",
                ))

            # Single point of failure
            if audit.hpa_min_replicas == 1:
                audit.issues.append(Issue(
                    "SPOF",
                    "critical",
                    "minReplicas = 1 — qualquer restart derruba o serviço (single point of failure).",
                ))

            # HPA sem CPU target
            if audit.hpa_cpu_target is None and audit.hpa_mem_target is None:
                audit.issues.append(Issue(
                    "HPA_NO_TARGET",
                    "warning",
                    "HPA configurado mas sem target de CPU ou memória — nunca vai escalar.",
                ))

            # CPU target muito alto
            if audit.hpa_cpu_target is not None and audit.hpa_cpu_target > 85:
                audit.issues.append(Issue(
                    "HPA_TARGET_HIGH",
                    "warning",
                    f"CPU target do HPA = {audit.hpa_cpu_target}% — muito alto, "
                    "HPA escalará tarde demais.",
                ))

        # Probes
        if not audit.has_readiness_probe:
            audit.issues.append(Issue(
                "NO_READINESS_PROBE",
                "warning",
                "Sem readiness probe — tráfego pode ser roteado para pods não-prontos.",
            ))
        if not audit.has_liveness_probe:
            audit.issues.append(Issue(
                "NO_LIVENESS_PROBE",
                "info",
                "Sem liveness probe — pods travados não serão reiniciados automaticamente.",
            ))

    # ── API helpers ───────────────────────────────────────────────────────

    def _iter_repos(self, org: str, skip_archived: bool, skip_forks: bool) -> Iterator[dict]:
        page = 1
        while True:
            url  = f"{self._base}/orgs/{org}/repos"
            resp = self._session.get(url, params={"per_page": 100, "page": page, "type": "all"}, timeout=20)
            if resp.status_code == 404:
                url  = f"{self._base}/users/{org}/repos"
                resp = self._session.get(url, params={"per_page": 100, "page": page, "type": "all"}, timeout=20)
            if resp.status_code == 403:
                raise RuntimeError(f"Rate limit GitHub. Remaining: {resp.headers.get('X-RateLimit-Remaining')}")
            resp.raise_for_status()
            repos = resp.json()
            if not repos:
                break
            for r in repos:
                if skip_archived and r.get("archived"): continue
                if skip_forks    and r.get("fork"):     continue
                yield r
            if len(repos) < 100:
                break
            page += 1

    def _get_file(self, owner: str, repo: str, path: str, ref: str = "HEAD") -> Optional[str]:
        url = f"{self._base}/repos/{owner}/{repo}/contents/{path}"
        try:
            resp = self._session.get(url, params={"ref": ref}, timeout=15)
            if resp.status_code in (404, 403):
                return None
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return None
            if data.get("encoding") == "base64":
                return base64.b64decode(data["content"]).decode("utf-8", errors="replace")
            return data.get("content", "")
        except Exception:
            return None

    def _get_default_branch(self, owner: str, repo: str) -> str:
        try:
            resp = self._session.get(f"{self._base}/repos/{owner}/{repo}", timeout=10)
            resp.raise_for_status()
            return resp.json().get("default_branch", "main")
        except Exception:
            return "main"


# ── Helpers numéricos ─────────────────────────────────────────────────────────

def _parse_cores(val: Optional[str]) -> float:
    if not val: return 0.0
    s = str(val).strip()
    if s.endswith("m"):
        try: return float(s[:-1]) / 1000
        except ValueError: return 0.0
    try: return float(s)
    except ValueError: return 0.0


def _parse_bytes(val: Optional[str]) -> int:
    if not val: return 0
    s = str(val).strip()
    for suffix, mult in [("Ki", 1024), ("Mi", 1024**2), ("Gi", 1024**3),
                          ("K", 1000), ("M", 1000**2), ("G", 1000**3)]:
        if s.endswith(suffix):
            try: return int(float(s[:-len(suffix)]) * mult)
            except ValueError: return 0
    try: return int(s)
    except ValueError: return 0