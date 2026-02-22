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

@dataclass
class GitHubAppInfo:
    repo: str                               # "owner/repo"
    default_branch: str = "main"

    # ── Tags Datadog (extraídas do deploy.yaml) ───────────────────────────
    # São a fonte de verdade para consultar métricas no Datadog
    dd_service: Optional[str] = None       # valor de DD_SERVICE ou label service:
    dd_env: str = "production"             # sempre production; pode vir de DD_ENV
    dd_version: Optional[str] = None       # informativo apenas

    # ── De Dockerfile ─────────────────────────────────────────────────────
    python_version: Optional[str] = None   # "3.11", "3.9", "2.7"
    base_image: Optional[str] = None       # "python:3.11-slim"
    startup_command: Optional[str] = None  # "gunicorn app:app -w 4 ..."
    gunicorn_workers: Optional[int] = None
    uvicorn_workers: Optional[int] = None
    exposed_port: Optional[int] = None

    # ── De pyproject.toml / requirements.txt ──────────────────────────────
    framework: Optional[str] = None        # "fastapi", "flask", "django", "aiohttp"
    framework_version: Optional[str] = None
    python_requires: Optional[str] = None  # ">=3.9"
    has_pandas: bool = False
    has_numpy: bool = False
    has_scipy: bool = False
    has_celery: bool = False
    has_sqlalchemy: bool = False
    dependency_manager: Optional[str] = None  # "pip" | "poetry"
    raw_dependencies: list = field(default_factory=list)

    # ── De deploy.yaml ─────────────────────────────────────────────────────
    deploy_yaml_found: bool = False
    k8s_namespace: Optional[str] = None
    k8s_deployment_name: Optional[str] = None  # nome do Deployment no k8s
    k8s_replicas: Optional[int] = None

    # Resources atuais — fonte de verdade mais confiável que Datadog histórico
    cpu_request: Optional[str] = None      # "250m"
    cpu_limit: Optional[str] = None        # "500m"
    mem_request: Optional[str] = None      # "256Mi"
    mem_limit: Optional[str] = None        # "512Mi"

    # HPA atual
    hpa_min_replicas: Optional[int] = None
    hpa_max_replicas: Optional[int] = None
    hpa_cpu_target: Optional[int] = None
    hpa_mem_target: Optional[int] = None
    hpa_api_version: Optional[str] = None

    # ── Diagnósticos ──────────────────────────────────────────────────────
    warnings: list = field(default_factory=list)
    files_found: list = field(default_factory=list)
    files_missing: list = field(default_factory=list)
    skipped: bool = False          # True se repo não tem deploy.yaml
    skip_reason: str = ""


# ──────────────────────────────────────────────────────────────────────────────
# Collector
# ──────────────────────────────────────────────────────────────────────────────

class GitHubCollector:

    BASE_URL = "https://api.github.com"
    DEPLOY_YAML_PATH = "manifests/kubernetes/main/deploy.yaml"
    DOCKERFILE_PATH = "Dockerfile"
    DEPS_CANDIDATES = [
        "pyproject.toml",
        "requirements.txt",
        "requirements/base.txt",
        "requirements/common.txt",
        "requirements/production.txt",
        "requirements/prod.txt",
    ]

    def __init__(self, token: Optional[str] = None, base_url: Optional[str] = None):
        if not HAS_REQUESTS:
            raise ImportError("requests não instalado. Rode: pip install requests")

        import os
        self._token = token or os.environ.get("GITHUB_TOKEN")
        self._base = (base_url or self.BASE_URL).rstrip("/")
        self._session = _requests.Session()
        self._session.headers.update({
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "k8s-resource-advisor/1.0",
        })
        if self._token:
            self._session.headers["Authorization"] = f"Bearer {self._token}"

    # ── Descoberta automática ─────────────────────────────────────────────

    def discover_apps(
        self,
        org: str,
        repo_filter: Optional[str] = None,
        skip_archived: bool = True,
        skip_forks: bool = True,
        max_repos: int = 500,
    ) -> list[GitHubAppInfo]:
        apps = []
        checked = 0

        for repo_meta in self._iter_repos(org, skip_archived, skip_forks):
            if checked >= max_repos:
                break
            checked += 1

            repo_name = repo_meta["name"]
            if repo_filter and repo_filter.lower() not in repo_name.lower():
                continue

            branch = repo_meta.get("default_branch", "main")

            # Verifica rápido se tem deploy.yaml antes de fazer mais requests
            deploy_content = self._get_file(org, repo_name, self.DEPLOY_YAML_PATH, branch)
            if not deploy_content:
                # Repo não tem deploy.yaml — não é uma app k8s que queremos analisar
                continue

            info = GitHubAppInfo(
                repo=f"{org}/{repo_name}",
                default_branch=branch,
                deploy_yaml_found=True,
                files_found=[self.DEPLOY_YAML_PATH],
            )

            # Extrai deploy.yaml (resources, HPA, tags DD)
            self._parse_deploy_yaml(deploy_content, info)

            # Se não tem DD_SERVICE configurado, não dá para consultar no Datadog
            if not info.dd_service:
                info.skipped = True
                info.skip_reason = (
                    "DD_SERVICE não encontrado no deploy.yaml. "
                    "Configure a variável de ambiente DD_SERVICE ou "
                    "a label 'tags.datadoghq.com/service' no Deployment."
                )
                apps.append(info)
                continue

            # Dockerfile
            dockerfile = self._get_file(org, repo_name, self.DOCKERFILE_PATH, branch)
            if dockerfile:
                info.files_found.append(self.DOCKERFILE_PATH)
                self._parse_dockerfile(dockerfile, info)
            else:
                info.files_missing.append(self.DOCKERFILE_PATH)

            # Dependências
            for candidate in self.DEPS_CANDIDATES:
                deps_content = self._get_file(org, repo_name, candidate, branch)
                if deps_content:
                    info.files_found.append(candidate)
                    if candidate == "pyproject.toml":
                        self._parse_pyproject(deps_content, info)
                    else:
                        self._parse_requirements(deps_content, info)
                    break
            else:
                info.files_missing.append("pyproject.toml / requirements.txt")

            apps.append(info)

            # Pequena pausa para não bater rate limit em orgs grandes
            time.sleep(0.05)

        return apps

    def collect(self, owner: str, repo: str, ref: Optional[str] = None) -> GitHubAppInfo:
        branch = ref or self._get_default_branch(owner, repo)
        info = GitHubAppInfo(repo=f"{owner}/{repo}", default_branch=branch)

        deploy_content = self._get_file(owner, repo, self.DEPLOY_YAML_PATH, branch)
        if deploy_content:
            info.files_found.append(self.DEPLOY_YAML_PATH)
            info.deploy_yaml_found = True
            self._parse_deploy_yaml(deploy_content, info)
        else:
            info.files_missing.append(self.DEPLOY_YAML_PATH)
            info.warnings.append(f"deploy.yaml não encontrado em {self.DEPLOY_YAML_PATH}")

        dockerfile = self._get_file(owner, repo, self.DOCKERFILE_PATH, branch)
        if dockerfile:
            info.files_found.append(self.DOCKERFILE_PATH)
            self._parse_dockerfile(dockerfile, info)
        else:
            info.files_missing.append(self.DOCKERFILE_PATH)
            info.warnings.append("Dockerfile não encontrado na raiz")

        for candidate in self.DEPS_CANDIDATES:
            deps_content = self._get_file(owner, repo, candidate, branch)
            if deps_content:
                info.files_found.append(candidate)
                if candidate == "pyproject.toml":
                    self._parse_pyproject(deps_content, info)
                else:
                    self._parse_requirements(deps_content, info)
                break

        if info.python_version and not info.python_requires:
            info.python_requires = f">={info.python_version}"

        return info

    def _iter_repos(
        self, org: str, skip_archived: bool, skip_forks: bool
    ) -> Iterator[dict]:
        page = 1
        while True:
            # Tenta como organização primeiro, depois como usuário
            url = f"{self._base}/orgs/{org}/repos"
            resp = self._session.get(
                url,
                params={"per_page": 100, "page": page, "type": "all"},
                timeout=20,
            )
            if resp.status_code == 404:
                # Tenta como usuário
                url = f"{self._base}/users/{org}/repos"
                resp = self._session.get(
                    url,
                    params={"per_page": 100, "page": page, "type": "all"},
                    timeout=20,
                )

            if resp.status_code == 403:
                remaining = resp.headers.get("X-RateLimit-Remaining", "?")
                reset = resp.headers.get("X-RateLimit-Reset", "?")
                raise RuntimeError(
                    f"Rate limit do GitHub atingido. "
                    f"Remaining: {remaining}. Reset em timestamp: {reset}. "
                    "Configure GITHUB_TOKEN para aumentar o limite para 5000/h."
                )

            resp.raise_for_status()
            repos = resp.json()
            if not repos:
                break

            for r in repos:
                if skip_archived and r.get("archived"):
                    continue
                if skip_forks and r.get("fork"):
                    continue
                yield r

            if len(repos) < 100:
                break
            page += 1

    def _get_file(
        self, owner: str, repo: str, path: str, ref: str = "HEAD"
    ) -> Optional[str]:
        url = f"{self._base}/repos/{owner}/{repo}/contents/{path}"
        try:
            resp = self._session.get(url, params={"ref": ref}, timeout=15)
            if resp.status_code in (404, 403):
                return None
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return None  # é um diretório
            if data.get("encoding") == "base64":
                return base64.b64decode(data["content"]).decode("utf-8", errors="replace")
            return data.get("content", "")
        except Exception:
            return None

    def _get_default_branch(self, owner: str, repo: str) -> str:
        try:
            resp = self._session.get(
                f"{self._base}/repos/{owner}/{repo}", timeout=10
            )
            resp.raise_for_status()
            return resp.json().get("default_branch", "main")
        except Exception:
            return "main"


    def _parse_deploy_yaml(self, content: str, info: GitHubAppInfo) -> None:

        if not HAS_YAML:
            self._parse_deploy_yaml_regex(content, info)
            return

        try:
            docs = list(_yaml.safe_load_all(content))
        except Exception as exc:
            info.warnings.append(f"Erro ao parsear deploy.yaml: {exc}")
            self._parse_deploy_yaml_regex(content, info)
            return

        for doc in docs:
            if not isinstance(doc, dict):
                continue
            kind = doc.get("kind", "")
            if kind == "Deployment":
                self._extract_deployment(doc, info)
            elif kind == "HorizontalPodAutoscaler":
                info.hpa_api_version = doc.get("apiVersion", "autoscaling/v2")
                self._extract_hpa(doc, info)

    def _extract_deployment(self, doc: dict, info: GitHubAppInfo) -> None:
        meta = doc.get("metadata", {})
        info.k8s_namespace = meta.get("namespace") or info.k8s_namespace
        info.k8s_deployment_name = meta.get("name")

        spec = doc.get("spec", {})
        if spec.get("replicas") is not None:
            info.k8s_replicas = int(spec["replicas"])

        pod_template = spec.get("template", {})
        pod_meta = pod_template.get("metadata", {})
        pod_labels = pod_meta.get("labels", {})
        pod_annotations = pod_meta.get("annotations", {})

        containers = pod_template.get("spec", {}).get("containers", [])
        if not containers:
            return

        container = containers[0]

        # ── Resources ────────────────────────────────────────────────────
        resources = container.get("resources", {})
        requests = resources.get("requests", {})
        limits = resources.get("limits", {})
        if requests.get("cpu"):     info.cpu_request = str(requests["cpu"])
        if requests.get("memory"):  info.mem_request = str(requests["memory"])
        if limits.get("cpu"):       info.cpu_limit = str(limits["cpu"])
        if limits.get("memory"):    info.mem_limit = str(limits["memory"])

        # ── Tags do Datadog ───────────────────────────────────────────────
        # Prioridade 1: variáveis de ambiente DD_SERVICE / DD_ENV
        env_vars = {}
        for env_entry in container.get("env", []):
            name = env_entry.get("name", "")
            # Valor direto
            if env_entry.get("value"):
                env_vars[name] = str(env_entry["value"])
            # ValueFrom → fieldRef (ex: DD_SERVICE vindo de uma label do pod)
            elif env_entry.get("valueFrom", {}).get("fieldRef"):
                field_path = env_entry["valueFrom"]["fieldRef"].get("fieldPath", "")
                # "metadata.labels['tags.datadoghq.com/service']"
                label_match = re.search(r"labels\['([^']+)'\]", field_path)
                if label_match:
                    label_key = label_match.group(1)
                    if label_key in pod_labels:
                        env_vars[name] = str(pod_labels[label_key])

        if "DD_SERVICE" in env_vars:
            info.dd_service = env_vars["DD_SERVICE"]
        if "DD_ENV" in env_vars:
            info.dd_env = env_vars["DD_ENV"]
        if "DD_VERSION" in env_vars:
            info.dd_version = env_vars["DD_VERSION"]

        # Prioridade 2: labels Datadog Unified Service Tagging no pod template
        # https://docs.datadoghq.com/getting_started/tagging/unified_service_tagging/
        if not info.dd_service:
            ust_service = (
                pod_labels.get("tags.datadoghq.com/service")
                or pod_annotations.get("tags.datadoghq.com/service")
            )
            if ust_service:
                info.dd_service = ust_service

        if not info.dd_service:
            ust_env = (
                pod_labels.get("tags.datadoghq.com/env")
                or pod_annotations.get("tags.datadoghq.com/env")
            )
            if ust_env:
                info.dd_env = ust_env

        # Prioridade 3: label genérica "service:" ou "app:" no pod template
        if not info.dd_service:
            info.dd_service = (
                pod_labels.get("service")
                or pod_labels.get("app.kubernetes.io/name")
                or pod_labels.get("app")
            )

        # Prioridade 4: nome do próprio Deployment (último recurso)
        if not info.dd_service and info.k8s_deployment_name:
            info.dd_service = info.k8s_deployment_name
            info.warnings.append(
                f"DD_SERVICE não encontrado — usando nome do Deployment '{info.dd_service}' "
                "como service tag. Valide se é o valor correto no Datadog."
            )

    def _extract_hpa(self, doc: dict, info: GitHubAppInfo) -> None:
        """Extrai minReplicas, maxReplicas e targets do HPA (v1 e v2)."""
        spec = doc.get("spec", {})
        if spec.get("minReplicas") is not None:
            info.hpa_min_replicas = int(spec["minReplicas"])
        if spec.get("maxReplicas") is not None:
            info.hpa_max_replicas = int(spec["maxReplicas"])

        # HPA v1
        if spec.get("targetCPUUtilizationPercentage") is not None:
            info.hpa_cpu_target = int(spec["targetCPUUtilizationPercentage"])
            return

        # HPA v2 — metrics array
        for metric in spec.get("metrics", []):
            if metric.get("type") != "Resource":
                continue
            resource = metric.get("resource", {})
            target = resource.get("target", {})
            avg_util = target.get("averageUtilization")
            if avg_util is None:
                continue
            if resource.get("name") == "cpu":
                info.hpa_cpu_target = int(avg_util)
            elif resource.get("name") == "memory":
                info.hpa_mem_target = int(avg_util)

    def _parse_dockerfile(self, content: str, info: GitHubAppInfo) -> None:
        for line in content.splitlines():
            stripped = line.strip()

            # FROM python:3.11-slim
            m = re.match(
                r"^FROM\s+(?:[\w./-]+/)?python:([\d.]+)(?:[-\w]*)?(?:\s+AS\s+\w+)?",
                stripped, re.IGNORECASE,
            )
            if m and not info.python_version:
                parts = m.group(1).split(".")
                info.python_version = f"{parts[0]}.{parts[1]}" if len(parts) >= 2 else m.group(1)
                info.base_image = f"python:{m.group(1)}"

            # CMD / ENTRYPOINT
            if re.match(r'^(?:CMD|ENTRYPOINT)\s+', stripped, re.IGNORECASE):
                cmd = stripped.split(None, 1)[1] if " " in stripped else ""
                cmd = re.sub(r'[\[\]"]', '', cmd).strip()
                if "gunicorn" in cmd.lower():
                    info.startup_command = cmd
                    wm = re.search(r'-w\s+(\d+)|--workers[=\s]+(\d+)', cmd)
                    if wm:
                        info.gunicorn_workers = int(wm.group(1) or wm.group(2))
                elif "uvicorn" in cmd.lower():
                    info.startup_command = cmd
                    wm = re.search(r'--workers[=\s]+(\d+)', cmd)
                    if wm:
                        info.uvicorn_workers = int(wm.group(1))

            # EXPOSE
            em = re.match(r'^EXPOSE\s+(\d+)', stripped, re.IGNORECASE)
            if em:
                info.exposed_port = int(em.group(1))

    def _parse_pyproject(self, content: str, info: GitHubAppInfo) -> None:
        info.dependency_manager = "poetry"
        py_match = re.search(
            r'python\s*=\s*["\']([^"\']+)["\']', content
        ) or re.search(r'requires-python\s*=\s*["\']([^"\']+)["\']', content)
        if py_match:
            info.python_requires = py_match.group(1)
            vm = re.search(r'(\d+\.\d+)', py_match.group(1))
            if vm and not info.python_version:
                info.python_version = vm.group(1)

        dep_lines = [
            l.strip().lower() for l in content.splitlines()
            if l.strip() and not l.strip().startswith("#") and not l.strip().startswith("[")
        ]
        all_deps = " ".join(dep_lines)
        info.raw_dependencies = [l for l in dep_lines if "=" in l or '"' in l]
        self._detect_libs(all_deps, info)

    def _parse_requirements(self, content: str, info: GitHubAppInfo) -> None:
        info.dependency_manager = "pip"
        lines = []
        for line in content.splitlines():
            s = line.strip().lower()
            if not s or s.startswith("#") or s.startswith("-"):
                continue
            lines.append(re.split(r'[\[>=<!;]', s)[0].strip())
        info.raw_dependencies = lines
        self._detect_libs(" ".join(lines), info)

    def _detect_libs(self, all_deps: str, info: GitHubAppInfo) -> None:
        for name, patterns in [
            ("fastapi",   ["fastapi"]),
            ("aiohttp",   ["aiohttp"]),
            ("django",    ["django"]),
            ("flask",     ["flask"]),
            ("tornado",   ["tornado"]),
            ("starlette", ["starlette"]),
        ]:
            if any(p in all_deps for p in patterns):
                info.framework = name
                vm = re.search(rf'{name}[^"\n]*[=><~^]+\s*["\']?(\d+[\d.]*)', all_deps)
                if vm:
                    info.framework_version = vm.group(1)
                break

        info.has_pandas    = bool(re.search(r'\bpandas\b', all_deps))
        info.has_numpy     = bool(re.search(r'\bnumpy\b', all_deps))
        info.has_scipy     = bool(re.search(r'\bscipy\b', all_deps))
        info.has_celery    = bool(re.search(r'\bcelery\b', all_deps))
        info.has_sqlalchemy = bool(re.search(r'\bsqlalchemy\b', all_deps))

    def _parse_deploy_yaml_regex(self, content: str, info: GitHubAppInfo) -> None:
        """Fallback sem PyYAML."""
        simple = {
            "cpu_request":  r'cpu:\s*["\']?(\d+m|\d+(?:\.\d+)?)["\']?',
            "mem_request":  r'memory:\s*["\']?(\d+(?:Mi|Gi|Ki|M|G))["\']?',
            "min_replicas": r'minReplicas:\s*(\d+)',
            "max_replicas": r'maxReplicas:\s*(\d+)',
            "cpu_target":   r'targetCPUUtilizationPercentage:\s*(\d+)',
            "dd_service_env": r'DD_SERVICE["\s:]+value["\s:]+["\']?([^\s"\']+)',
            "dd_service_label": r'tags\.datadoghq\.com/service["\s:]+["\']?([^\s"\']+)',
        }
        for key, pat in simple.items():
            m = re.search(pat, content, re.IGNORECASE)
            if m:
                v = m.group(1)
                if key == "cpu_request" and not info.cpu_request:
                    info.cpu_request = v
                elif key == "mem_request" and not info.mem_request:
                    info.mem_request = v
                elif key == "min_replicas":
                    info.hpa_min_replicas = int(v)
                elif key == "max_replicas":
                    info.hpa_max_replicas = int(v)
                elif key == "cpu_target":
                    info.hpa_cpu_target = int(v)
                elif key in ("dd_service_env", "dd_service_label") and not info.dd_service:
                    info.dd_service = v


# ──────────────────────────────────────────────────────────────────────────────
# Helpers de conversão de strings k8s → tipos Python
# ──────────────────────────────────────────────────────────────────────────────

def _k8s_cpu_to_cores(val: Optional[str]) -> float:
    if not val: return 0.0
    val = str(val).strip()
    if val.endswith("m"): return float(val[:-1]) / 1000
    try: return float(val)
    except ValueError: return 0.0


def _k8s_mem_to_bytes(val: Optional[str]) -> int:
    if not val: return 0
    val = str(val).strip()
    for suffix, mult in [("Ki",1024),("Mi",1024**2),("Gi",1024**3),("Ti",1024**4),
                          ("K",1000),("M",1000**2),("G",1000**3)]:
        if val.endswith(suffix):
            try: return int(float(val[:-len(suffix)]) * mult)
            except ValueError: return 0
    try: return int(val)
    except ValueError: return 0


def github_info_to_app_config(info: GitHubAppInfo) -> dict:
    """
    Converte GitHubAppInfo em um app_config completo para o pipeline.
    Elimina a necessidade do bloco apps: no settings.yaml.

    O nome da app é derivado do dd_service (que é o nome canônico no Datadog).
    """
    name = info.dd_service or info.repo.split("/")[-1]

    config = {
        "name": name,
        "namespace": info.k8s_namespace or "production",
        "dd_service": info.dd_service or name,
        "dd_env": info.dd_env,
        "framework": info.framework or "wsgi",
        "python_version": info.python_version or "3.x",
        "has_pandas": info.has_pandas or info.has_numpy or info.has_scipy,
        "sla_latency_p95_ms": 500.0,    # default conservador
        "peak_traffic_multiplier": 3.0,  # default conservador

        # Internos — usados pelo pipeline para sobrescrever valores do Datadog
        "_current_cpu_request":        info.cpu_request,
        "_current_cpu_limit":          info.cpu_limit,
        "_current_mem_request":        info.mem_request,
        "_current_mem_limit":          info.mem_limit,
        "_current_cpu_request_cores":  _k8s_cpu_to_cores(info.cpu_request),
        "_current_cpu_limit_cores":    _k8s_cpu_to_cores(info.cpu_limit),
        "_current_mem_request_bytes":  _k8s_mem_to_bytes(info.mem_request),
        "_current_mem_limit_bytes":    _k8s_mem_to_bytes(info.mem_limit),
        "_current_hpa_min":            info.hpa_min_replicas,
        "_current_hpa_max":            info.hpa_max_replicas,
        "_current_hpa_cpu_target":     info.hpa_cpu_target,
        "_gunicorn_workers":           info.gunicorn_workers,
        "_github_info":                info,
    }
    return config


# mantém compatibilidade com código anterior que chama enrich_app_config
def enrich_app_config(app_config: dict, info: GitHubAppInfo) -> dict:
    """
    Merge de GitHubAppInfo em um app_config existente.
    GitHub sobrescreve apenas campos não definidos, EXCETO resources e HPA
    do deploy.yaml que sempre são a fonte de verdade.
    """
    enriched = dict(app_config)

    if info.python_version and not app_config.get("python_version"):
        enriched["python_version"] = info.python_version
    if info.framework and not app_config.get("framework"):
        enriched["framework"] = info.framework
    if info.has_pandas or info.has_numpy or info.has_scipy:
        enriched["has_pandas"] = True
    if info.k8s_namespace and not app_config.get("namespace"):
        enriched["namespace"] = info.k8s_namespace
    if info.dd_service and not app_config.get("dd_service"):
        enriched["dd_service"] = info.dd_service
    if info.gunicorn_workers:
        enriched["_gunicorn_workers"] = info.gunicorn_workers

    # Resources e HPA: sempre sobrescreve (fonte de verdade = deploy.yaml)
    if info.cpu_request:
        enriched["_current_cpu_request"]       = info.cpu_request
        enriched["_current_cpu_request_cores"] = _k8s_cpu_to_cores(info.cpu_request)
    if info.cpu_limit:
        enriched["_current_cpu_limit"]         = info.cpu_limit
        enriched["_current_cpu_limit_cores"]   = _k8s_cpu_to_cores(info.cpu_limit)
    if info.mem_request:
        enriched["_current_mem_request"]       = info.mem_request
        enriched["_current_mem_request_bytes"] = _k8s_mem_to_bytes(info.mem_request)
    if info.mem_limit:
        enriched["_current_mem_limit"]         = info.mem_limit
        enriched["_current_mem_limit_bytes"]   = _k8s_mem_to_bytes(info.mem_limit)
    if info.hpa_min_replicas is not None:
        enriched["_current_hpa_min"]           = info.hpa_min_replicas
    if info.hpa_max_replicas is not None:
        enriched["_current_hpa_max"]           = info.hpa_max_replicas
    if info.hpa_cpu_target is not None:
        enriched["_current_hpa_cpu_target"]    = info.hpa_cpu_target

    enriched["_github_info"] = info
    return enriched
