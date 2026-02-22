"""
collectors/github.py

Extrai informações de cada app diretamente do repositório GitHub — sem configuração manual.

Regras de descoberta:
  - Repositórios válidos começam com "service-"
  - Devem ter Dockerfile na raiz (obrigatório)
  - Devem ter manifests/kubernetes/main/deploy.yaml

Estratégia de dependências (por ordem de prioridade):
  1. pyproject.toml existe → assume poetry; extrai Python version, framework,
     bibliotecas pesadas direto de lá. Dockerfile ainda é lido para startup command.
  2. pyproject.toml não existe → lê o Dockerfile para encontrar:
       a. Versão do Python (FROM python:X.Y-...)
       b. Path do arquivo de requirements (COPY requirements*.txt ou similar —
          desenvolvedores nem sempre usam o nome padrão)
     Depois carrega esse arquivo e extrai as dependências.
  3. Nenhuma versão de Python detectável no Dockerfile (imagem custom/interna) →
     assume Python 2.7 + framework wsgi, gera warning.

O DD_SERVICE é extraído do deploy.yaml (variável DD_SERVICE, label UST, ou nome do Deployment).
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


# ──────────────────────────────────────────────────────────────────────────────
# Resultado da coleta por repositório
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class GitHubAppInfo:
    """
    Tudo que o GitHub collector extrai de um repositório.
    Campos não encontrados ficam None — o pipeline aplica defaults seguros.
    """
    repo: str                               # "owner/repo"
    default_branch: str = "main"

    # ── Tags Datadog (extraídas do deploy.yaml) ───────────────────────────
    dd_service: Optional[str] = None
    dd_env: str = "production"
    dd_version: Optional[str] = None

    # ── De Dockerfile ─────────────────────────────────────────────────────
    python_version: Optional[str] = None   # "3.11", "3.9", "2.7"
    base_image: Optional[str] = None       # "python:3.11-slim"
    startup_command: Optional[str] = None
    gunicorn_workers: Optional[int] = None
    uvicorn_workers: Optional[int] = None
    exposed_port: Optional[int] = None
    requirements_path: Optional[str] = None  # path detectado no Dockerfile

    # ── De pyproject.toml / requirements ──────────────────────────────────
    framework: Optional[str] = None
    framework_version: Optional[str] = None
    python_requires: Optional[str] = None
    has_pandas: bool = False
    has_numpy: bool = False
    has_scipy: bool = False
    has_celery: bool = False
    has_sqlalchemy: bool = False
    dependency_manager: Optional[str] = None  # "poetry" | "pip"
    raw_dependencies: list = field(default_factory=list)

    # ── De deploy.yaml ─────────────────────────────────────────────────────
    deploy_yaml_found: bool = False
    k8s_namespace: Optional[str] = None
    k8s_deployment_name: Optional[str] = None
    k8s_replicas: Optional[int] = None
    cpu_request: Optional[str] = None
    cpu_limit: Optional[str] = None
    mem_request: Optional[str] = None
    mem_limit: Optional[str] = None
    hpa_min_replicas: Optional[int] = None
    hpa_max_replicas: Optional[int] = None
    hpa_cpu_target: Optional[int] = None
    hpa_mem_target: Optional[int] = None
    hpa_api_version: Optional[str] = None

    # ── Diagnósticos ──────────────────────────────────────────────────────
    warnings: list = field(default_factory=list)
    files_found: list = field(default_factory=list)
    files_missing: list = field(default_factory=list)
    skipped: bool = False
    skip_reason: str = ""


# ──────────────────────────────────────────────────────────────────────────────
# Collector
# ──────────────────────────────────────────────────────────────────────────────

class GitHubCollector:
    """
    Coleta informações de contexto de repositórios GitHub via API REST v3.
    Suporta descoberta automática de todos os repos service-* de uma organização.
    """

    BASE_URL = "https://api.github.com"
    DEPLOY_YAML_PATH = "manifests/kubernetes/main/deploy.yaml"
    DOCKERFILE_PATH = "Dockerfile"
    PYPROJECT_PATH = "pyproject.toml"

    # Prefixo obrigatório para um repo ser considerado uma aplicação
    REPO_PREFIX = "service-"

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
        """
        Descobre automaticamente todas as apps da organização.

        Critérios de inclusão (todos devem ser verdadeiros):
          - Nome do repositório começa com "service-"
          - Possui Dockerfile na raiz
          - Possui manifests/kubernetes/main/deploy.yaml

        Args:
            org:           organização ou usuário GitHub
            repo_filter:   substring adicional para filtrar nome (além do prefixo)
            skip_archived: ignora repos arquivados (default: True)
            skip_forks:    ignora forks (default: True)
            max_repos:     limite de segurança (default: 500)
        """
        apps = []
        checked = 0

        for repo_meta in self._iter_repos(org, skip_archived, skip_forks):
            if checked >= max_repos:
                break

            repo_name = repo_meta["name"]

            # Filtro 1: deve começar com "service-"
            if not repo_name.startswith(self.REPO_PREFIX):
                continue

            checked += 1

            # Filtro adicional opcional (ex: --filter api)
            if repo_filter and repo_filter.lower() not in repo_name.lower():
                continue

            branch = repo_meta.get("default_branch", "main")
            info = self._collect_repo(org, repo_name, branch)
            apps.append(info)

            time.sleep(0.05)  # pausa suave para não bater rate limit em orgs grandes

        return apps

    def collect(self, owner: str, repo: str, ref: Optional[str] = None) -> GitHubAppInfo:
        """Coleta informações de um repositório específico pelo nome completo."""
        branch = ref or self._get_default_branch(owner, repo)
        return self._collect_repo(owner, repo, branch)

    # ── Lógica principal de coleta por repo ──────────────────────────────

    def _collect_repo(self, org: str, repo_name: str, branch: str) -> GitHubAppInfo:
        """
        Executa a coleta completa de um repositório.
        Aplica a estratégia de dependências em cascata:
          pyproject.toml → requirements pelo Dockerfile → fallback Python 2.7
        """
        info = GitHubAppInfo(repo=f"{org}/{repo_name}", default_branch=branch)

        # ── 1. Dockerfile (obrigatório) ───────────────────────────────────
        dockerfile = self._get_file(org, repo_name, self.DOCKERFILE_PATH, branch)
        if not dockerfile:
            info.skipped = True
            info.skip_reason = "Dockerfile não encontrado na raiz do repositório"
            info.files_missing.append(self.DOCKERFILE_PATH)
            return info

        info.files_found.append(self.DOCKERFILE_PATH)
        self._parse_dockerfile(dockerfile, info)

        # ── 2. deploy.yaml ────────────────────────────────────────────────
        deploy_content = self._get_file(org, repo_name, self.DEPLOY_YAML_PATH, branch)
        if not deploy_content:
            info.skipped = True
            info.skip_reason = f"deploy.yaml não encontrado em {self.DEPLOY_YAML_PATH}"
            info.files_missing.append(self.DEPLOY_YAML_PATH)
            return info

        info.files_found.append(self.DEPLOY_YAML_PATH)
        info.deploy_yaml_found = True
        self._parse_deploy_yaml(deploy_content, info)

        if not info.dd_service:
            info.skipped = True
            info.skip_reason = (
                "DD_SERVICE não encontrado no deploy.yaml. "
                "Configure DD_SERVICE como env var ou label 'tags.datadoghq.com/service'."
            )
            return info

        # ── 3. Dependências — estratégia em cascata ───────────────────────
        pyproject = self._get_file(org, repo_name, self.PYPROJECT_PATH, branch)

        if pyproject:
            # Caso A: pyproject.toml existe → poetry
            # Python version, framework e libs vêm daqui.
            # O Dockerfile já foi lido para startup_command; python_version do pyproject
            # tem prioridade sobre o Dockerfile se o Dockerfile usar imagem custom.
            info.files_found.append(self.PYPROJECT_PATH)
            self._parse_pyproject(pyproject, info)

        else:
            # Caso B: sem pyproject.toml → pip com requirements
            # O path do arquivo já foi extraído do Dockerfile em _parse_dockerfile.
            # Se o Dockerfile não apontou um path explícito, tenta nomes comuns.
            reqs_path = info.requirements_path
            reqs_content = None

            if reqs_path:
                reqs_content = self._get_file(org, repo_name, reqs_path, branch)
                if reqs_content:
                    info.files_found.append(reqs_path)
                else:
                    info.files_missing.append(reqs_path)
                    info.warnings.append(
                        f"requirements apontado no Dockerfile ({reqs_path}) não encontrado"
                    )

            if not reqs_content:
                # Fallback: tenta nomes convencionais
                for candidate in ["requirements.txt", "requirements/base.txt",
                                  "requirements/common.txt", "requirements/production.txt",
                                  "requirements/prod.txt"]:
                    reqs_content = self._get_file(org, repo_name, candidate, branch)
                    if reqs_content:
                        info.files_found.append(candidate)
                        info.requirements_path = candidate
                        break

            if reqs_content:
                self._parse_requirements(reqs_content, info)
            else:
                info.files_missing.append("requirements (nenhum encontrado)")
                info.warnings.append(
                    "Arquivo de dependências não encontrado. "
                    "Framework e bibliotecas não serão detectados automaticamente."
                )

        # ── 4. Fallback de Python version ─────────────────────────────────
        # Se após todos os parsers ainda não temos versão, o Dockerfile usa
        # uma imagem interna/custom sem tag python:X.Y — assume 2.7 + wsgi.
        if not info.python_version:
            info.python_version = "2.7"
            info.framework = info.framework or "wsgi"
            info.dependency_manager = info.dependency_manager or "pip"
            info.warnings.append(
                "Versão do Python não detectável no Dockerfile (imagem custom/interna). "
                "Assumindo Python 2.7 e framework wsgi. "
                "Verifique e corrija manualmente se necessário."
            )

        if info.python_version and not info.python_requires:
            info.python_requires = f">={info.python_version}"

        return info

    # ── API helpers ───────────────────────────────────────────────────────

    def _iter_repos(
        self, org: str, skip_archived: bool, skip_forks: bool
    ) -> Iterator[dict]:
        """Itera sobre todos os repos da org com paginação automática."""
        page = 1
        while True:
            url = f"{self._base}/orgs/{org}/repos"
            resp = self._session.get(
                url,
                params={"per_page": 100, "page": page, "type": "all"},
                timeout=20,
            )
            if resp.status_code == 404:
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
        """Retorna conteúdo decodificado de um arquivo, ou None se não existir."""
        url = f"{self._base}/repos/{owner}/{repo}/contents/{path}"
        try:
            resp = self._session.get(url, params={"ref": ref}, timeout=15)
            if resp.status_code in (404, 403):
                return None
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return None  # é diretório
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

    # ── Parsers ───────────────────────────────────────────────────────────

    def _parse_dockerfile(self, content: str, info: GitHubAppInfo) -> None:
        """
        Extrai do Dockerfile:
          - Versão do Python a partir de FROM python:X.Y[-variant]
          - Path de requirements a partir de instruções COPY/ADD
          - Startup command (CMD/ENTRYPOINT) com workers
          - Porta exposta (EXPOSE)

        Padrões de COPY que detectamos para requirements:
          COPY requirements.txt .
          COPY requirements/prod.txt requirements/prod.txt
          ADD reqs.txt /app/reqs.txt
          COPY --chown=app:app deps/requirements-prod.txt ./
        """
        lines = content.splitlines()
        req_candidates = []

        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue

            # FROM python:3.11-slim  |  FROM python:3.9  |  FROM python:3.8-alpine
            m = re.match(
                r"^FROM\s+(?:[\w.\-/]+/)?python:([\d.]+)(?:[-\w.]*)?(?:\s+AS\s+\S+)?",
                stripped, re.IGNORECASE,
            )
            if m and not info.python_version:
                raw = m.group(1)
                parts = raw.split(".")
                info.python_version = f"{parts[0]}.{parts[1]}" if len(parts) >= 2 else raw
                info.base_image = f"python:{raw}"

            # COPY / ADD — procura por arquivos que parecem requirements
            # Captura o primeiro argumento (source) antes de qualquer espaço ou ponto final
            copy_m = re.match(
                r"^(?:COPY|ADD)\s+(?:--\S+\s+)*(.+)$", stripped, re.IGNORECASE
            )
            if copy_m:
                args_raw = copy_m.group(1).strip()
                # Remove destino (último token) — ficamos com os sources
                # Suporta paths com e sem espaços entre source e dest
                tokens = args_raw.split()
                sources = tokens[:-1] if len(tokens) > 1 else tokens
                for src in sources:
                    src_clean = src.strip("'\"")
                    # Considera candidato se parece um arquivo de requirements
                    if re.search(r'req[\w\-]*\.txt|requirements[\w/.\-]*\.txt', src_clean, re.IGNORECASE):
                        req_candidates.append(src_clean)

            # CMD / ENTRYPOINT
            if re.match(r'^(?:CMD|ENTRYPOINT)\s+', stripped, re.IGNORECASE):
                cmd = stripped.split(None, 1)[1] if " " in stripped else ""
                cmd = re.sub(r'[\[\]",]', ' ', cmd).strip()
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

        # Escolhe o melhor candidato de requirements encontrado no Dockerfile.
        # Preferência por paths com "prod", "production" ou "base" no nome.
        if req_candidates:
            ranked = sorted(
                req_candidates,
                key=lambda p: (
                    0 if re.search(r'prod|production|base|common', p, re.IGNORECASE) else 1,
                    len(p),
                )
            )
            info.requirements_path = ranked[0]

    def _parse_pyproject(self, content: str, info: GitHubAppInfo) -> None:
        """
        Lê pyproject.toml (Poetry ou PEP 517/621).
        O python_version do pyproject tem prioridade sobre o Dockerfile
        porque é a versão de fato usada pelo projeto, não a da imagem base.
        """
        info.dependency_manager = "poetry"

        # Python version constraint: python = "^3.11"  ou  requires-python = ">=3.9"
        py_match = (
            re.search(r'^python\s*=\s*["\']([^"\']+)["\']', content, re.MULTILINE)
            or re.search(r'requires-python\s*=\s*["\']([^"\']+)["\']', content)
        )
        if py_match:
            info.python_requires = py_match.group(1)
            vm = re.search(r'(\d+\.\d+)', py_match.group(1))
            if vm:
                # Sobrescreve o que veio do Dockerfile — pyproject é mais preciso
                info.python_version = vm.group(1)

        dep_lines = [
            l.strip().lower() for l in content.splitlines()
            if l.strip()
            and not l.strip().startswith("#")
            and not l.strip().startswith("[")
        ]
        all_deps = " ".join(dep_lines)
        info.raw_dependencies = [l for l in dep_lines if "=" in l or '"' in l]
        self._detect_libs(all_deps, info)

    def _parse_requirements(self, content: str, info: GitHubAppInfo) -> None:
        """Lê requirements.txt em qualquer variante de nome."""
        info.dependency_manager = "pip"
        lines = []
        for line in content.splitlines():
            s = line.strip().lower()
            if not s or s.startswith("#") or s.startswith("-"):
                continue
            lines.append(re.split(r'[\[>=<!;@]', s)[0].strip())
        info.raw_dependencies = lines
        self._detect_libs(" ".join(lines), info)

    def _detect_libs(self, all_deps: str, info: GitHubAppInfo) -> None:
        """Detecta framework e bibliotecas relevantes."""
        for name, patterns in [
            ("fastapi",   ["fastapi"]),
            ("aiohttp",   ["aiohttp"]),
            ("django",    ["django"]),
            ("flask",     ["flask"]),
            ("tornado",   ["tornado"]),
            ("starlette", ["starlette"]),
        ]:
            if any(p in all_deps for p in patterns):
                if not info.framework:  # não sobrescreve se já veio do Dockerfile
                    info.framework = name
                vm = re.search(rf'{name}[^"\n]*[=><~^]+\s*["\']?(\d+[\d.]*)', all_deps)
                if vm:
                    info.framework_version = vm.group(1)
                break

        info.has_pandas     = bool(re.search(r'\bpandas\b', all_deps))
        info.has_numpy      = bool(re.search(r'\bnumpy\b', all_deps))
        info.has_scipy      = bool(re.search(r'\bscipy\b', all_deps))
        info.has_celery     = bool(re.search(r'\bcelery\b', all_deps))
        info.has_sqlalchemy = bool(re.search(r'\bsqlalchemy\b', all_deps))

    # ── deploy.yaml ───────────────────────────────────────────────────────

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

        # Resources
        resources = container.get("resources", {})
        reqs = resources.get("requests", {})
        lims = resources.get("limits", {})
        if reqs.get("cpu"):    info.cpu_request = str(reqs["cpu"])
        if reqs.get("memory"): info.mem_request = str(reqs["memory"])
        if lims.get("cpu"):    info.cpu_limit = str(lims["cpu"])
        if lims.get("memory"): info.mem_limit = str(lims["memory"])

        # DD tags — prioridade 1: env vars diretas ou via fieldRef
        env_vars = {}
        for env_entry in container.get("env", []):
            name = env_entry.get("name", "")
            if env_entry.get("value"):
                env_vars[name] = str(env_entry["value"])
            elif env_entry.get("valueFrom", {}).get("fieldRef"):
                field_path = env_entry["valueFrom"]["fieldRef"].get("fieldPath", "")
                lm = re.search(r"labels\['([^']+)'\]", field_path)
                if lm and lm.group(1) in pod_labels:
                    env_vars[name] = str(pod_labels[lm.group(1)])

        if "DD_SERVICE" in env_vars: info.dd_service = env_vars["DD_SERVICE"]
        if "DD_ENV" in env_vars:     info.dd_env = env_vars["DD_ENV"]
        if "DD_VERSION" in env_vars: info.dd_version = env_vars["DD_VERSION"]

        # Prioridade 2: Unified Service Tagging labels/annotations
        if not info.dd_service:
            info.dd_service = (
                pod_labels.get("tags.datadoghq.com/service")
                or pod_annotations.get("tags.datadoghq.com/service")
            )
        if not info.dd_env:
            info.dd_env = (
                pod_labels.get("tags.datadoghq.com/env")
                or pod_annotations.get("tags.datadoghq.com/env")
                or "production"
            )

        # Prioridade 3: labels genéricas
        if not info.dd_service:
            info.dd_service = (
                pod_labels.get("service")
                or pod_labels.get("app.kubernetes.io/name")
                or pod_labels.get("app")
            )

        # Prioridade 4: nome do Deployment (último recurso com warning)
        if not info.dd_service and info.k8s_deployment_name:
            info.dd_service = info.k8s_deployment_name
            info.warnings.append(
                f"DD_SERVICE não encontrado — usando nome do Deployment "
                f"'{info.dd_service}'. Valide se corresponde ao service tag no Datadog."
            )

    def _extract_hpa(self, doc: dict, info: GitHubAppInfo) -> None:
        spec = doc.get("spec", {})
        if spec.get("minReplicas") is not None:
            info.hpa_min_replicas = int(spec["minReplicas"])
        if spec.get("maxReplicas") is not None:
            info.hpa_max_replicas = int(spec["maxReplicas"])

        # HPA v1
        if spec.get("targetCPUUtilizationPercentage") is not None:
            info.hpa_cpu_target = int(spec["targetCPUUtilizationPercentage"])
            return

        # HPA v2
        for metric in spec.get("metrics", []):
            if metric.get("type") != "Resource":
                continue
            resource = metric.get("resource", {})
            avg = resource.get("target", {}).get("averageUtilization")
            if avg is None:
                continue
            if resource.get("name") == "cpu":    info.hpa_cpu_target = int(avg)
            elif resource.get("name") == "memory": info.hpa_mem_target = int(avg)

    def _parse_deploy_yaml_regex(self, content: str, info: GitHubAppInfo) -> None:
        """Fallback sem PyYAML."""
        for key, pat, attr in [
            ("cpu_request",       r'cpu:\s*["\']?(\d+m|\d+(?:\.\d+)?)["\']?',     "cpu_request"),
            ("mem_request",       r'memory:\s*["\']?(\d+(?:Mi|Gi|Ki|M|G))["\']?', "mem_request"),
            ("min_replicas",      r'minReplicas:\s*(\d+)',                          "hpa_min_replicas"),
            ("max_replicas",      r'maxReplicas:\s*(\d+)',                          "hpa_max_replicas"),
            ("cpu_target",        r'targetCPUUtilizationPercentage:\s*(\d+)',       "hpa_cpu_target"),
            ("dd_service_env",    r'DD_SERVICE["\s:]+value["\s:]+["\']?([^\s"\']+)', "dd_service"),
            ("dd_service_label",  r'tags\.datadoghq\.com/service["\s:]+["\']?([^\s"\']+)', "dd_service"),
        ]:
            m = re.search(pat, content, re.IGNORECASE)
            if m:
                val = m.group(1)
                current = getattr(info, attr, None)
                if not current:
                    if attr in ("hpa_min_replicas", "hpa_max_replicas", "hpa_cpu_target"):
                        setattr(info, attr, int(val))
                    else:
                        setattr(info, attr, val)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers de conversão k8s → Python
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
    for suffix, mult in [("Ki", 1024), ("Mi", 1024**2), ("Gi", 1024**3), ("Ti", 1024**4),
                          ("K", 1000), ("M", 1000**2), ("G", 1000**3)]:
        if val.endswith(suffix):
            try: return int(float(val[:-len(suffix)]) * mult)
            except ValueError: return 0
    try: return int(val)
    except ValueError: return 0


def github_info_to_app_config(info: GitHubAppInfo) -> dict:
    """
    Converte GitHubAppInfo em app_config completo para o pipeline.
    Elimina a necessidade do bloco apps: no settings.yaml.
    """
    name = info.dd_service or info.repo.split("/")[-1]
    return {
        "name": name,
        "namespace": info.k8s_namespace or "production",
        "dd_service": info.dd_service or name,
        "dd_env": info.dd_env,
        "framework": info.framework or "wsgi",
        "python_version": info.python_version or "2.7",
        "has_pandas": info.has_pandas or info.has_numpy or info.has_scipy,
        "sla_latency_p95_ms": 500.0,
        "peak_traffic_multiplier": 3.0,
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


def enrich_app_config(app_config: dict, info: GitHubAppInfo) -> dict:
    """Merge de GitHubAppInfo em app_config existente."""
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