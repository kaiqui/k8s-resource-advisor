import dataclasses
import json
import math
import os
import random
from pathlib import Path

# ── Apps de exemplo representando os perfis comuns ──────────────────────────

MOCK_APPS = [
    dict(
        # FastAPI async — bem configurado, sem problemas
        name="api-gateway", framework="fastapi", python_version="3.11",
        cpu_base=0.30, mem_base_mb=180, replicas=5, throttle=0.01, sla_ms=200,
        cpu_request="200m", cpu_limit="500m", mem_request="256Mi", mem_limit="512Mi",
        hpa_min=2, hpa_max=10, hpa_cpu=70,
        dd_service="api-gateway", namespace="production",
        startup_cmd="uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 2",
        dep_manager="poetry",
    ),
    dict(
        # WSGI Python 2.7 com OOMKills e throttling alto — caso crítico
        name="legacy-auth", framework="wsgi", python_version="2.7",
        cpu_base=0.50, mem_base_mb=320, replicas=3, throttle=0.22, oom_kills=4, sla_ms=800,
        cpu_request="500m", cpu_limit="1000m", mem_request="512Mi", mem_limit="1Gi",
        hpa_min=2, hpa_max=8, hpa_cpu=60,
        dd_service="legacy-auth", namespace="production",
        startup_cmd="gunicorn app:app -w 4 -b 0.0.0.0:8000",
        dep_manager="pip",
    ),
    dict(
        # Flask com pandas — memory-bound, lib pesada
        name="data-processor", framework="flask", python_version="3.9",
        cpu_base=0.80, mem_base_mb=512, replicas=4, has_pandas=True, sla_ms=1000,
        cpu_request="750m", cpu_limit="1500m", mem_request="1Gi", mem_limit="2Gi",
        hpa_min=2, hpa_max=10, hpa_cpu=65,
        dd_service="data-processor", namespace="production",
        startup_cmd="gunicorn app:app -w 2 -b 0.0.0.0:5000",
        dep_manager="pip",
    ),
    dict(
        # aiohttp async — I/O-bound, sub-utilizado
        name="notification-svc", framework="aiohttp", python_version="3.10",
        cpu_base=0.15, mem_base_mb=128, replicas=2, sla_ms=300,
        cpu_request="100m", cpu_limit="300m", mem_request="128Mi", mem_limit="256Mi",
        hpa_min=2, hpa_max=6, hpa_cpu=70,
        dd_service="notification-svc", namespace="production",
        startup_cmd="python -m notification_svc",
        dep_manager="poetry",
    ),
    dict(
        # Django com throttling moderado
        name="billing-api", framework="django", python_version="3.8",
        cpu_base=0.40, mem_base_mb=256, replicas=3, throttle=0.15, sla_ms=500,
        cpu_request="400m", cpu_limit="800m", mem_request="384Mi", mem_limit="768Mi",
        hpa_min=2, hpa_max=10, hpa_cpu=60,
        dd_service="billing-api", namespace="production",
        startup_cmd="gunicorn billing.wsgi:application -w 3 -b 0.0.0.0:8000",
        dep_manager="pip",
    ),
]


def sine_series(n, base, amp, noise=0.05):
    result = []
    for i in range(n):
        val = base + amp * math.sin(2 * math.pi * i / 288)
        val += base * math.sin(2 * math.pi * i / (288 * 7)) * 0.3
        val += random.gauss(0, base * noise)
        result.append(max(0.0, val))
    return result


def generate(out_dir="./output/data", weeks=2):
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    n = int(weeks * 7 * 24 * 3600 / 300)

    for app in MOCK_APPS:
        name = app["name"]

        # ── Métricas (simula Datadog) ────────────────────────────────────
        metrics = {
            "app_name": name,
            "namespace": app.get("namespace", "production"),
            "dd_service": app["dd_service"],
            "dd_env": "production",
            "framework": app["framework"],
            "python_version": app["python_version"],
            "collection_start": "2025-10-24T00:00:00+00:00",
            "collection_end": "2025-11-21T00:00:00+00:00",
            "weeks_collected": weeks,
            "cpu_usage_cores": sine_series(n, app["cpu_base"], app["cpu_base"] * 0.4),
            "cpu_throttle_ratio": [
                max(0, app.get("throttle", 0.02) + random.gauss(0, 0.01))
                for _ in range(n)
            ],
            "memory_bytes": sine_series(
                n, app["mem_base_mb"] * 1024**2,
                app["mem_base_mb"] * 0.2 * 1024**2, noise=0.02
            ),
            "rps": sine_series(n, 50.0, 30.0),
            "replica_count": [
                float(app["replicas"]) + random.randint(-1, 2) for _ in range(n)
            ],
            "oom_kill_count": app.get("oom_kills", 0),
            "startup_p50_seconds": {"fastapi":3,"flask":5,"wsgi":6,"django":9,"aiohttp":2.5}.get(app["framework"],5),
            "startup_p95_seconds": {"fastapi":8,"flask":14,"wsgi":16,"django":22,"aiohttp":7}.get(app["framework"],14),
            "current_cpu_request_cores": 0.0,  # será preenchido pelo GitHub cache
            "current_cpu_limit_cores":   0.0,
            "current_mem_request_bytes": 0.0,
            "current_mem_limit_bytes":   0.0,
            "has_pandas": app.get("has_pandas", False),
            "sla_latency_p95_ms": app.get("sla_ms", 500.0),
            "peak_traffic_multiplier": 3.0,
        }

        with open(f"{out_dir}/{name}-metrics.json", "w") as f:
            json.dump(metrics, f, indent=2)

        # ── Info GitHub (simula discovery) ───────────────────────────────
        github = {
            "repo": f"minha-org/{name}",
            "default_branch": "main",
            "dd_service": app["dd_service"],
            "dd_env": "production",
            "dd_version": None,
            "python_version": app["python_version"],
            "base_image": f"python:{app['python_version']}-slim",
            "startup_command": app.get("startup_cmd"),
            "gunicorn_workers": (
                int(w.group(1))
                if (w := __import__("re").search(r"-w\s+(\d+)", app.get("startup_cmd", "")))
                else None
            ),
            "uvicorn_workers": (
                int(w.group(1))
                if (w := __import__("re").search(r"--workers\s+(\d+)", app.get("startup_cmd", "")))
                else None
            ),
            "exposed_port": 8000,
            "framework": app["framework"],
            "framework_version": None,
            "python_requires": f">={app['python_version']}",
            "has_pandas": app.get("has_pandas", False),
            "has_numpy": app.get("has_pandas", False),
            "has_scipy": False,
            "has_celery": False,
            "has_sqlalchemy": False,
            "dependency_manager": app.get("dep_manager", "pip"),
            "raw_dependencies": [],
            "deploy_yaml_found": True,
            "k8s_namespace": app.get("namespace", "production"),
            "k8s_deployment_name": name,
            "k8s_replicas": app["replicas"],
            "cpu_request": app["cpu_request"],
            "cpu_limit": app["cpu_limit"],
            "mem_request": app["mem_request"],
            "mem_limit": app["mem_limit"],
            "hpa_min_replicas": app["hpa_min"],
            "hpa_max_replicas": app["hpa_max"],
            "hpa_cpu_target": app["hpa_cpu"],
            "hpa_mem_target": None,
            "hpa_api_version": "autoscaling/v2",
            "warnings": [],
            "files_found": ["Dockerfile", "pyproject.toml" if app.get("dep_manager") == "poetry" else "requirements.txt", "manifests/kubernetes/main/deploy.yaml"],
            "files_missing": [],
            "skipped": False,
            "skip_reason": "",
        }

        with open(f"{out_dir}/{name}-github.json", "w") as f:
            json.dump(github, f, indent=2)

        print(f"  ✓ {name}")

    print(f"\nDados mock gerados em {out_dir}/")
    print("Rode: python main.py generate --output ./output")


if __name__ == "__main__":
    generate()
