k8s-resource-advisor/                          (3.100 linhas no total)
│
├── main.py                                    (470 linhas)
│   └── CLI: run | list | generate
│       ├── run       → descobre org → Datadog → YAML + relatório
│       ├── list      → mostra apps descobertas sem coletar métricas
│       └── generate  → regenera YAMLs do cache local
│
├── collectors/
│   ├── github.py                              (733 linhas)
│   │   ├── GitHubCollector.discover_apps()    → lista toda a org, filtra repos com deploy.yaml
│   │   ├── GitHubCollector.collect()          → coleta um repo específico
│   │   ├── _parse_deploy_yaml()               → resources, HPA, DD_SERVICE/DD_ENV
│   │   ├── _parse_dockerfile()                → python_version, workers, porta
│   │   ├── _parse_pyproject() / _parse_requirements()  → framework, pandas, numpy...
│   │   ├── github_info_to_app_config()        → converte GitHubAppInfo → app_config (sem bloco apps:)
│   │   └── enrich_app_config()                → merge em config existente (compat. com versão anterior)
│   │
│   └── datadog.py                             (366 linhas)
│       ├── AppMetrics (dataclass)             → estrutura de dados de métricas
│       └── DatadogCollector.collect()         → CPU, memória, RPS, OOMKills, throttle, réplicas
│
├── analyzers/
│   └── profiler.py                            (343 linhas)
│       ├── AnalyzedApp (dataclass)            → resultado da análise com percentis
│       ├── AppProfiler.analyze()              → classifica perfil (cpu/io/memory-bound)
│       ├── _compute_stats()                   → P50/P75/P95/P99 de CPU e memória
│       ├── _detect_memory_leak()              → regressão linear na série temporal
│       ├── _estimate_cpu_knee_point()         → ponto de saturação por pod
│       └── _generate_warnings()              → OOMKill, throttle, leak, Python EOL, startup lento
│
├── calculators/
│   ├── resources.py                           (192 linhas)
│   │   └── ResourceCalculator.calculate()
│   │       ├── cpu_request  = P75 × headroom (sobe para P95 se throttle detectado)
│   │       ├── cpu_limit    = request × burst (2x WSGI / 1.5x async)
│   │       ├── mem_request  = P75 × headroom × lib_factor (pandas +20%)
│   │       └── mem_limit    = request × headroom (P99 se leak, +30% se OOMKills)
│   │
│   └── hpa.py                                 (228 linhas)
│       └── HPACalculator.calculate()
│           ├── minReplicas  = histórico P10 + startup bump
│           ├── maxReplicas  = RPS pico projetado / RPS por pod
│           ├── cpu_target   = (knee / limit) × safety_margin → clamp [40%, 80%]
│           ├── mem_target   = 75% (65% se leak detectado)
│           └── behavior     = scaleUp rápido / scaleDown conservador (ajuste por startup time)
│
├── reporters/
│   ├── yaml_gen.py                            (233 linhas)
│   │   ├── generate_resources_patch()         → Strategic Merge Patch do Deployment
│   │   ├── generate_hpa_manifest()            → HPA v2 com behavior completo
│   │   └── write_manifests()                  → escreve os 2 YAMLs em output/manifests/
│   │
│   └── report.py                              (265 linhas)
│       └── generate_report()                  → HTML com comparativo atual vs recomendado
│
├── config/
│   └── settings.example.yaml                  (60 linhas)
│       ├── github: org + token                → sem bloco apps:
│       ├── datadog: api_key + app_key + env
│       ├── thresholds: headrooms e targets
│       └── overrides: ajustes opcionais por dd_service
│
├── tools/
│   └── mock_data.py                           (200 linhas)
│       └── Gera cache fake (metrics + github) para testar sem credenciais
│
└── requirements.txt                           (10 linhas)
    datadog-api-client, PyYAML, click, pandas, numpy,
    jinja2, rich, python-dotenv, requests, matplotlib


# Instala tudo
poetry install

# Roda direto pelo entrypoint configurado
poetry run advisor run --weeks 4

# Ou com o ambiente ativado
poetry shell
advisor run --weeks 4
advisor list