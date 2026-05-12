# snack-shack

An end-to-end data engineering pipeline that ingests raw food reviews, validates and cleans them using PySpark, and produces high-quality training data for an AI model — orchestrated by Airflow, containerized with Docker, and deployed on Kubernetes.

| Raw reviews | Accepted | Rejected | Quality rules |
|---|---|---|---|
| 203 | 6 | 197 | 7 |

---

## Pipeline Flow

Each step must succeed before the next begins. A failure stops the run and surfaces in the Airflow UI.

```
Raw reviews (203 records)
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│ Step 1 · generate_raw_data                      [BashOperator]  │
│ src/generate_data.py                                            │
│                                                                 │
│ Simulates 203 raw food reviews with intentional noise:          │
│ duplicates, PII, invalid ratings, gibberish, non-English text.  │
│                                                                 │
│ Output → src/data/raw/snackshack_reviews.jsonl                  │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│ Step 2 · run_spark_pipeline                     [BashOperator]  │
│ src/spark_pipeline.py                                           │
│                                                                 │
│ Reads raw reviews into a Spark DataFrame. Normalizes text,      │
│ detects PII, deduplicates, validates rating/length/language,    │
│ computes a quality score, and writes three Parquet outputs.     │
│                                                                 │
│ Output → src/data/processed/ · rejected/ · metrics/            │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│ Step 3 · validate_outputs_exist               [PythonOperator]  │
│ dags/snackshack_pipeline_dag.py                                 │
│                                                                 │
│ Checks that the processed and metrics Parquet folders were      │
│ actually written to disk. Fails the run if any output is        │
│ missing.                                                        │
│                                                                 │
│ Output → Pass / Fail signal only                                │
└─────────────────────────────────────────────────────────────────┘
```

---

## Quality Rules

Defined in `src/quality_rules.py` and tested in `tests/test_quality_rules.py`. Applied by both the local pipeline and the Spark pipeline.

| Rule | Logic |
|---|---|
| Normalization | Lowercase, strip whitespace, replace emails → `[EMAIL]`, phones → `[PHONE]` |
| PII Detection | Flags reviews containing email addresses or phone numbers |
| Rating Validation | Accepts only ratings in {1, 2, 3, 4, 5} |
| Length Validation | Text must be between 10 and 500 characters |
| Language Detection | Simple heuristic: must contain common English words |
| Deduplication | Removes exact duplicate normalized texts |
| Quality Score | Starts at 1.0 — penalizes short text −0.3, PII −0.2, null rating −0.2, gibberish −0.5 |

---

## Why Each Technology

**PySpark** · Cleaning engine  
Processes data as a distributed DataFrame across many CPU cores in parallel. The same pipeline that handles 200 reviews locally can handle 1 billion reviews on a cluster with zero code changes — just point it at a different Spark master.

**Parquet** · Output format  
Columnar binary format that compresses 3–5x over raw JSON. A downstream model training job that only needs `text` and `quality_score` reads just those two columns, skipping the rest entirely. Enables partition pruning at scale.

**Airflow** · Scheduler / orchestrator  
Orchestrates the three steps in the right order, retries on failure, provides a UI to monitor every run, and can schedule the pipeline to run automatically on a cron schedule with no manual intervention.

**Docker** · Containerization  
Packages Airflow, Java, PySpark, and all Python dependencies into a single image that runs identically on any machine. Eliminates "works on my machine" problems and makes the pipeline reproducible.

**Kubernetes** · Container orchestration  
Manages the container at the infrastructure level. If the pod crashes, Kubernetes automatically restarts it. At production scale, it can run many pods across many machines, schedule jobs on spare capacity, and auto-scale based on load.

---

## Infrastructure Layers

> Your code lives in `src/` and `dags/`. **Docker** packages it. **Kubernetes** runs and manages it. **Airflow** schedules and monitors it. Each layer is independent — you can swap any one without rewriting the others.

| Layer | Tool | What it manages | Config file |
|---|---|---|---|
| Code | Python | Business logic — cleaning rules, Spark jobs, DAG definition | `src/` · `dags/` |
| Dependencies | pip + requirements.txt | Python packages baked into the image at build time | `requirements.txt` · `Dockerfile` |
| Container image | Docker | Reproducible runtime: OS + Java + Airflow + PySpark | `Dockerfile` |
| Local dev | Docker Compose | Single-container setup, one command to run | `docker-compose.yml` |
| Orchestration | Kubernetes | Pod lifecycle, restarts, resource limits, networking | `k8s/*.yaml` |
| Scheduling | Airflow | Task ordering, retries, monitoring, cron scheduling | `dags/*.py` |

---

## Parquet Output Structure

Spark writes three separate Parquet directories. Each is a folder of `part-XXXXX.snappy.parquet` files — Snappy-compressed, columnar binary that compresses 3–5x versus raw JSON.

| Output path | Contents | Consumer |
|---|---|---|
| `src/data/processed/spark_cleaned_reviews/` | Accepted reviews: normalized text, rating, quality score, dataset version | Model training job |
| `src/data/rejected/spark_rejected_reviews/` | Rejected reviews with rejection reason flags attached | Data quality audit |
| `src/data/metrics/spark_metrics/` | Aggregate stats: post-dedup count, avg/min/max quality score | Monitoring dashboard |

---

## Repository Structure

| File | Purpose |
|---|---|
| `src/generate_data.py` | Generates simulated raw reviews with noise |
| `src/quality_rules.py` | Pure functions: normalize, PII check, rating/length/language validation, quality score |
| `src/local_cleaning.py` | Single-machine version of the pipeline (no Spark, writes .jsonl) |
| `src/spark_pipeline.py` | Distributed version using PySpark — reads, cleans, deduplicates, writes Parquet |
| `dags/snackshack_pipeline_dag.py` | Airflow DAG wiring the 3 tasks in order with dependency arrows |
| `tests/test_quality_rules.py` | 7 pytest unit tests validating every quality rule function |
| `Dockerfile` | Builds a custom image: Airflow 3 + Java (for PySpark) + requirements.txt |
| `docker-compose.yml` | Single-container local dev setup — one command to run Airflow |
| `k8s/namespace.yaml` | Kubernetes namespace isolating all snackshack resources |
| `k8s/configmap.yaml` | Airflow environment config injected into the pod |
| `k8s/pvc.yaml` | 2Gi persistent volume for data that survives pod restarts |
| `k8s/deployment.yaml` | Runs the Airflow pod with resource limits and self-healing |
| `k8s/service.yaml` | Exposes Airflow UI on NodePort 30080 |
| `pyproject.toml` | Tells pytest to include project root on Python path |

---

## Scale Story

The pipeline is production-ready in architecture. Only the infrastructure config changes at scale — the code does not.

| Dimension | Today (local) | At 1 TB | At 1 PB |
|---|---|---|---|
| Data source | Local .jsonl file (203 rows) | S3 / GCS bucket | S3 / GCS with date partitioning |
| Spark master | `local[*]` (all Mac cores) | Spark cluster, 20 nodes | Databricks / EMR, 500+ nodes |
| Partitions | 1 output file | ~1,000 part files | ~250,000 part files (coalesced) |
| Airflow backend | SQLite + standalone | PostgreSQL + CeleryExecutor | PostgreSQL + KubernetesExecutor |
| Deduplication | `dropDuplicates()` exact match | `dropDuplicates()` across cluster | Bloom filter pre-screen + shuffle |
| Orchestration | Docker Desktop Kubernetes | Managed Kubernetes (EKS/GKE) | Custom internal platform |

---

## Running the Pipeline

### Local (no Docker)

```bash
# Run tests
python -m pytest tests/

# Run local cleaning pipeline
python -m src.local_cleaning

# Run Spark pipeline
python -m src.spark_pipeline
```

### Docker Compose

```bash
docker compose up --build
# Open http://localhost:8080 · login with credentials from logs
```

### Kubernetes

```bash
# Build and load image into cluster
docker build -t snack-shack-airflow:latest --load .
docker save snack-shack-airflow:latest | docker exec -i desktop-control-plane ctr --namespace k8s.io images import -

# Deploy
kubectl apply -f k8s/

# Access UI
kubectl port-forward service/airflow 8082:8080 -n snackshack
# Open http://localhost:8082
```

---

## Viewing Results

```bash
# Accepted reviews (clean training data)
kubectl exec -n snackshack deployment/airflow -- python -c "
import pandas as pd
df = pd.read_parquet('src/data/processed/spark_cleaned_reviews')
print(df.to_string())
"

# Pipeline metrics
kubectl exec -n snackshack deployment/airflow -- python -c "
import pandas as pd
df = pd.read_parquet('src/data/metrics/spark_metrics')
print(df.to_string())
"

# Rejected reviews with reasons
kubectl exec -n snackshack deployment/airflow -- python -c "
import pandas as pd
df = pd.read_parquet('src/data/rejected/spark_rejected_reviews')
print(df[['id','text','reject_reasons']].head(10).to_string())
"
```
