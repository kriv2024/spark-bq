# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install with dev + jupyter extras
pip install -e .[dev,jupyter]

# Tests
python -m pytest tests/ -v
python -m pytest tests/test_data_processing.py -v          # single file
python -m pytest tests/ --cov=src --cov-report=term-missing

# Code quality (run in order before committing)
python -m black src/ tests/ scripts/
python -m isort src/ tests/ scripts/
python -m flake8 src/ tests/ scripts/
python -m mypy src/

# Run pipeline directly
python src/pipelines/sample_pipeline.py

# Jupyter
python -m jupyter lab --notebook-dir=notebooks
```

## Architecture

### Configuration layer (`config/`)
`config/settings.py` defines three Pydantic `BaseSettings` classes with env var prefixes:
- `SparkConfig` — prefix `SPARK_*`, controls memory, serializer, BQ connector JAR version
- `BigQueryConfig` — prefix `BQ_*`, plus `GOOGLE_CLOUD_PROJECT_ID` and `BIGQUERY_DATASET`
- `AppConfig` — no prefix, controls log level/format

Use `get_spark_config()` / `get_bigquery_config()` / `get_app_config()` — never instantiate the classes directly. All config comes from `.env` (copy from `.env.example`).

### Spark session (`src/spark_session.py`)
`SparkSessionManager` is a class-level singleton. The BigQuery connector JAR (`spark-bigquery-with-dependencies_2.12`) is downloaded at session creation via `spark.jars.packages`. Always use the `get_spark_session(app_name)` convenience wrapper — it handles the singleton and passes config from `get_spark_config()`. Adaptive query execution (AQE) is enabled by default.

### BigQuery connector (`src/bigquery_connector.py`)
`BigQueryConnector` wraps two distinct clients:
1. Spark BigQuery connector (via `df.read/write.format("bigquery")`) — used for `read_table()`, `write_table()`, `execute_query()`
2. `google-cloud-bigquery` Python client (lazy `_client` property) — used only for `get_table_schema()`

Table ID normalization: bare name → `project.dataset.table`; one dot → `project.dataset.table`; two dots → used as-is.

### Pipeline pattern (`src/pipelines/`)
New pipelines follow the extract→transform→load pattern in `SamplePipeline`. Each pipeline instantiates its own `SparkSession` (via singleton) and `BigQueryConnector`. Validation (`validate_required_columns`, `check_data_quality`) runs inside `transform_data` before any mutations.

### Transformations (`src/transformations/common_transforms.py`)
Stateless functions returning new DataFrames. Key functions: `standardize_column_names`, `add_audit_columns` (adds `load_timestamp`, `source_system`, `load_id`), `create_date_dimensions`, `apply_business_rules` (rule-config-driven), `aggregate_metrics`.

### Logging
All modules call `get_logger(__name__)` from `config/logging.py`. Outputs JSON by default (structlog). Set `LOG_FORMAT=console` in `.env` for human-readable output during local development.

## Study scope and roadmap

This is a **learning/interview-prep project**, not a production system. The goal is deep understanding of Spark internals for senior/lead data engineer roles.

**Phase 1 — Local:** All development runs locally via `.venv`. Validate concepts, build intuition.

**Phase 2 — GCP:** Migrate to Dataproc (managed Spark), Cloud Composer/Airflow (orchestration), and use the Spark UI for job inspection and optimization.

### Core Spark concepts this codebase should demonstrate
- **Execution model:** RDD → DataFrame → Dataset abstraction layers; driver vs. worker roles; jobs → stages → tasks decomposition
- **Lazy evaluation:** transformations build a DAG, actions trigger execution — always identify which operations are lazy vs. eager
- **Shuffling:** wide vs. narrow transformations; when a stage boundary is introduced; cost of shuffles on partitions
- **Optimizations:** Catalyst optimizer, Tungsten execution engine, AQE (enabled in `config/settings.py`), broadcast joins, partition pruning
- **Caching/persistence:** when to cache, storage levels, cache invalidation
- **Partitioning:** default parallelism, repartition vs. coalesce, skew handling

When adding new code, annotate with which of these concepts it exercises so the codebase doubles as a study reference.

## Active tools and skills

| Tool / Skill | When to use |
|---|---|
| `/fewer-permission-prompts` | Run once after setup to pre-authorize pytest, spark, file reads |
| `/simplify` | After writing any new Spark code — checks for concept anti-patterns (eager actions, missing caches, suboptimal joins) |
| `/review` | After completing each concept module — ask it to verify the code *demonstrates* the intended Spark mechanism, not just that it runs |
| `EnterPlanMode` | Before implementing a new concept exercise — align on approach first, especially for partitioning/shuffle topics where setup determines what gets demonstrated |
| `TodoWrite` | Track which Spark concepts are covered, in progress, or pending |
| `Explore` subagent | Cross-codebase searches ("find all shuffle-triggering operations", "find all eager actions") — keeps main context clean |
| `NotebookEdit` | Direct editing of `notebooks/` for interactive concept exploration (DAG inspection, `.explain()` output, quick experiments) |
| `WebSearch` / `WebFetch` | Official Spark docs, Databricks internals posts, GCP Dataproc migration guides |

## What NOT to scan
- `.venv/` — virtual environment
- `notebooks/` — exploratory notebooks, not production code
- `data/` — excluded from git, runtime only
- `src/_version.py` — auto-generated by `setuptools_scm`
