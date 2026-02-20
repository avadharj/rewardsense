"""
RewardSense - Main Data Pipeline DAG

Orchestrates the end-to-end data pipeline for the RewardSense
credit card recommendation system.

Schedule: Weekly (Sunday 6:00 AM UTC)
Owner: rewardsense

Pipeline stages:
    1. Ingestion   — Scrape card data, fetch API, generate synthetic data
    2. Preprocessing — Clean, feature-engineer, and transform datasets
    3. Versioning  — Version artifacts with DVC
    4. Reporting   — Generate pipeline report, log metrics, send alerts

Task Groups:
    ingestion/      Parallel data acquisition from multiple sources
    preprocessing/  Sequential cleaning → features → transform
    versioning/     DVC add + push (placeholder for Story 5.4)
    reporting/      Report generation, metrics logging, and alerting

Notes:
    - Task callables use deferred imports (import inside function body)
      to keep DAG parsing fast and avoid import-time failures.
    - Story 5.1 defines the DAG structure with placeholder task bodies.
      Stories 5.2 and 5.3 will wire in real implementations.
    - Story 5.5 implements monitoring, alerting, and callbacks.
"""

from datetime import datetime, timedelta
from pathlib import Path
import json
import time
import traceback

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


# =============================================================================
# DAG documentation (rendered as markdown in the Airflow UI)
# =============================================================================

DAG_DOC_MD = """
## RewardSense Data Pipeline

### Overview
Weekly pipeline that ingests credit card data from multiple sources,
generates synthetic user/transaction data, cleans and transforms
everything, then versions the output with DVC.

### Pipeline Flow

┌─────────────────── Ingestion ───────────────────┐
│                                                  │
│  scrape_nerdwallet ──┐                           │
│  scrape_issuers ─────┼──► merge_card_data        │
│  fetch_api_data ─────┘                           │
│  generate_synthetic_data (parallel)              │
│                                                  │
└──────────────────────────────────────────────────┘
│
▼
┌─────────────── Preprocessing ───────────────────┐
│                                                  │
│  clean_data ──► engineer_features                │
│                      │                           │
│                      ▼                           │
│              run_transform_pipeline              │
│                                                  │
└──────────────────────────────────────────────────┘
│
▼
┌──────────────── Versioning ─────────────────────┐
│  version_with_dvc                                │
└──────────────────────────────────────────────────┘
│
▼
┌───────────────── Reporting ─────────────────────┐
│                                                  │
│  generate_pipeline_report                        │
│         │                                        │
│         ├──► log_pipeline_metrics                │
│         └──► send_pipeline_alerts                │
│                                                  │
└──────────────────────────────────────────────────┘

### Data Sources
| Source | Type | Module |
|--------|------|--------|
| NerdWallet | Web scrape | `scrapers.NerdWalletScraper` |
| Chase, Amex, Citi, Capital One, Discover | Web scrape | `scrapers.issuer_scrapers` |
| CreditCardBonuses API | REST API | `api_fetcher.CreditCardBonusesClient` |
| Synthetic users & transactions | Generator | `generators.*` |

### Configuration
- Scraper config: `config/scraper_config.yaml`
- Transform config: `config/transform.yaml`
- Generator config: `config/generator_config.yaml`
- Alerting config: `config/alerting_config.yaml`

### Contacts
- **Owner**: RewardSense Team
"""


# =============================================================================
# Callbacks (deferred imports to keep DAG parsing lightweight)
# =============================================================================


def _on_task_failure(context):
    """Route task failures to AlertDispatcher (CRITICAL)."""
    from data_pipeline.monitoring.callbacks import on_task_failure_callback

    on_task_failure_callback(context)


def _on_dag_success(context):
    """Send a summary alert when the full DAG succeeds."""
    from data_pipeline.monitoring.callbacks import on_dag_success_callback

    on_dag_success_callback(context)


# =============================================================================
# Default args
# =============================================================================

default_args = {
    "owner": "rewardsense",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # Keep a generous global timeout, but set tighter per-task timeouts below.
    "execution_timeout": timedelta(hours=4),
    "sla": timedelta(hours=3),
    "on_failure_callback": _on_task_failure,
}


# =============================================================================
# Helpers (Story 5.2)
# =============================================================================


def _repo_root() -> Path:
    # dags/ is at <repo>/dags, so parents[1] is repo root
    return Path(__file__).resolve().parents[1]


def _stage_root(context) -> Path:
    """
    Shared staging directory for THIS DAG RUN.

    IMPORTANT:
      scripts/download_data.py performs an atomic commit to data/processed/current.
      If each ingestion task committed independently, they'd overwrite each other.
      So: each ingestion task writes into a shared stage dir, and ONLY merge commits.
    """
    repo = _repo_root()
    dag_id = (
        context["dag"].dag_id if context.get("dag") else "rewardsense_data_pipeline"
    )
    run_id = context["dag_run"].run_id if context.get("dag_run") else "manual"
    stage = repo / "data" / ".airflow_staging" / dag_id / run_id
    stage.mkdir(parents=True, exist_ok=True)
    return stage


def _on_failure_callback(context):
    """
    Minimal alert/logging hook (can be swapped later for Slack/email).
    """
    ti = context.get("task_instance")
    dag = context.get("dag")
    msg = (
        f"[RewardSense DAG Failure] dag={dag.dag_id if dag else 'unknown'} "
        f"task={ti.task_id if ti else 'unknown'} run_id={context.get('run_id')} "
        f"execution_date={context.get('execution_date')}"
    )
    print(msg)
    err = context.get("exception")
    if err:
        print("Exception:", repr(err))
        print(traceback.format_exc())


# =============================================================================
# Task callables
#
# Ingestion (Story 5.2): real implementations
# Preprocessing/Reporting remain placeholders for later stories.
# =============================================================================


def _scrape_nerdwallet(**context):
    """Scrape credit card data from NerdWallet into the per-run staging dir."""
    import logging

    logger = logging.getLogger("airflow.task")
    stage = _stage_root(context)

    try:
        # Deferred import to keep DAG parsing lightweight
        from scripts.download_data import run_nerdwallet_scraper

        offers, n, files = run_nerdwallet_scraper(
            stage_dir=stage,
            logger=logger,
            use_selenium=False,
        )

        meta = {
            "source": "nerdwallet",
            "records": n,
            "stage_root": str(stage),
            "files": [str(p) for p in files],
            "ts": time.time(),
        }
        logger.info("✅ NerdWallet scrape complete: %s", meta)
        return meta

    except Exception as e:
        logger.error("❌ NerdWallet scrape failed: %s", e, exc_info=True)
        raise AirflowException(f"NerdWallet scrape failed: {type(e).__name__}: {e}")


def _scrape_issuers(**context):
    """Scrape credit card data from issuer websites into the per-run staging dir."""
    import logging

    logger = logging.getLogger("airflow.task")
    stage = _stage_root(context)

    # Match your docstring list (and your codebase convention)
    issuers = ["chase", "amex", "citi", "capitalone", "discover"]

    try:
        from scripts.download_data import run_issuer_scrapers

        offers, n, files = run_issuer_scrapers(
            stage_dir=stage,
            logger=logger,
            issuers=issuers,
        )

        meta = {
            "source": "issuers",
            "issuers_scraped": issuers,
            "records": n,
            "stage_root": str(stage),
            "files": [str(p) for p in files],
            "ts": time.time(),
        }
        logger.info("✅ Issuer scrapes complete: %s", meta)
        return meta

    except Exception as e:
        logger.error("❌ Issuer scrapes failed: %s", e, exc_info=True)
        raise AirflowException(f"Issuer scrapes failed: {type(e).__name__}: {e}")


def _fetch_api_data(**context):
    """Fetch credit card data from the CreditCardBonuses API into the per-run staging dir."""
    import logging

    logger = logging.getLogger("airflow.task")
    stage = _stage_root(context)

    try:
        from scripts.download_data import run_creditcardbonuses_api

        offers, n, files = run_creditcardbonuses_api(
            stage_dir=stage,
            logger=logger,
            include_raw=False,
        )

        meta = {
            "source": "creditcardbonuses_api",
            "records": n,
            "stage_root": str(stage),
            "files": [str(p) for p in files],
            "ts": time.time(),
        }
        logger.info("✅ API fetch complete: %s", meta)
        return meta

    except Exception as e:
        logger.error("❌ API fetch failed: %s", e, exc_info=True)
        raise AirflowException(f"API fetch failed: {type(e).__name__}: {e}")


def _generate_synthetic_data(**context):
    """Generate synthetic user profiles and transaction data into the per-run staging dir."""
    import logging

    logger = logging.getLogger("airflow.task")
    stage = _stage_root(context)

    try:
        from scripts.download_data import run_synthetic_generators

        # Defaults can be moved to Airflow Variables later if you want
        num_users = 500
        history_months = 6
        seed = 42

        preview, total_records, files = run_synthetic_generators(
            stage_dir=stage,
            logger=logger,
            num_users=num_users,
            history_months=history_months,
            seed=seed,
            fmt="csv",
        )

        meta = {
            "source": "synthetic",
            "params": {
                "num_users": num_users,
                "history_months": history_months,
                "seed": seed,
            },
            "records": total_records,
            "stage_root": str(stage),
            "files": [str(p) for p in files],
            "preview": preview,  # small; safe to pass via XCom
            "ts": time.time(),
        }
        logger.info("✅ Synthetic generation complete (preview omitted from log).")
        return meta

    except Exception as e:
        logger.error("❌ Synthetic generation failed: %s", e, exc_info=True)
        raise AirflowException(f"Synthetic generation failed: {type(e).__name__}: {e}")


def _merge_card_data(**context):
    """
    Merge/dedupe card data from scrapers+API and atomically commit stage -> data/processed/current.
    Synthetic generation runs in parallel and also lands in stage/, so the commit includes it too.
    """
    import logging

    logger = logging.getLogger("airflow.task")
    stage = _stage_root(context)
    repo = _repo_root()
    processed_dir = repo / "data" / "processed"

    try:
        ti = context["ti"]

        nerd = ti.xcom_pull(task_ids="ingestion.scrape_nerdwallet")
        issuers = ti.xcom_pull(task_ids="ingestion.scrape_issuers")
        api = ti.xcom_pull(task_ids="ingestion.fetch_api_data")

        logger.info("Upstream XCom nerdwallet=%s", nerd)
        logger.info("Upstream XCom issuers=%s", issuers)
        logger.info("Upstream XCom api=%s", api)

        offers_dir = stage / "offers"
        offers_dir.mkdir(parents=True, exist_ok=True)

        # Merge all offers JSONs in stage/offers/*.json into stage/offers/merged_offers.json
        all_offers = []
        for p in sorted(offers_dir.glob("*.json")):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    payload = json.load(f)
                if (
                    isinstance(payload, dict)
                    and "offers" in payload
                    and isinstance(payload["offers"], list)
                ):
                    all_offers.extend(payload["offers"])
            except Exception as e:
                logger.warning("Skipping unreadable offers file %s: %s", p, e)

        # Simple dedupe by (issuer, card_name) when present
        seen = set()
        deduped = []
        dupes = 0
        for o in all_offers:
            issuer = (
                (o.get("issuer") or o.get("bank") or o.get("source") or "")
                .strip()
                .lower()
            )
            name = (o.get("card_name") or o.get("name") or "").strip().lower()
            key = (issuer, name)
            if name and key in seen:
                dupes += 1
                continue
            seen.add(key)
            deduped.append(o)

        merged_path = offers_dir / "merged_offers.json"
        merged_payload = {
            "source": "merged",
            "fetched_at": time.time(),
            "total_offers": len(all_offers),
            "deduped_offers": len(deduped),
            "duplicates_removed": dupes,
            "offers": deduped,
        }
        with open(merged_path, "w", encoding="utf-8") as f:
            json.dump(merged_payload, f, indent=2, ensure_ascii=False)
            f.write("\n")

        # Atomic commit of the entire stage dir to data/processed/current
        from scripts.download_data import commit_stage_to_processed

        commit_stage_to_processed(
            stage_dir=stage, processed_dir=processed_dir, logger=logger
        )

        committed_current = processed_dir / "current"
        meta = {
            "total_merged_cards": len(deduped),
            "duplicates_removed": dupes,
            "merged_offers_path": str(merged_path),
            "stage_root": str(stage),
            "committed_current": str(committed_current),
            "ts": time.time(),
        }
        logger.info("✅ Merge+commit complete: %s", meta)
        return meta

    except Exception as e:
        logger.error("❌ Merge/commit failed: %s", e, exc_info=True)
        raise AirflowException(f"Merge/commit failed: {type(e).__name__}: {e}")


# ------------------------- placeholders (Story 5.3+) ---------------------------


def _clean_data(**context):
    """Run data cleaning on all datasets."""
    import logging

    logger = logging.getLogger("airflow.task")
    logger.info("🧹 [PLACEHOLDER] Cleaning credit card, transaction, and user data...")
    return {"datasets_cleaned": 3, "status": "placeholder"}


def _engineer_features(**context):
    """Run feature engineering on cleaned datasets."""
    import logging

    logger = logging.getLogger("airflow.task")
    logger.info("⚙️ [PLACEHOLDER] Engineering features for cards and transactions...")
    return {"features_engineered": 0, "status": "placeholder"}


def _run_transform_pipeline(**context):
    """Run the full transformation pipeline."""
    import logging

    logger = logging.getLogger("airflow.task")
    logger.info("🔄 [PLACEHOLDER] Running TransformationPipeline...")
    return {"transform_status": "placeholder"}


# =============================================================================
# Reporting / Monitoring task callables  (Story 5.5)
# =============================================================================


def _generate_pipeline_report(**context):
    """Generate a summary report of the pipeline run."""
    from data_pipeline.monitoring.pipeline_report import PipelineReportGenerator

    generator = PipelineReportGenerator()
    return generator.generate(context)


def _log_pipeline_metrics(**context):
    """Log timing, record counts, and error metrics for the pipeline run."""
    from data_pipeline.monitoring.metrics import PipelineMetricsLogger

    logger = PipelineMetricsLogger()
    return logger.log_metrics(context)


def _send_pipeline_alerts(**context):
    """Send end-of-pipeline alerts via configured channels."""
    import logging

    from data_pipeline.monitoring.alerting import AlertDispatcher, Severity

    log = logging.getLogger("airflow.task")
    ti = context.get("ti")
    dag_run = context.get("dag_run")

    # Pull the report summary from upstream task
    report_summary = ti.xcom_pull(task_ids="reporting.generate_pipeline_report") or {}

    dag_id = dag_run.dag_id if dag_run else "unknown"
    run_id = str(dag_run.run_id) if dag_run else "unknown"
    duration = report_summary.get("total_duration_sec", "N/A")

    message = (
        f"Pipeline *{dag_id}* run completed.\n"
        f"Run ID: {run_id}\n"
        f"Duration: {duration}s\n"
        f"Report: {report_summary.get('report_path', 'N/A')}"
    )

    dispatcher = AlertDispatcher()
    results = dispatcher.dispatch(
        message=message,
        severity=Severity.INFO,
        subject=f"Pipeline Summary: {dag_id}",
    )
    log.info("Alert dispatch results: %s", results)

    return {"alerts_sent": results, "status": "completed"}


# =============================================================================
# DAG definition
# =============================================================================

with DAG(
    dag_id="rewardsense_data_pipeline",
    default_args=default_args,
    description="Weekly pipeline: ingest → preprocess → version → report for credit card recommendation data",
    doc_md=DAG_DOC_MD,
    schedule="0 6 * * 0",  # Every Sunday at 06:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["rewardsense", "data-pipeline", "weekly"],
    on_success_callback=_on_dag_success,
) as dag:

    # ── Start sentinel ──────────────────────────────────────────────────
    pipeline_start = EmptyOperator(task_id="pipeline_start")

    # ── Task Group: Ingestion ───────────────────────────────────────────
    with TaskGroup(
        "ingestion", tooltip="Data acquisition from all sources"
    ) as ingestion_group:

        scrape_nerdwallet = PythonOperator(
            task_id="scrape_nerdwallet",
            python_callable=_scrape_nerdwallet,
            doc_md="Scrape credit card listings from NerdWallet.",
            execution_timeout=timedelta(minutes=30),
            on_failure_callback=_on_failure_callback,
        )

        scrape_issuers = PythonOperator(
            task_id="scrape_issuers",
            python_callable=_scrape_issuers,
            doc_md="Scrape credit card data from Chase, Amex, Citi, Capital One, and Discover.",
            execution_timeout=timedelta(minutes=45),
            on_failure_callback=_on_failure_callback,
        )

        fetch_api = PythonOperator(
            task_id="fetch_api_data",
            python_callable=_fetch_api_data,
            doc_md="Fetch normalized credit card offers from CreditCardBonuses API.",
            execution_timeout=timedelta(minutes=15),
            on_failure_callback=_on_failure_callback,
        )

        generate_synthetic = PythonOperator(
            task_id="generate_synthetic_data",
            python_callable=_generate_synthetic_data,
            doc_md="Generate synthetic user profiles and transaction histories.",
            execution_timeout=timedelta(minutes=20),
            on_failure_callback=_on_failure_callback,
        )

        merge_cards = PythonOperator(
            task_id="merge_card_data",
            python_callable=_merge_card_data,
            doc_md="Merge and deduplicate card data from scrapers and API, then commit stage → data/processed/current.",
            execution_timeout=timedelta(minutes=15),
            on_failure_callback=_on_failure_callback,
        )

        # Scraping and API run in parallel, then converge at merge
        [scrape_nerdwallet, scrape_issuers, fetch_api] >> merge_cards

        # Synthetic data generation runs in parallel (no merge dependency).
        # Note: The commit in merge_card_data will still include synthetic outputs,
        # because generate_synthetic writes into the same per-run stage dir.

    # ── Task Group: Preprocessing ───────────────────────────────────────
    with TaskGroup(
        "preprocessing",
        tooltip="Data cleaning, feature engineering, and transformation",
    ) as preprocessing_group:

        clean = PythonOperator(
            task_id="clean_data",
            python_callable=_clean_data,
            doc_md="Clean and validate credit card, transaction, and user profile data.",
        )

        features = PythonOperator(
            task_id="engineer_features",
            python_callable=_engineer_features,
            doc_md="Engineer ML features: reward rates, spending patterns, net card value.",
        )

        transform = PythonOperator(
            task_id="run_transform_pipeline",
            python_callable=_run_transform_pipeline,
            doc_md="Run end-to-end TransformationPipeline with checkpointing and audit.",
        )

        clean >> features >> transform

    # ── Task Group: Versioning ──────────────────────────────────────────
    with TaskGroup(
        "versioning", tooltip="Data versioning with DVC"
    ) as versioning_group:

        version_dvc = BashOperator(
            task_id="version_with_dvc",
            bash_command=(
                'echo "[PLACEHOLDER] DVC versioning — Story 5.4 will implement:" && '
                'echo "  dvc add data/processed/current/transformed/" && '
                'echo "  dvc push"'
            ),
            doc_md="Version processed data artifacts with DVC and push to remote.",
        )

    # ── Task Group: Reporting & Monitoring ──────────────────────────────
    with TaskGroup("reporting", tooltip="Pipeline report, metrics, and alerting") as reporting_group:

        report = PythonOperator(
            task_id="generate_pipeline_report",
            python_callable=_generate_pipeline_report,
            doc_md="Generate summary report with card counts, cleaning stats, and timing.",
        )

        metrics = PythonOperator(
            task_id="log_pipeline_metrics",
            python_callable=_log_pipeline_metrics,
            doc_md="Log pipeline metrics (durations, record counts, errors) to data/metrics/.",
        )

        alerts = PythonOperator(
            task_id="send_pipeline_alerts",
            python_callable=_send_pipeline_alerts,
            doc_md="Dispatch end-of-pipeline alerts to Slack and/or Email.",
        )

        # Report first, then metrics and alerts run in parallel
        report >> [metrics, alerts]

    # ── End sentinel ────────────────────────────────────────────────────
    pipeline_end = EmptyOperator(
        task_id="pipeline_end",
        trigger_rule="none_failed",
    )

    # ── Cross-group dependencies ────────────────────────────────────────
    # start → ingestion → preprocessing → versioning → reporting → end
    pipeline_start >> ingestion_group >> preprocessing_group
    preprocessing_group >> versioning_group >> reporting_group >> pipeline_end
