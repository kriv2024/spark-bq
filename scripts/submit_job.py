"""
Job submission script for PySpark BigQuery pipelines.
"""

import sys
from pathlib import Path

import click

# Add src directory to Python path
sys.path.append(str(Path(__file__).parent.parent))

from config.logging import get_logger, setup_logging  # noqa: E402
from src.pipelines.sample_pipeline import SamplePipeline  # noqa: E402

# Setup logging
setup_logging()
logger = get_logger(__name__)


@click.command()
@click.option("--source-table", required=True, help="Source BigQuery table")
@click.option("--target-table", required=True, help="Target BigQuery table")
@click.option("--filter-condition", help="Optional WHERE clause filter")
@click.option(
    "--mode",
    default="overwrite",
    type=click.Choice(["overwrite", "append", "error", "ignore"]),
    help="Write mode for target table",
)
@click.option("--partition-by", help="Optional partition column")
@click.option(
    "--app-name", default="pyspark-bigquery-job", help="Spark application name"
)
def submit_job(
    source_table, target_table, filter_condition, mode, partition_by, app_name
):
    """Submit a PySpark BigQuery ETL job."""
    try:
        logger.info(
            "Job submission started",
            source=source_table,
            target=target_table,
            app_name=app_name,
        )

        # Initialize and run pipeline
        pipeline = SamplePipeline(app_name)
        pipeline.run_pipeline(
            source_table=source_table,
            target_table=target_table,
            filter_condition=filter_condition,
            mode=mode,
            partition_by=partition_by,
        )

        logger.info("Job completed successfully")

    except Exception as e:
        logger.error("Job failed", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    submit_job()
