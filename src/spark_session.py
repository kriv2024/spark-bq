"""
Spark session management utilities.
"""

from typing import Any, Dict, Optional

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from config.logging import get_logger
from config.settings import get_bigquery_config, get_spark_config

logger = get_logger(__name__)


class SparkSessionManager:
    """Manages Spark session lifecycle and configuration."""

    _instance: Optional[SparkSession] = None

    @classmethod
    def get_session(
        cls,
        app_name: Optional[str] = None,
        config_overrides: Optional[Dict[str, Any]] = None,
    ) -> SparkSession:
        """
        Get or create a Spark session with BigQuery connector.

        Args:
            app_name: Optional custom application name
            config_overrides: Optional configuration overrides

        Returns:
            Configured SparkSession instance
        """
        if cls._instance is None:
            cls._instance = cls._create_session(app_name, config_overrides)

        return cls._instance

    @classmethod
    def _create_session(
        cls,
        app_name: Optional[str] = None,
        config_overrides: Optional[Dict[str, Any]] = None,
    ) -> SparkSession:
        """Create a new Spark session with optimized configuration."""

        # Get configurations
        spark_config = get_spark_config()
        bq_config = get_bigquery_config()

        # Override app name if provided
        if app_name:
            spark_config["spark.app.name"] = app_name

        # Apply any configuration overrides
        if config_overrides:
            spark_config.update(config_overrides)

        # Create Spark configuration
        conf = SparkConf()
        for key, value in spark_config.items():
            conf.set(key, value)

        # Set BigQuery specific configurations
        if bq_config.credentials_path:
            conf.set(
                "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                bq_config.credentials_path,
            )

        if bq_config.temp_gcs_bucket:
            conf.set("spark.conf.temporaryGcsBucket", bq_config.temp_gcs_bucket)

        # Create Spark session
        spark = SparkSession.builder.config(conf=conf).getOrCreate()

        # Set BigQuery configurations in Spark context
        spark.conf.set("viewsEnabled", "true")
        spark.conf.set("materializationDataset", bq_config.dataset)
        spark.conf.set("materializationProject", bq_config.project_id)

        if bq_config.parent_project:
            spark.conf.set("parentProject", bq_config.parent_project)

        logger.info(
            "Spark session created",
            app_name=spark.conf.get("spark.app.name"),
            master=spark.conf.get("spark.master"),
        )

        return spark

    @classmethod
    def stop_session(cls) -> None:
        """Stop the current Spark session."""
        if cls._instance is not None:
            cls._instance.stop()
            cls._instance = None
            logger.info("Spark session stopped")

    @classmethod
    def restart_session(
        cls,
        app_name: Optional[str] = None,
        config_overrides: Optional[Dict[str, Any]] = None,
    ) -> SparkSession:
        """Restart the Spark session with new configuration."""
        cls.stop_session()
        return cls.get_session(app_name, config_overrides)


def get_spark_session(
    app_name: Optional[str] = None, config_overrides: Optional[Dict[str, Any]] = None
) -> SparkSession:
    """
    Convenience function to get a Spark session.

    Args:
        app_name: Optional custom application name
        config_overrides: Optional configuration overrides

    Returns:
        Configured SparkSession instance
    """
    return SparkSessionManager.get_session(app_name, config_overrides)
