"""
Spark and BigQuery configuration settings.
"""
import os
from typing import Dict, Any, Optional
from pydantic_settings import BaseSettings
from pydantic import validator
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class SparkConfig(BaseSettings):
    """Spark configuration settings."""
    
    app_name: str = "pyspark-bigquery-app"
    master: str = "local[*]"
    sql_warehouse_dir: str = "./spark-warehouse"
    serializer: str = "org.apache.spark.serializer.KryoSerializer"
    
    # Memory settings
    driver_memory: str = "2g"
    executor_memory: str = "2g"
    executor_cores: str = "2"
    
    # BigQuery connector settings
    bigquery_connector_version: str = "0.32.2"
    
    class Config:
        env_prefix = "SPARK_"


class BigQueryConfig(BaseSettings):
    """BigQuery configuration settings."""
    
    project_id: str
    dataset: str
    temp_gcs_bucket: Optional[str] = None
    credentials_path: Optional[str] = None
    parent_project: Optional[str] = None
    
    @validator('project_id')
    def project_id_must_not_be_empty(cls, v):
        if not v:
            raise ValueError('GOOGLE_CLOUD_PROJECT_ID environment variable must be set')
        return v
    
    @validator('dataset')
    def dataset_must_not_be_empty(cls, v):
        if not v:
            raise ValueError('BIGQUERY_DATASET environment variable must be set')
        return v
    
    class Config:
        env_prefix = "BQ_"


class AppConfig(BaseSettings):
    """Application configuration settings."""
    
    environment: str = "development"
    debug: bool = False
    log_level: str = "INFO"
    log_format: str = "json"
    
    class Config:
        env_prefix = ""


def get_spark_config() -> Dict[str, Any]:
    """Get Spark configuration as dictionary."""
    config = SparkConfig()
    
    spark_conf = {
        "spark.app.name": config.app_name,
        "spark.master": config.master,
        "spark.sql.warehouse.dir": config.sql_warehouse_dir,
        "spark.serializer": config.serializer,
        "spark.driver.memory": config.driver_memory,
        "spark.executor.memory": config.executor_memory,
        "spark.executor.cores": config.executor_cores,
        
        # BigQuery connector
        "spark.jars.packages": f"com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:{config.bigquery_connector_version}",
        
        # Additional optimizations
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
    }
    
    return spark_conf


def get_bigquery_config() -> BigQueryConfig:
    """Get BigQuery configuration."""
    return BigQueryConfig(
        project_id=os.getenv("GOOGLE_CLOUD_PROJECT_ID", ""),
        dataset=os.getenv("BIGQUERY_DATASET", ""),
        temp_gcs_bucket=os.getenv("BQ_TEMP_GCS_BUCKET"),
        credentials_path=os.getenv("BQ_CREDENTIALS_PATH"),
        parent_project=os.getenv("BQ_PARENT_PROJECT")
    )


def get_app_config() -> AppConfig:
    """Get application configuration."""
    return AppConfig()