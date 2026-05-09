"""
BigQuery integration utilities for PySpark.
"""

from typing import Any, Dict, List, Optional

from google.cloud import bigquery
from pyspark.sql import DataFrame, SparkSession

from config.logging import get_logger
from config.settings import get_bigquery_config

logger = get_logger(__name__)


class BigQueryConnector:
    """Handles BigQuery operations with PySpark integration."""

    def __init__(self, spark: SparkSession):
        """
        Initialize BigQuery connector.

        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        self.config = get_bigquery_config()
        self._client: Optional[bigquery.Client] = None

    @property
    def client(self) -> bigquery.Client:
        """Get BigQuery client instance."""
        if self._client is None:
            if self.config.credentials_path:
                self._client = bigquery.Client.from_service_account_json(
                    self.config.credentials_path, project=self.config.project_id
                )
            else:
                self._client = bigquery.Client(project=self.config.project_id)
        return self._client

    def read_table(
        self,
        table_id: str,
        columns: Optional[List[str]] = None,
        filter_condition: Optional[str] = None,
        **options: Any,
    ) -> DataFrame:
        """
        Read data from BigQuery table into Spark DataFrame.

        Args:
            table_id: BigQuery table ID (dataset.table or project.dataset.table)
            columns: Optional list of columns to select
            filter_condition: Optional WHERE clause filter
            **options: Additional BigQuery connector options

        Returns:
            Spark DataFrame containing the table data
        """
        # Format table reference
        if "." not in table_id:
            table_ref = f"{self.config.project_id}.{self.config.dataset}.{table_id}"
        elif table_id.count(".") == 1:
            table_ref = f"{self.config.project_id}.{table_id}"
        else:
            table_ref = table_id

        # Build read options
        read_options = {
            "table": table_ref,
            "parentProject": self.config.parent_project or self.config.project_id,
        }

        # Add optional parameters
        if self.config.temp_gcs_bucket:
            read_options["temporaryGcsBucket"] = self.config.temp_gcs_bucket

        read_options.update(options)

        # Read from BigQuery
        df = self.spark.read.format("bigquery").options(**read_options).load()

        # Apply column selection
        if columns:
            df = df.select(*columns)

        # Apply filter condition
        if filter_condition:
            df = df.filter(filter_condition)

        logger.info(
            "Read table from BigQuery",
            table=table_ref,
            columns=columns,
            filter=filter_condition,
        )

        return df

    def write_table(
        self,
        df: DataFrame,
        table_id: str,
        mode: str = "overwrite",
        partition_by: Optional[str] = None,
        cluster_by: Optional[List[str]] = None,
        **options: Any,
    ) -> None:
        """
        Write Spark DataFrame to BigQuery table.

        Args:
            df: Spark DataFrame to write
            table_id: Target BigQuery table ID
            mode: Write mode ('overwrite', 'append', 'error', 'ignore')
            partition_by: Column name for table partitioning
            cluster_by: List of column names for table clustering
            **options: Additional BigQuery connector options
        """
        # Format table reference
        if "." not in table_id:
            table_ref = f"{self.config.project_id}.{self.config.dataset}.{table_id}"
        elif table_id.count(".") == 1:
            table_ref = f"{self.config.project_id}.{table_id}"
        else:
            table_ref = table_id

        # Build write options
        write_options = {
            "table": table_ref,
            "parentProject": self.config.parent_project or self.config.project_id,
        }

        # Add optional parameters
        if self.config.temp_gcs_bucket:
            write_options["temporaryGcsBucket"] = self.config.temp_gcs_bucket

        if partition_by:
            write_options["partitionField"] = partition_by
            write_options["partitionType"] = "DAY"  # Default to daily partitioning

        if cluster_by:
            write_options["clusteredFields"] = ",".join(cluster_by)

        write_options.update(options)

        # Write to BigQuery
        (df.write.format("bigquery").mode(mode).options(**write_options).save())

        logger.info(
            "Wrote table to BigQuery", table=table_ref, mode=mode, rows=df.count()
        )

    def execute_query(self, query: str, **options: Any) -> DataFrame:
        """
        Execute a BigQuery SQL query and return results as Spark DataFrame.

        Args:
            query: SQL query to execute
            **options: Additional BigQuery connector options

        Returns:
            Spark DataFrame containing query results
        """
        # Build query options
        query_options = {
            "query": query,
            "parentProject": self.config.parent_project or self.config.project_id,
        }

        if self.config.temp_gcs_bucket:
            query_options["temporaryGcsBucket"] = self.config.temp_gcs_bucket

        query_options.update(options)

        # Execute query
        df = self.spark.read.format("bigquery").options(**query_options).load()

        logger.info("Executed BigQuery query", query_length=len(query))

        return df

    def get_table_schema(self, table_id: str) -> Dict[str, Any]:
        """
        Get BigQuery table schema information.

        Args:
            table_id: BigQuery table ID

        Returns:
            Dictionary containing table schema information
        """
        # Format table reference
        if "." not in table_id:
            table_ref = f"{self.config.project_id}.{self.config.dataset}.{table_id}"
        elif table_id.count(".") == 1:
            table_ref = f"{self.config.project_id}.{table_id}"
        else:
            table_ref = table_ref

        # Get table information
        table = self.client.get_table(table_ref)

        schema_info = {
            "table_id": table.table_id,
            "dataset_id": table.dataset_id,
            "project_id": table.project,
            "num_rows": table.num_rows,
            "num_bytes": table.num_bytes,
            "created": table.created,
            "modified": table.modified,
            "schema": [
                {
                    "name": field.name,
                    "field_type": field.field_type,
                    "mode": field.mode,
                    "description": field.description,
                }
                for field in table.schema
            ],
        }

        logger.info(
            "Retrieved table schema",
            table=table_ref,
            num_columns=len(schema_info["schema"]),
        )

        return schema_info
