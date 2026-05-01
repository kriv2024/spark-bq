"""
Sample data pipeline for demonstrating PySpark with BigQuery integration.
"""
from typing import Optional
from pyspark.sql import DataFrame
from src.spark_session import get_spark_session
from src.bigquery_connector import BigQueryConnector
from src.transformations.common_transforms import (
    standardize_column_names,
    add_audit_columns,
    create_date_dimensions
)
from src.utils.data_validation import validate_required_columns, check_data_quality
from config.logging import get_logger

logger = get_logger(__name__)


class SamplePipeline:
    """Sample data pipeline demonstrating common ETL patterns."""
    
    def __init__(self, app_name: str = "sample-pipeline"):
        """Initialize the pipeline with Spark session and BigQuery connector."""
        self.spark = get_spark_session(app_name)
        self.bq_connector = BigQueryConnector(self.spark)
    
    def extract_data(self, source_table: str, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract data from BigQuery source table.
        
        Args:
            source_table: Source table identifier
            filter_condition: Optional WHERE clause filter
            
        Returns:
            DataFrame containing extracted data
        """
        logger.info("Starting data extraction", source_table=source_table)
        
        df = self.bq_connector.read_table(
            table_id=source_table,
            filter_condition=filter_condition
        )
        
        logger.info("Data extraction completed", 
                   rows_extracted=df.count(),
                   source_table=source_table)
        
        return df
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply transformations to the extracted data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        logger.info("Starting data transformation")
        
        # Validate required columns (example)
        required_columns = ["id"]  # Adjust based on your data
        try:
            validate_required_columns(df, required_columns)
        except ValueError as e:
            logger.error("Column validation failed", error=str(e))
            raise
        
        # Check data quality
        quality_report = check_data_quality(df)
        logger.info("Data quality check completed", 
                   total_rows=quality_report["total_rows"])
        
        # Apply transformations
        transformed_df = df
        
        # Standardize column names
        transformed_df = standardize_column_names(transformed_df, "snake_case")
        
        # Add audit columns
        transformed_df = add_audit_columns(transformed_df)
        
        # Add date dimensions if date columns exist
        date_columns = [col for col in transformed_df.columns 
                       if "date" in col.lower() or "timestamp" in col.lower()]
        for date_col in date_columns[:1]:  # Process first date column found
            try:
                transformed_df = create_date_dimensions(transformed_df, date_col)
            except Exception as e:
                logger.warning("Failed to create date dimensions", 
                             column=date_col, error=str(e))
        
        logger.info("Data transformation completed", 
                   final_columns=len(transformed_df.columns))
        
        return transformed_df
    
    def load_data(
        self, 
        df: DataFrame, 
        target_table: str, 
        mode: str = "overwrite",
        partition_by: Optional[str] = None
    ) -> None:
        """
        Load transformed data to BigQuery target table.
        
        Args:
            df: Transformed DataFrame
            target_table: Target table identifier
            mode: Write mode ('overwrite', 'append', etc.)
            partition_by: Optional partition column
        """
        logger.info("Starting data load", 
                   target_table=target_table, 
                   mode=mode)
        
        self.bq_connector.write_table(
            df=df,
            table_id=target_table,
            mode=mode,
            partition_by=partition_by
        )
        
        logger.info("Data load completed", target_table=target_table)
    
    def run_pipeline(
        self,
        source_table: str,
        target_table: str,
        filter_condition: Optional[str] = None,
        mode: str = "overwrite",
        partition_by: Optional[str] = None
    ) -> None:
        """
        Run the complete ETL pipeline.
        
        Args:
            source_table: Source table identifier
            target_table: Target table identifier
            filter_condition: Optional WHERE clause filter
            mode: Write mode for target table
            partition_by: Optional partition column
        """
        try:
            logger.info("Pipeline started", 
                       source=source_table, 
                       target=target_table)
            
            # Extract
            raw_df = self.extract_data(source_table, filter_condition)
            
            # Transform
            transformed_df = self.transform_data(raw_df)
            
            # Load
            self.load_data(transformed_df, target_table, mode, partition_by)
            
            logger.info("Pipeline completed successfully")
            
        except Exception as e:
            logger.error("Pipeline failed", error=str(e))
            raise
        finally:
            # Optional: Clean up Spark session
            # self.spark.stop()
            pass


def main():
    """Example usage of the sample pipeline."""
    pipeline = SamplePipeline("sample-etl-pipeline")
    
    # Example pipeline execution
    pipeline.run_pipeline(
        source_table="raw_data.sample_table",
        target_table="processed_data.sample_table_transformed",
        filter_condition="created_date >= '2024-01-01'",
        mode="overwrite",
        partition_by="load_timestamp"
    )


if __name__ == "__main__":
    main()