"""
Common data transformation functions for PySpark.
"""
from typing import List, Dict, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, when, coalesce, trim, lower, upper, regexp_replace,
    to_date, to_timestamp, year, month, dayofmonth, date_format,
    sum as spark_sum, count as spark_count, avg, max as spark_max, min as spark_min
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from config.logging import get_logger

logger = get_logger(__name__)


def standardize_column_names(df: DataFrame, naming_convention: str = "snake_case") -> DataFrame:
    """
    Standardize column names according to specified convention.
    
    Args:
        df: Input DataFrame
        naming_convention: Convention to use ('snake_case', 'camelCase', 'lower')
        
    Returns:
        DataFrame with standardized column names
    """
    if naming_convention == "snake_case":
        # Convert to snake_case
        new_columns = []
        for col_name in df.columns:
            # Replace spaces and special chars with underscores, convert to lowercase
            new_name = regexp_replace(lit(col_name), r"[^a-zA-Z0-9]", "_").alias("temp").collect()[0][0]
            new_name = new_name.lower().strip("_")
            # Remove consecutive underscores
            while "__" in new_name:
                new_name = new_name.replace("__", "_")
            new_columns.append(new_name)
    
    elif naming_convention == "lower":
        new_columns = [col_name.lower().replace(" ", "_") for col_name in df.columns]
    
    else:
        raise ValueError(f"Unsupported naming convention: {naming_convention}")
    
    # Rename columns
    for old_name, new_name in zip(df.columns, new_columns):
        df = df.withColumnRenamed(old_name, new_name)
    
    logger.info("Column names standardized", 
               convention=naming_convention,
               num_columns=len(new_columns))
    
    return df


def add_audit_columns(df: DataFrame, load_timestamp: Optional[str] = None) -> DataFrame:
    """
    Add audit columns to DataFrame for tracking data lineage.
    
    Args:
        df: Input DataFrame
        load_timestamp: Optional custom timestamp (defaults to current_timestamp)
        
    Returns:
        DataFrame with audit columns added
    """
    from pyspark.sql.functions import current_timestamp, lit
    
    if load_timestamp:
        timestamp_col = lit(load_timestamp).cast(TimestampType())
    else:
        timestamp_col = current_timestamp()
    
    result_df = (df
                .withColumn("load_timestamp", timestamp_col)
                .withColumn("source_system", lit("pyspark_etl"))
                .withColumn("load_id", lit("batch_load")))  # Could be replaced with actual batch ID
    
    logger.info("Audit columns added")
    
    return result_df


def create_date_dimensions(df: DataFrame, date_column: str) -> DataFrame:
    """
    Create date dimension columns from a date column.
    
    Args:
        df: Input DataFrame
        date_column: Name of the date column to decompose
        
    Returns:
        DataFrame with additional date dimension columns
    """
    result_df = (df
                .withColumn(f"{date_column}_year", year(col(date_column)))
                .withColumn(f"{date_column}_month", month(col(date_column)))
                .withColumn(f"{date_column}_day", dayofmonth(col(date_column)))
                .withColumn(f"{date_column}_formatted", date_format(col(date_column), "yyyy-MM-dd")))
    
    logger.info("Date dimensions created", date_column=date_column)
    
    return result_df


def apply_business_rules(df: DataFrame, rules: Dict[str, Any]) -> DataFrame:
    """
    Apply business rules to DataFrame based on configuration.
    
    Args:
        df: Input DataFrame
        rules: Dictionary containing business rule definitions
        
    Returns:
        DataFrame with business rules applied
    """
    result_df = df
    
    # Example business rules
    for rule_name, rule_config in rules.items():
        if rule_config["type"] == "filter":
            condition = rule_config["condition"]
            result_df = result_df.filter(condition)
            logger.info("Applied filter rule", rule=rule_name, condition=condition)
        
        elif rule_config["type"] == "derive_column":
            column_name = rule_config["column"]
            expression = rule_config["expression"]
            result_df = result_df.withColumn(column_name, expression)
            logger.info("Applied column derivation rule", rule=rule_name, column=column_name)
        
        elif rule_config["type"] == "categorize":
            column_name = rule_config["column"]
            categories = rule_config["categories"]
            when_expr = None
            for category, condition in categories.items():
                if when_expr is None:
                    when_expr = when(condition, category)
                else:
                    when_expr = when_expr.when(condition, category)
            
            result_df = result_df.withColumn(f"{column_name}_category", 
                                           when_expr.otherwise("Other"))
            logger.info("Applied categorization rule", rule=rule_name, column=column_name)
    
    return result_df


def aggregate_metrics(
    df: DataFrame,
    group_by_columns: List[str],
    metrics: Dict[str, str],
    alias_prefix: str = ""
) -> DataFrame:
    """
    Calculate aggregation metrics grouped by specified columns.
    
    Args:
        df: Input DataFrame
        group_by_columns: List of columns to group by
        metrics: Dictionary mapping column names to aggregation functions
        alias_prefix: Optional prefix for result column names
        
    Returns:
        DataFrame with calculated metrics
    """
    # Build aggregation expressions
    agg_exprs = []
    
    for column, agg_func in metrics.items():
        alias_name = f"{alias_prefix}{column}_{agg_func}" if alias_prefix else f"{column}_{agg_func}"
        
        if agg_func == "sum":
            agg_exprs.append(spark_sum(col(column)).alias(alias_name))
        elif agg_func == "count":
            agg_exprs.append(spark_count(col(column)).alias(alias_name))
        elif agg_func == "avg":
            agg_exprs.append(avg(col(column)).alias(alias_name))
        elif agg_func == "max":
            agg_exprs.append(spark_max(col(column)).alias(alias_name))
        elif agg_func == "min":
            agg_exprs.append(spark_min(col(column)).alias(alias_name))
        else:
            raise ValueError(f"Unsupported aggregation function: {agg_func}")
    
    # Perform aggregation
    result_df = df.groupBy(*group_by_columns).agg(*agg_exprs)
    
    logger.info("Metrics aggregated", 
               group_by_columns=group_by_columns,
               metrics_calculated=len(metrics))
    
    return result_df