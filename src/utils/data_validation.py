"""
Common data validation utilities for PySpark DataFrames.
"""
from typing import List, Dict, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, isnan, isnull, when
from config.logging import get_logger

logger = get_logger(__name__)


def validate_required_columns(df: DataFrame, required_columns: List[str]) -> None:
    """
    Validate that all required columns exist in the DataFrame.
    
    Args:
        df: Input DataFrame
        required_columns: List of required column names
        
    Raises:
        ValueError: If any required columns are missing
    """
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    logger.info("Column validation passed", required=len(required_columns))


def check_data_quality(df: DataFrame, columns: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Perform basic data quality checks on DataFrame columns.
    
    Args:
        df: Input DataFrame
        columns: Optional list of columns to check (defaults to all columns)
        
    Returns:
        Dictionary containing data quality metrics
    """
    if columns is None:
        columns = df.columns
    
    total_rows = df.count()
    quality_report = {
        "total_rows": total_rows,
        "columns": {}
    }
    
    for column in columns:
        # Count null and NaN values
        null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
        
        # Calculate distinct values count
        distinct_count = df.select(column).distinct().count()
        
        quality_report["columns"][column] = {
            "null_count": null_count,
            "null_percentage": (null_count / total_rows) * 100 if total_rows > 0 else 0,
            "distinct_count": distinct_count,
            "completeness": ((total_rows - null_count) / total_rows) * 100 if total_rows > 0 else 0
        }
    
    logger.info("Data quality check completed", 
               total_rows=total_rows, 
               columns_checked=len(columns))
    
    return quality_report


def remove_duplicates(
    df: DataFrame, 
    subset: Optional[List[str]] = None,
    keep: str = "first"
) -> DataFrame:
    """
    Remove duplicate rows from DataFrame.
    
    Args:
        df: Input DataFrame
        subset: Optional list of columns to consider for duplicate detection
        keep: Which duplicate to keep ('first' or 'last')
        
    Returns:
        DataFrame with duplicates removed
    """
    initial_count = df.count()
    
    if subset:
        deduplicated_df = df.dropDuplicates(subset)
    else:
        deduplicated_df = df.dropDuplicates()
    
    final_count = deduplicated_df.count()
    duplicates_removed = initial_count - final_count
    
    logger.info("Duplicates removed", 
               initial_rows=initial_count,
               final_rows=final_count,
               duplicates_removed=duplicates_removed)
    
    return deduplicated_df


def handle_null_values(
    df: DataFrame,
    strategy: str = "drop",
    fill_values: Optional[Dict[str, Any]] = None,
    subset: Optional[List[str]] = None
) -> DataFrame:
    """
    Handle null values in DataFrame using specified strategy.
    
    Args:
        df: Input DataFrame
        strategy: Strategy to handle nulls ('drop', 'fill', 'forward_fill')
        fill_values: Dictionary of column -> fill value mappings
        subset: Optional list of columns to consider
        
    Returns:
        DataFrame with null values handled
    """
    if strategy == "drop":
        result_df = df.dropna(subset=subset)
    elif strategy == "fill":
        if fill_values:
            result_df = df.fillna(fill_values, subset=subset)
        else:
            # Fill with default values based on data type
            result_df = df.fillna(0, subset=subset)  # This is simplified
    else:
        raise ValueError(f"Unsupported null handling strategy: {strategy}")
    
    logger.info("Null values handled", 
               strategy=strategy,
               subset=subset or "all_columns")
    
    return result_df