<!-- Use this file to provide workspace-specific custom instructions to Copilot. For more details, visit https://code.visualstudio.com/docs/copilot/copilot-customization#_use-a-githubcopilotinstructionsmd-file -->

## PySpark + BigQuery Project Instructions

This project uses PySpark for distributed data processing with BigQuery as the data warehouse. 

### Key Technologies:
- **PySpark**: For distributed data processing and analytics
- **Google Cloud BigQuery**: For data storage and querying
- **Python**: Primary programming language
- **Jupyter Notebooks**: For interactive development and analysis

### Project Structure Guidelines:
- Keep Spark configuration in `config/` directory
- Store reusable transformations in `src/transformations/`
- Place utility functions in `src/utils/`
- Store notebooks in `notebooks/` for exploration
- Keep tests in `tests/` directory

### Coding Standards:
- Use type hints for all function parameters and returns
- Follow PEP 8 for Python code style
- Write docstrings for all functions and classes
- Use meaningful variable names that reflect BigQuery table/column names
- Handle Spark DataFrame operations with proper error handling
- Use configuration files for BigQuery project IDs and dataset names

### BigQuery Best Practices:
- Always use parameterized queries to prevent injection
- Partition and cluster tables appropriately
- Use appropriate data types for cost optimization
- Cache intermediate results when possible
- Monitor query costs and performance

### Spark Best Practices:
- Use appropriate partitioning strategies
- Cache DataFrames when used multiple times
- Use broadcast joins for small lookup tables
- Handle null values explicitly
- Use appropriate serialization formats (Parquet, Avro)