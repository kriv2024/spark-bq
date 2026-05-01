# PySpark BigQuery Project

A comprehensive PySpark project template for data processing with Google BigQuery integration. This project provides a production-ready structure for building ETL pipelines that process data between BigQuery and Spark environments.

## 🚀 Features

- **PySpark Integration**: Optimized Spark session management with BigQuery connector
- **BigQuery Connector**: Seamless reading from and writing to BigQuery tables
- **Data Validation**: Comprehensive data quality checks and validation utilities
- **Common Transformations**: Reusable transformation functions for data processing
- **Configuration Management**: Environment-based configuration with Pydantic
- **Structured Logging**: JSON-based logging with structlog
- **Testing Framework**: Unit tests with pytest and mocking capabilities
- **Code Quality**: Black formatting, isort, flake8 linting, and mypy type checking
- **VS Code Integration**: Tasks, debugging, and Jupyter notebook support

## 📁 Project Structure

```
├── .github/
│   └── copilot-instructions.md    # GitHub Copilot workspace instructions
├── .vscode/
│   └── tasks.json                 # VS Code tasks configuration
├── config/
│   ├── __init__.py
│   ├── logging.py                 # Logging configuration
│   └── settings.py                # Application settings and configuration
├── data/                          # Data directory (excluded from git)
├── notebooks/                     # Jupyter notebooks for exploration
├── scripts/
│   └── submit_job.py              # Job submission script
├── src/
│   ├── __init__.py
│   ├── bigquery_connector.py      # BigQuery integration utilities
│   ├── spark_session.py           # Spark session management
│   ├── pipelines/
│   │   ├── __init__.py
│   │   └── sample_pipeline.py     # Sample ETL pipeline
│   ├── transformations/
│   │   ├── __init__.py
│   │   └── common_transforms.py   # Common data transformations
│   └── utils/
│       ├── __init__.py
│       └── data_validation.py     # Data validation utilities
├── tests/
│   └── test_data_processing.py    # Unit tests
├── .env.example                   # Environment variables template
├── .gitignore                     # Git ignore rules
├── pyproject.toml                 # Project configuration and dependencies
└── README.md                      # This file
```

## 🔧 Installation

### Prerequisites

- Python 3.8 or higher
- Google Cloud Platform account with BigQuery access
- Service account key for BigQuery authentication

### Setup

1. **Clone or create the project structure** (already done in this workspace)

2. **Install dependencies:**
   ```bash
   pip install -e .
   ```
   
   Or install development dependencies:
   ```bash
   pip install -e .[dev,jupyter]
   ```

3. **Configure environment variables:**
   ```bash
   cp .env.example .env
   # Edit .env with your actual configuration values
   ```

4. **Set up Google Cloud credentials:**
   - Download your service account key JSON file
   - Update `GOOGLE_APPLICATION_CREDENTIALS` in `.env` file
   - Set your project ID and dataset name

## ⚙️ Configuration

### Environment Variables

Create a `.env` file based on `.env.example`:

```env
# Google Cloud Configuration
GOOGLE_APPLICATION_CREDENTIALS=path/to/your/service-account-key.json
GOOGLE_CLOUD_PROJECT_ID=your-project-id
BIGQUERY_DATASET=your_dataset_name

# Spark Configuration  
SPARK_APP_NAME=pyspark-bigquery-app
SPARK_MASTER=local[*]

# BigQuery Configuration
BQ_TEMP_GCS_BUCKET=your-temp-bucket
BQ_PARENT_PROJECT=your-billing-project
```

### Spark Configuration

The project includes optimized Spark configurations in [config/settings.py](config/settings.py):

- Adaptive query execution enabled
- Kryo serialization for better performance
- BigQuery connector with proper version management
- Memory and executor settings

## 🚀 Usage

### Running the Sample Pipeline

```bash
# Using VS Code tasks (Ctrl+Shift+P -> "Tasks: Run Task")
# Select "Run Sample Pipeline (Local)"

# Or run directly:
python src/pipelines/sample_pipeline.py
```

### Using the BigQuery Connector

```python
from src.spark_session import get_spark_session
from src.bigquery_connector import BigQueryConnector

# Get Spark session
spark = get_spark_session("my-app")

# Initialize BigQuery connector
bq = BigQueryConnector(spark)

# Read from BigQuery
df = bq.read_table("dataset.table_name")

# Write to BigQuery
bq.write_table(df, "dataset.output_table", mode="overwrite")
```

### Data Transformations

```python
from src.transformations.common_transforms import (
    standardize_column_names,
    add_audit_columns,
    create_date_dimensions
)

# Standardize column names
df = standardize_column_names(df, "snake_case")

# Add audit columns
df = add_audit_columns(df)

# Create date dimensions
df = create_date_dimensions(df, "created_date")
```

### Data Validation

```python
from src.utils.data_validation import (
    validate_required_columns,
    check_data_quality,
    handle_null_values
)

# Validate required columns
validate_required_columns(df, ["id", "name", "email"])

# Check data quality
quality_report = check_data_quality(df)

# Handle null values
df = handle_null_values(df, strategy="drop")
```

## 🧪 Testing

Run tests using VS Code tasks or command line:

```bash
# Run all tests
python -m pytest tests/ -v

# Run with coverage
python -m pytest tests/ --cov=src --cov-report=term-missing

# Run specific test file
python -m pytest tests/test_data_processing.py -v
```

## 🔍 Code Quality

The project includes several code quality tools:

```bash
# Format code with Black
python -m black src/ tests/ scripts/

# Sort imports with isort
python -m isort src/ tests/ scripts/

# Lint with flake8
python -m flake8 src/ tests/ scripts/

# Type checking with mypy
python -m mypy src/
```

## 📊 Development with Jupyter

Start Jupyter Lab for interactive development:

```bash
# Using VS Code task
# Ctrl+Shift+P -> "Tasks: Run Task" -> "Start Jupyter Lab"

# Or directly:
python -m jupyter lab --notebook-dir=notebooks
```

## 🔐 Security Best Practices

1. **Never commit credentials**: Use environment variables and `.env` files
2. **Parameterized queries**: Always use parameterized queries for BigQuery
3. **Least privilege**: Configure service accounts with minimal required permissions
4. **Audit logging**: Enable audit logging for data access and processing

## 🐛 Troubleshooting

### Common Issues

1. **Import errors**: Ensure PySpark is properly installed and SPARK_HOME is set if needed
2. **BigQuery authentication**: Verify service account key path and permissions
3. **Memory issues**: Adjust Spark memory settings in `config/settings.py`
4. **Network connectivity**: Configure proxy settings if behind corporate firewall

### Debugging

1. Enable debug logging in `.env`:
   ```env
   LOG_LEVEL=DEBUG
   DEBUG=true
   ```

2. Use VS Code debugger with breakpoints in Python files

3. Check Spark UI at http://localhost:4040 during job execution

## 📚 Additional Resources

- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [BigQuery Python Client](https://googleapis.dev/python/bigquery/latest/index.html)
- [Spark BigQuery Connector](https://github.com/GoogleCloudDataproc/spark-bigquery-connector)

## 🤝 Contributing

1. Follow the established code style (Black, isort, flake8)
2. Add tests for new functionality
3. Update documentation as needed
4. Use meaningful commit messages

## 📄 License

This project is licensed under the MIT License. See LICENSE file for details.

---

**Happy Data Processing! 🚀**