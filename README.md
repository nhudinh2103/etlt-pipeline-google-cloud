# GitHub API ETL Pipeline

An Apache Airflow ETL pipeline that extracts commit data from the Apache Airflow GitHub repository and loads it into BigQuery using a medallion architecture.

## Architecture

- **Bronze Layer**: Raw data from GitHub API stored as Parquet files in GCS
- **Staging Layer**: Transformed data using DuckDB, stored as Parquet files in GCS
- **Gold Layer**: Final data stored in BigQuery with daily partitioning

## Data Flow

1. **Extract (Bronze)**: 
   - Fetches commit data from GitHub API
   - Stores raw data in GCS as Parquet files
   - Path pattern: `gs://bucket/bronze/github_commits/dt=YYYY-MM-DD/commits.parquet`

2. **Transform (Staging)**:
   - Uses DuckDB for robust data processing
   - Reads bronze data
   - Applies SQL transformations:
     - Strips whitespace from text fields
     - Normalizes email addresses to lowercase
     - Converts timestamps to UTC
   - Stores transformed data in GCS
   - Path pattern: `gs://bucket/staging/github_commits/dt=YYYY-MM-DD/commits_transformed.parquet`

3. **Load (Gold)**:
   - Loads transformed data into BigQuery
   - Creates daily partitioned tables
   - Table: `project.dataset.commits$YYYYMMDD`

## Schedule

- Runs daily at midnight UTC
- Processes data for the previous day
- Idempotent execution

## Setup

1. Set up Cloud Composer environment
2. Configure Airflow variables:
   ```
   github_token: Your GitHub API token
   ```

3. Update `pipeline_config.py` with your:
   - GCS bucket
   - BigQuery project and dataset
   - Other configurations

## CI/CD

The pipeline uses Cloud Build for continuous deployment:

1. On commit to main branch:
   - Runs tests
   - If tests pass, syncs DAGs and plugins to Cloud Composer

2. Configure Cloud Build trigger with these substitutions:
   - `_COMPOSER_ENV_NAME`: Your Composer environment name
   - `_COMPOSER_LOCATION`: Your Composer environment location

## Development

1. Clone the repository
2. Install development dependencies:
   ```bash
   pip install pytest apache-airflow[google]==2.10.2 duckdb
   ```

3. Run tests:
   ```bash
   python -m pytest tests/
   ```

## Project Structure

```
github_api_etl/
├── dags/
│   ├── github_commits_etl.py    # Main DAG file
│   ├── sql/
│   │   └── transform_commits.sql # DuckDB transformations
│   └── config/
│       └── pipeline_config.py   # Configuration
├── plugins/
│   └── operators/
│       ├── github_to_gcs.py     # Bronze layer operator
│       └── duckdb_transform.py  # DuckDB transformation operator
├── tests/
│   └── test_operators.py        # Unit tests
└── cloudbuild.yaml              # CI/CD configuration
```

## Why DuckDB?

DuckDB was chosen for data transformation because:
1. **Robust Processing**: In-process analytical database with efficient memory management
2. **Native Parquet Support**: Efficient reading and writing of Parquet files
3. **SQL-Based Transformations**: Simple, readable transformations using SQL
4. **Resource Management**: Better memory handling than pandas
5. **Performance**: Optimized for analytical queries with parallel execution
6. **Simplicity**: No cluster setup required, runs in the Airflow worker process
