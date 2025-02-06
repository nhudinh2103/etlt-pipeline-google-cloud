# GitHub API ETL Pipeline

An Apache Airflow ETL pipeline that extracts commit data from the Apache Airflow GitHub repository and loads it into BigQuery using a medallion architecture.

## Medallion Architecture

The project follows the medallion architecture pattern, which organizes data into different layers of refinement:

- **Bronze Layer**: Raw data from GitHub API stored as JSON files in GCS
- **Staging Layer**: Transformed data stored as JSON files in GCS
- **Gold Layer**: Final data stored in BigQuery with daily partitioning in Parquet format

## Architecture

### High Level Overview

#### Pipeline Architecture
![Pipeline Architecture](images/pipeline-architecture.png)

The pipeline architecture illustrates the data flow within Google Cloud Platform (GCP):
1. Extract raw data by calling GitHub API
2. Store bronze, staging, and gold data in Google Cloud Storage (GCS)
3. Load data to BigQuery using BigQueryInsertJob for analysis

#### Deployment Architecture
![Deployment Architecture](images/deployment-architecture.png)

The deployment process follows a CI/CD approach:
1. Data engineer commits code to GitHub repository
2. GitHub Actions triggers CI/CD job on new commits
3. On successful tests, code is deployed to GCS bucket
4. Cloud Composer (Airflow) automatically syncs code from GCS bucket

### ETL Pipeline Components
![ETL Pipeline](images/etl-pipeline.png)

The Airflow DAG consists of several tasks:
- `init_table`: Initialize required tables
- `extract_github_raw_data_to_gcs`: Extract data from GitHub API
- `transform_gcs_raw_to_staging_data`: Transform raw data
- `convert_json_to_parquet_gcs_data`: Convert JSON to Parquet format
- `update_staging_commits_table`: Update staging tables
- `update_d_date`: Update date dimension base on new staging data
- `update_f_commits_hourly`: Update commits fact table with granularity hours.

### ETL Pipeline Design

The ETL pipeline is designed with the following key principles:

#### Data Partitioning
- Each task operates on a daily partition basis
- Data is organized by date to enable efficient processing and management
- Partitioning allows for:
  - Parallel processing of different date ranges
  - Easy reprocessing of specific time periods
  - Efficient data organization and retrieval

#### Idempotency
- All tasks are designed to be idempotent, guaranteeing consistent results across multiple runs
- Each rerun of a task for a specific partition will produce the same output
- This is achieved through:
  - Partition-based data overwriting: Each run completely replaces the data for its partition
  - Isolated partition processing: Operations on one day's data do not affect other days
  - Deterministic transformations: Same input always produces the same output

#### Operation and Maintainability
- **Data Backfilling Capabilities**
  + Pros:
    - Can rerun data independently by day if errors occur
    - Enables selective historical data reprocessing
  + Cons:
    - Can consume significant resources and time when running data for extended periods (1 year or more)
    - Requires careful resource planning for large-scale backfills
    - May impact current production workloads during extensive backfill operations

## Data Model

The project implements a star schema design optimized for analyzing GitHub commit patterns:

![Data Model](images/data-model.png)

### 🔄 Staging Layer
`staging_commits`
- Serves as the intermediate storage for transformed GitHub commit data
- Partitioned by date for efficient data loading and historical analysis
- Key fields:
  - `commit_sha`: Unique identifier for each commit
  - `committer_id`, `committer_name`, `committer_email`: Committer details
  - `committer_date`: Timestamp of the commit
  - `dt`: Partition date

### 📊 Dimension Tables

#### Time Dimension (`d_time`)
- Breaks down 24-hour periods into 3-hour ranges
- Enables time-based aggregation and analysis
- Fields:
  - `d_time_id`: Hour identifier (0-23)
  - `hour_range_str`: Human-readable time range (e.g., "01-03", "04-06")

#### Date Dimension (`d_date`)
- Stores calendar attributes for temporal analysis
- Fields:
  - `d_date_id`: Unique date identifier
  - `date_str`: String representation of date
  - `weekday`: Day of the week
  - `dt`: Date value

### 📈 Fact Table

`f_commits_hourly`
- Central table for commit activity analysis
- Granularity: Hourly commits per committer
- Uses `committer_email` as a reliable identifier
  > **Why email instead of ID?** GitHub API may return null values for `committer_id` in repository commits. Email addresses provide a more reliable way to track commit activity.
- Key metrics:
  - Time dimensions: Links to both date and time for flexible analysis
  - `commit_count`: Number of commits in the time period
  - Partitioned by date for optimal query performance

### Key Features
- ✨ Optimized star schema for commit analysis
- 📅 Date-based partitioning across tables
- 🔗 Maintained referential integrity through foreign keys
- 📧 Reliable committer tracking using email addresses

## Schedule

- Runs daily
- Processes data for the previous day
- Idempotent execution

## Setup

1. Set up Cloud Composer environment
2. Configure Airflow variables:
   ```
   github_token: Your GitHub API token
   ```

3. Update `config.py` with your:
   - GCS bucket
   - BigQuery project and dataset
   - Other configurations

## CI/CD

The pipeline uses GitHub Actions for continuous integration and deployment:

1. Required Secrets:
   ```
   AIRR_LABS_GIHUB_TOKEN: GitHub token for container registry access
   AIRR_LABS_GCP_SA_KEY: GCP service account key
   AIRR_LABS_GCP_PROJECT_ID: GCP project ID
   AIRR_LABS_COMPOSER_ENV_NAME: Cloud Composer environment name
   AIRR_LABS_COMPOSER_LOCATION: Cloud Composer environment location
   ```

2. Workflow Steps:
   - On push/PR to main branch:
     - Runs tests in custom Docker container
     - If tests pass, authenticates with GCP
     - Syncs DAGs, SQL, and plugins to Cloud Composer's GCS bucket

## Project Structure

```
.
├── dags/
│   ├── dag_github_commits_etl.py    # Main DAG file
│   └── config/
│       └── config.py                # Configuration
├── docker-build/                    # Docker build configurations
│   ├── Dockerfile
│   ├── build.sh
│   ├── dockerconfig.json
│   ├── push-gar-gcp.sh
│   └── push-ghcr-github.sh
├── plugins/
│   ├── gcs.py                       # GCS utilities
│   ├── operators/
│   │   ├── github_to_gcs.py        # Bronze layer operator
│   │   ├── gcs_json_to_parquet.py  # Parquet conversion operator
│   │   └── gcs_transform.py        # GCS transformation operator
│   └── utils/
│       └── time_utils.py           # Time utility functions
├── sql/
│   ├── init_table.sql              # Table initialization
│   ├── merge_d_date.sql            # Date dimension merge
│   ├── merge_f_commits_hourly.sql  # Commits fact table merge
│   └── query/                      # Analysis queries
│       ├── 1-top-5-committers.sql
│       ├── 2-committer-longest-streak-by-day.sql
│       └── 3-generate-heat-map.sql
├── requirements.txt                 # Python dependencies
```

## Data Analysis Results

The following analysis was performed on commit data collected from the Linux kernel repository (https://github.com/torvalds/linux):

### Top 5 Committers by Commit Count

Query results show the most active contributors based on total number of commits:

1. kuba@kernel.org (2,792 commits)
2. akpm@linux-foundation.org (1,570 commits)
3. gregkh@linuxfoundation.org (1,481 commits)
4. torvalds@linux-foundation.org (1,444 commits)
5. alexander.deucher@amd.com (1,318 commits)

![Top 5 Committers Query](images/top-5-committers.png)

### Longest Commit Streak

Analysis of continuous daily commit patterns reveals:
- Linus Torvalds (torvalds@linux-foundation.org) holds the longest streak at 35 consecutive days of commits

![Longest Commit Streak Query](images/longest-commit-streak.png)

### Commit Activity Heatmap

The heatmap analysis shows commit patterns across days of the week and time blocks (24-hour divided into 3-hour ranges):

Key insights:
- Highest activity: Tuesday-Thursday during 10-12 time block
- Weekend activity is notably lower, especially on Sundays
- Early morning hours (01-03) show consistent but lower activity
- Peak hours vary by day but generally fall within working hours
- Saturday shows interesting spikes in early morning (01-03) and late night (22-00)

![Commit Activity Heatmap Query](images/commit-heatmap.png)

## Development

> **Note**: Local Airflow development environment setup is work in progress. Currently using Cloud Composer - if you're interested in accessing the environment, please contact me and provide your Gmail address for IAM grant in GCP.
