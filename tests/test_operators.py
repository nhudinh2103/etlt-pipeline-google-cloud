import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock, mock_open
from plugins.operators.github_to_gcs import GitHubToGCSOperator
from plugins.operators.duckdb_transform import DuckDBTransformOperator

def test_github_to_gcs_operator():
    with patch('requests.get') as mock_get:
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = [{
            'sha': 'test_sha',
            'commit': {
                'author': {
                    'name': 'Test Author',
                    'email': 'test@example.com',
                    'date': '2025-02-03T00:00:00Z'
                },
                'message': 'Test commit'
            }
        }]
        mock_get.return_value = mock_response

        operator = GitHubToGCSOperator(
            task_id='test_task',
            github_token='test_token',
            gcs_bucket='test_bucket',
            bronze_path='test_path',
            api_url='test_url'
        )

        # Test execution
        with patch('pandas.DataFrame.to_parquet') as mock_to_parquet:
            operator.execute({
                'execution_date': datetime(2025, 2, 3)
            })
            
            assert mock_to_parquet.called

def test_duckdb_transform_operator():
    # Mock SQL content
    sql_content = """
    SELECT
        commit_sha,
        TRIM(author_name) as author_name,
        LOWER(TRIM(author_email)) as author_email,
        TRIM(commit_message) as commit_message,
        committed_at,
        created_date
    FROM raw_commits
    """

    # Mock data
    test_data = {
        'commit_sha': ['test_sha'],
        'author_name': ['  Test Author  '],
        'author_email': ['TEST@EXAMPLE.COM'],
        'commit_message': ['  Test message  '],
        'committed_at': ['2025-02-03T00:00:00Z'],
        'created_date': ['2025-02-03']
    }

    with patch('builtins.open', mock_open(read_data=sql_content)), \
         patch('os.path.exists', return_value=True), \
         patch('duckdb.connect') as mock_connect:
        
        # Mock DuckDB connection and execution
        mock_con = MagicMock()
        mock_connect.return_value = mock_con
        
        # Mock fetchone for row count
        mock_con.execute().fetchone.return_value = [1]

        operator = DuckDBTransformOperator(
            task_id='test_transform',
            source_path='gs://test-bucket/bronze/dt=2025-02-03/commits.parquet',
            destination_path='gs://test-bucket/staging/dt=2025-02-03/commits_transformed.parquet',
            sql_path='dags/sql/transform_commits.sql'
        )

        # Test execution
        operator.execute(context={})
        
        # Verify DuckDB operations
        assert mock_con.execute.called
        
        # Verify SQL execution calls
        sql_calls = [call[0][0] for call in mock_con.execute.call_args_list]
        assert any('CREATE VIEW raw_commits' in sql for sql in sql_calls)
        assert any('COPY' in sql for sql in sql_calls)
        assert any('COUNT' in sql for sql in sql_calls)
