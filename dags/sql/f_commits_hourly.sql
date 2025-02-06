MERGE INTO `personal-project-447516.airr_labs_interview.f_commits_hourly` AS target
USING (
  SELECT
    CAST(FORMAT_DATE('%Y%m%d', dt) AS INT64) AS d_date_id,
    EXTRACT(HOUR FROM PARSE_DATETIME('%Y-%m-%dT%H:%M:%SZ', committer_date) + INTERVAL 7 hour) AS d_time_id, 
    committer_id,
    committer_email,
    dt,
    COUNT(1) AS commit_count
  FROM `personal-project-447516.airr_labs_interview.staging_commits`
  WHERE dt = '{{ ds }}'
  GROUP BY d_date_id, d_time_id, committer_id, committer_email, dt
) AS source
ON target.committer_email = source.committer_email
   AND target.d_time_id = source.d_time_id
   AND target.d_date_id = source.d_date_id

WHEN MATCHED THEN
  UPDATE SET target.commit_count = source.commit_count

WHEN NOT MATCHED BY TARGET THEN
  INSERT (committer_id, committer_email, d_date_id, d_time_id, dt, commit_count)
  VALUES (source.committer_id, source.committer_email, source.d_date_id, source.d_time_id, source.dt, source.commit_count)

WHEN NOT MATCHED BY SOURCE AND target.dt = '{{ ds }}' THEN
  UPDATE SET target.commit_count = 0;

DELETE FROM `personal-project-447516.airr_labs_interview.f_commits_hourly`
WHERE commit_count = 0 AND dt = '{{ ds }}';
