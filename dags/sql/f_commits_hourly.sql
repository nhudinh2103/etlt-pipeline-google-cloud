MERGE INTO `personal-project-447516.airr_labs_interview.f_commits_hourly` AS target
USING (
  SELECT 
    committer_id,
    committer_email,
    EXTRACT(HOUR FROM PARSE_DATETIME('%Y-%m-%dT%H:%M:%SZ', committer_date) + INTERVAL 7 hour) AS hour, 
    dt,
    COUNT(1) AS commit_count
  FROM `personal-project-447516.airr_labs_interview.staging_commits`
  WHERE dt = '{{ ds }}'
  GROUP BY committer_id, committer_email, hour, dt
) AS source
ON target.committer_email = source.committer_email
   AND target.hour = source.hour
   AND target.dt = source.dt

WHEN MATCHED THEN
  UPDATE SET target.commit_count = source.commit_count

WHEN NOT MATCHED BY TARGET THEN
  INSERT (committer_id, committer_email, hour, dt, commit_count)
  VALUES (source.committer_id, source.committer_email, source.hour, source.dt, source.commit_count)

WHEN NOT MATCHED BY SOURCE AND target.dt = '{{ ds }}' THEN
  UPDATE SET target.commit_count = 0;

DELETE FROM `personal-project-447516.airr_labs_interview.f_commits_hourly`
WHERE commit_count = 0 AND dt = '{{ ds }}';
