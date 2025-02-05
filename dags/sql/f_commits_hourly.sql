MERGE INTO `personal-project-447516.airr_labs_interview.f_commits_hourly` AS target
USING (
  SELECT 
    committer_id,
    committer_name,
    EXTRACT(HOUR FROM PARSE_DATETIME('%Y-%m-%dT%H:%M:%SZ', committer_date) + INTERVAL 7 hour) AS hour, 
    dt,
    COUNT(1) AS commit_count
  FROM `personal-project-447516.airr_labs_interview.raw_commits`
  WHERE dt = '{{ ds }}'
  GROUP BY committer_id, committer_name, hour, dt
) AS source
ON target.committer_id = source.committer_id 
   AND target.hour = source.hour
   AND target.dt = source.dt

WHEN MATCHED THEN
  UPDATE SET target.commit_count = source.commit_count

WHEN NOT MATCHED BY TARGET THEN
  INSERT (committer_id, committer_name, hour, dt, commit_count)
  VALUES (source.committer_id, source.committer_name, source.hour, source.dt, source.commit_count)

WHEN NOT MATCHED BY SOURCE THEN
  UPDATE SET target.commit_count = 0;

DELETE FROM `personal-project-447516.airr_labs_interview.f_commits_hourly`
WHERE commit_count = 0 AND dt = '{{ ds }}';
