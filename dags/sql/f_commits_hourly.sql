
DELETE FROM `personal-project-447516.airr_labs_interview.f_commits_hourly`
WHERE dt = '{{ dt }}'

INSERT INTO `personal-project-447516.airr_labs_interview.f_commits_hourly`
SELECT 
  committer_id,
  committer_name,
  EXTRACT(HOUR FROM PARSE_DATETIME('%Y-%m-%dT%H:%M:%SZ', committer_date)) AS hour, 
  dt,
  COUNT(1) AS commit_count
FROM `personal-project-447516.airr_labs_interview.raw_commits`
WHERE dt = '{{ dt }}'
GROUP BY committer_id, committer_name, hour, dt