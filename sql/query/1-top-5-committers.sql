WITH commiter_agg AS (
  SELECT committer_email, SUM(commit_count) as sum_count
  FROM `personal-project-447516.airr_labs_interview.f_commits_hourly`
  GROUP BY committer_email
)

SELECT committer_email, sum_count, RANK() OVER (ORDER BY sum_count DESC) as r
FROM commiter_agg
ORDER BY r ASC
LIMIT 5
;