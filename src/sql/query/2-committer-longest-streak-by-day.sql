WITH commits_agg_by_dt AS (
  SELECT
    committer_email,
    dt,
    IF (SUM(commit_count) = 0, 0, 1) as has_commit
  FROM `personal-project-447516.airr_labs_interview.f_commits_hourly`
  GROUP BY committer_email, dt
),

streak_boundaries AS (
  SELECT 
    committer_email,
    dt,
    -- Detect start of a new streak when previous day had no commit
    CASE 
      WHEN LAG(dt) OVER (PARTITION BY committer_email ORDER BY dt) IS NULL 
        OR DATE_DIFF(dt, LAG(dt) OVER (PARTITION BY committer_email ORDER BY dt), DAY) > 1 
      THEN 1 
      ELSE 0 
    END as new_streak
  FROM commits_agg_by_dt
),

streak_groups AS (
  SELECT
    committer_email,
    dt,
    SUM(new_streak) OVER (PARTITION BY committer_email ORDER BY dt) as streak_group
    FROM streak_boundaries
),

streak_lengths AS (
  SELECT
    committer_email,
    streak_group,
    COUNT(1) as streak_length
  FROM streak_groups
  GROUP BY committer_email, streak_group
),

ranked_streaks AS (
  SELECT 
    committer_email,
    MAX(streak_length) as longest_streak,
    RANK() OVER(ORDER BY MAX(streak_length) DESC) as r
  FROM streak_lengths
  GROUP BY committer_email    
)

SELECT committer_email, longest_streak
FROM ranked_streaks
WHERE r = 1;