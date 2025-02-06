WITH tmp AS (
  SELECT DISTINCT 
        d.weekday,
        d.weekday_number,
        t.hour_range_str, 
        f.commit_count
    FROM `personal-project-447516.airr_labs_interview.f_commits_hourly` f
    LEFT JOIN `personal-project-447516.airr_labs_interview.d_date` d 
        ON f.d_date_id = d.d_date_id
    LEFT JOIN `personal-project-447516.airr_labs_interview.d_time` t 
        ON f.d_time_id = t.d_time_id
)
SELECT 
    weekday,
    SUM(CASE WHEN hour_range_str = '01-03' THEN commit_count ELSE 0 END) AS `01-03`,
    SUM(CASE WHEN hour_range_str = '04-06' THEN commit_count ELSE 0 END) AS `04-06`,
    SUM(CASE WHEN hour_range_str = '07-09' THEN commit_count ELSE 0 END) AS `07-09`,
    SUM(CASE WHEN hour_range_str = '10-12' THEN commit_count ELSE 0 END) AS `10-12`,
    SUM(CASE WHEN hour_range_str = '13-15' THEN commit_count ELSE 0 END) AS `13-15`,
    SUM(CASE WHEN hour_range_str = '16-18' THEN commit_count ELSE 0 END) AS `16-18`,
    SUM(CASE WHEN hour_range_str = '19-21' THEN commit_count ELSE 0 END) AS `19-21`,
    SUM(CASE WHEN hour_range_str = '22-00' THEN commit_count ELSE 0 END) AS `22-00`
FROM tmp
GROUP BY weekday, weekday_number
ORDER BY weekday_number;