MERGE INTO `personal-project-447516.airr_labs_interview.d_date` AS target
USING (
  SELECT CAST(FORMAT_DATE('%Y%m%d', DATE(dt)) AS INT64) as d_date_id, FORMAT_DATE('%Y-%m-%d', dt) as date_str, FORMAT_DATE('%A', dt) as weekday, MOD(EXTRACT(DAYOFWEEK FROM dt) + 5, 7) + 1 as weekday_number, dt
  FROM `personal-project-447516.airr_labs_interview.staging_commits`
  WHERE dt = '{{ ds }}'
  LIMIT 1
) AS source
ON target.dt = source.dt

WHEN NOT MATCHED BY TARGET THEN
  INSERT (d_date_id, date_str, weekday, weekday_number, dt)
  VALUES (source.d_date_id, source.date_str, source.weekday, source.weekday_number, source.dt)

WHEN NOT MATCHED BY SOURCE AND target.dt = '{{ ds }}' THEN
  DELETE
