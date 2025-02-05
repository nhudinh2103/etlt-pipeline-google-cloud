MERGE INTO `personal-project-447516.airr_labs_interview.d_date` AS target
USING (
  SELECT CAST(FORMAT_DATE('%Y%m%d', DATE(dt)) AS INT64) as d_date_id, FORMAT_DATE('%Y-%m-%d', dt) as date_str, FORMAT_DATE('%A', dt) as weekday, dt
  FROM `personal-project-447516.airr_labs_interview.d_date`
  WHERE dt = '{{ params.dt }}'
) AS source
ON target.dt = source.dt

WHEN NOT MATCHED BY TARGET THEN
  INSERT (d_date_id, date_str, weekday, dt)
  VALUES (source.d_date_id, source.date_str, source.weekday, source.dt)

WHEN NOT MATCHED BY SOURCE THEN
  DELETE
