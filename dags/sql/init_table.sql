CREATE TABLE IF NOT EXISTS `personal-project-447516.airr_labs_interview.staging_commits`
(
  commit_sha STRING,
  committer_id INT64,
  committer_name STRING,
  committer_email STRING,
  committer_date STRING,
  dt DATE
)
PARTITION BY dt;

CREATE TABLE IF NOT EXISTS `personal-project-447516.airr_labs_interview.d_date`
(
  d_date_id INT64,
  date_str STRING,
  weekday STRING,
  dt DATE
);

CREATE TABLE IF NOT EXISTS `personal-project-447516.airr_labs_interview.d_time` AS
SELECT hour_value as d_time_id, FORMAT('%02d-%02d', 
    IF(hour_value = 0, 22, CAST(((hour_value - 1) / 3) * 3 + 1 AS INT64)),
    IF(hour_value = 0, 0, CAST(((hour_value - 1) / 3) * 3 + 3 AS INT64))
  ) AS hour_range_str
FROM UNNEST(GENERATE_ARRAY(0, 23)) AS hour_value;

CREATE TABLE IF NOT EXISTS `personal-project-447516.airr_labs_interview.f_commits_hourly`
(
  d_date_id INT64,
  d_time_id INT64,
  committer_id INT64,
  committer_email STRING,
  commit_count INT64,
  dt DATE
)
PARTITION BY dt;