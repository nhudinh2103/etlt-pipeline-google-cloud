CREATE TABLE IF NOT EXISTS `personal-project-447516.airr_labs_interview.raw_commits`
(
  committer_id INT64,
  committer_name STRING,
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

CREATE TABLE IF NOT EXISTS `personal-project-447516.airr_labs_interview.d_time`
(
  d_time_id INT64,
  hour_range_str STRING
);