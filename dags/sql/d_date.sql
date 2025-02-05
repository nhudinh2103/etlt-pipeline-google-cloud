WITH date_check AS (
  SELECT COUNT(1) as count
  FROM `personal-project-447516.airr_labs_interview.raw_commits`
  WHERE dt = '{{ params.dt }}'
)

DELETE FROM `personal-project-447516.airr_labs_interview.d_date`
WHERE dt = '{{ params.dt }}'

INSERT INTO `personal-project-447516.airr_labs_interview.d_date`
SELECT FORMAT_DATE('%Y%m%d', DATE('{{ params.dt }}')) AS INT64 as d_date_id, 
       FORMAT_DATE('%Y-%m-%d', '{{ params.dt }}') as date_str,
       FORMAT_DATE('%A', '{{ params.dt }}') as weekday, '{{ params.dt }}'