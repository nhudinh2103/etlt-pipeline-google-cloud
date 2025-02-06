WITH committer_agg_by_day AS
  (SELECT committer_email,
          d_date_id,
          SUM(commit_count) AS sum_count,
          IF (SUM(commit_count) = 0,
              0,
              1) AS got_commit
   FROM `personal-project-447516.airr_labs_interview.f_commits_hourly`
   GROUP BY committer_email,
            d_date_id),
      
      distinct_committer_email as 
      (
        SELECT DISTINCT committer_email
         FROM `personal-project-447516.airr_labs_interview.f_commits_hourly` f
         WHERE committer_email = 'torvalds@linux-foundation.org'
      ),

     streak AS
  (SELECT AA.committer_email,
          AA.d_date_id,
          IF (IF(ff.committer_email IS NULL, 0, ff.sum_count) > 0,
              1,
              0) AS got_commit,
              -- Sequence increase for each email
             ROW_NUMBER() OVER (PARTITION BY AA.committer_email
                                ORDER BY AA.d_date_id)
              -- Sequence increase for each email and got_commit. got_commit is flag to check where email have commit in one specific day 
            - ROW_NUMBER() OVER (PARTITION BY AA.committer_email, got_commit
                                 ORDER BY AA.d_date_id)
              -- Substract these two row numbers, we will have streak index for each committer email
              AS rk
   FROM
     (SELECT committer_email, d_date_id
      FROM distinct_committer_email
      -- Cross join to generate all user and date (fill missing records that user date not have any commit)
      CROSS JOIN `personal-project-447516.airr_labs_interview.d_date` d) AA
   LEFT JOIN committer_agg_by_day ff ON ff.committer_email = AA.committer_email AND ff.d_date_id = AA.d_date_id
   ORDER BY AA.committer_email,
            AA.d_date_id),
     commiter_streak_count AS
  (SELECT committer_email,
          rk,
          SUM(got_commit) AS streak_count
   FROM streak
   GROUP BY committer_email,
            rk),
    ranked_longest_streak AS (
      SELECT committer_email,
            MAX(streak_count) AS longest_streak,
            RANK() OVER (ORDER BY MAX(streak_count) DESC) as r
      FROM commiter_streak_count
      GROUP BY committer_email
    )
    SELECT committer_email, longest_streak FROM ranked_longest_streak WHERE r = 1
;