SELECT dag_id, execution_date, state 
FROM dag_run 
WHERE dag_id = 'mobile_aws_pipeline';


DELETE FROM dag_run 
WHERE dag_id = 'mobile_aws_pipeline' 
  AND execution_date = '2025-05-01 00:00:00+00';