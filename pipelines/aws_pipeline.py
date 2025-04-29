from etls.aws_etl import connect_to_s3, create_bucket_if_not_exits, upload_to_s3
from utils.constants import AWS_BUCKET_NAME


def aws_pipeline(ti):
    file_path = ti.xcom_pull(task_ids='reddit_extraction', key='return_value')

    s3 = connect_to_s3()
    create_bucket_if_not_exits(s3, AWS_BUCKET_NAME)
    upload_to_s3(s3, file_path, AWS_BUCKET_NAME, file_path.split('/')[-1])
    print(f"File {file_path} uploaded to S3 bucket {AWS_BUCKET_NAME}.")