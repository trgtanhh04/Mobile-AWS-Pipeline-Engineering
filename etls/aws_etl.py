import s3fs
from utils.constants import AWS_ACCESS_KEY, AWS_ACCESS_KEY_ID, AWS_BUCKET_NAME, AWS_REGION

def connect_to_s3():
    try:
        s3 = s3fs.S3FileSystem(
            key=AWS_ACCESS_KEY_ID, 
            secret=AWS_ACCESS_KEY
        )
        print('CConnect success to S3')
        return s3
    except Exception as e:
        raise Exception(f'faild connect to S3 by s3fs: {str(e)}')
    

def create_bucket_if_not_exits(s3, bucket_name):

    try:
        existing_buckets = s3.ls('')
    
        if bucket_name in existing_buckets:
            print(f"Bucket '{bucket_name}' đã tồn tại.")
        else:
            # Tạo bucket
            s3.mkdir(bucket_name)
            print(f"Đã tạo bucket '{bucket_name}' thành công.")
    except Exception as e:
        raise Exception(f"Lỗi khi kiểm tra hoặc tạo bucket: {str(e)}")


def upload_to_s3(s3: s3fs.S3FileSystem, file_path: str, bucket: str, s3_file_name: str):
    """
    Tải file lên S3 bằng s3fs.

    :param s3: Đối tượng kết nối S3 từ s3fs.S3FileSystem.
    :param file_path: Đường dẫn tới file cục bộ cần tải lên.
    :param bucket: Tên bucket trên S3.
    :param s3_file_name: Tên file trên bucket S3 (bao gồm cả đường dẫn trong bucket).
    """
    try:
        # Đường dẫn đầy đủ trên S3
        s3_full_path = f"{bucket}/raw/{s3_file_name}"
        
        # Tải file lên S3
        s3.put(file_path, s3_full_path)
        print(f"File '{file_path}' đã được tải lên S3 tại '{s3_full_path}'.")
    except FileNotFoundError:
        print(f"Lỗi: File '{file_path}' không tồn tại.")
    except PermissionError:
        print(f"Lỗi: Không có quyền truy cập file '{file_path}'.")
    except Exception as e:
        print(f"Lỗi không xác định khi tải file lên S3: {str(e)}")

