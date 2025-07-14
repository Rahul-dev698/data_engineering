import boto3
from botocore.exceptions import NoCredentialsError, ClientError

# AWS configuration
AWS_ACCESS_KEY = 'your-access-key'
AWS_SECRET_KEY = 'your-secret-key'
REGION_NAME = 'us-east-1'
BUCKET_NAME = 'your-bucket-name'
LOCAL_FILE = 'data/processed/ecommerce_cleaned.parquet'
S3_OBJECT_NAME = 'ecommerce/ecommerce_cleaned.parquet'

# Initialize S3 client
s3 = boto3.client(
    's3',
    region_name=REGION_NAME,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

# Upload file to S3
try:
    s3.upload_file(LOCAL_FILE, BUCKET_NAME, S3_OBJECT_NAME)
    print(f"✅ Uploaded '{LOCAL_FILE}' to 's3://{BUCKET_NAME}/{S3_OBJECT_NAME}'")
except FileNotFoundError:
    print("❌ File not found:", LOCAL_FILE)
except NoCredentialsError:
    print("❌ AWS credentials not found")
except ClientError as e:
    print("❌ AWS error:", e)
