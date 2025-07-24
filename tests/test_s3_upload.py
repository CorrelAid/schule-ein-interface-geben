import boto3
import os
from dotenv import load_dotenv
import tempfile

load_dotenv()

S3_BUCKET_NAME = "cdl-segg"

session = boto3.session.Session()

client = session.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT"),
    aws_access_key_id=os.getenv("S3_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("S3_SECRET_ACCESS_KEY"),
)
def test_s3_upload():
    with tempfile.NamedTemporaryFile() as tmp:
        tmp.write(b"Hello World")
        tmp.seek(0)
        client.upload_file(tmp.name, S3_BUCKET_NAME, "test.txt")
        client.download_file(S3_BUCKET_NAME, "test.txt", "test.txt")
        with open("test.txt", "r") as f:
            assert f.read() == "Hello World"
        os.remove("test.txt")
    client.delete_object(Bucket=S3_BUCKET_NAME, Key="test.txt")


