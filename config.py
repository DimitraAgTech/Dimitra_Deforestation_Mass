import os

from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv('HOST', None)
DATABASE = os.getenv('DATABASE', None)
DB_USERNAME = os.getenv('DB_USERNAME', None)
PASSWORD = os.getenv('PASSWORD', None)

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', None)
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', None)
AWS_REGION = os.getenv('AWS_REGION', None)
S3_RESULT_BUCKET = os.getenv('S3_RESULT_BUCKET', None)

SYNC_KEY = os.getenv('SYNC_KEY', None)

DATABASE_URI = f"mysql+pymysql://{DB_USERNAME}:{PASSWORD}@{HOST}/{DATABASE}"
