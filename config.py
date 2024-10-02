import os

from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv("HOST")
DATABASE = os.getenv("DATABASE")
DB_USERNAME = os.getenv("DB_USERNAME")
PASSWORD = os.getenv("PASSWORD")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_RESULT_BUCKET = os.getenv("S3_RESULT_BUCKET")

SYNC_KEY = os.getenv("SYNC_KEY")

DEFORESTATION_API = os.getenv("DEFORESTATION_API")
NODE_CALLBACK_URL = os.getenv("NODE_CALLBACK_URL")
GOOGLE_MAP_API_DEV_KEY = os.getenv("GOOGLE_MAP_API_DEV_KEY")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 15))
WORKERS = int(os.getenv("WORKERS", 10))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

DATABASE_URI = f"mysql+pymysql://{DB_USERNAME}:{PASSWORD}@{HOST}/{DATABASE}"
