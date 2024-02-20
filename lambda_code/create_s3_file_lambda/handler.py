import json
import os
import random
import tempfile
import time
from datetime import datetime

import boto3

S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
S3_INPUT_PREFIX = os.environ["S3_INPUT_PREFIX"]
CREATE_FILE_EVERY_X_SECONDS = json.loads(os.environ["CREATE_FILE_EVERY_X_SECONDS"])
NUM_LINES_IN_S3_FILE = json.loads(os.environ["NUM_LINES_IN_S3_FILE"])

s3_resource = boto3.resource("s3")


def lambda_handler(event, context) -> None:
    for _ in range(0, 60, CREATE_FILE_EVERY_X_SECONDS):
        start_time = time.time()
        with open("shakespeare.txt") as f:
            lines = f.read().splitlines()
        with tempfile.NamedTemporaryFile(mode="w") as f:
            f.write("\n".join(random.choices(lines, k=NUM_LINES_IN_S3_FILE)))
            f.flush()
            now = datetime.utcnow().strftime("%Y_%m_%dT%H_%M_%S")
            key = f"{S3_INPUT_PREFIX}/shakespeare_{now}.txt"
            s3_resource.Bucket(S3_BUCKET_NAME).upload_file(f.name, key)
            print(f"Created file at s3://{S3_BUCKET_NAME}{key}")
        time.sleep(
            CREATE_FILE_EVERY_X_SECONDS
            - (time.time() - start_time)  # right on the second
            - 0.1  # a little less for Lambda to finish in under 1 minute
        )
