import requests
import boto3
import json
from datetime import datetime, timedelta

def extract_orders_to_s3():
    # --- CONFIG ---
    bucket = "my-company-data-lake"
    region = "us-west-2"
    secret_name = "api/example_app_creds"  # Stored in AWS Secrets Manager

    # --- RETRIEVE API CREDENTIALS SECURELY ---
    secrets_client = boto3.client("secretsmanager", region_name=region)
    secret_value = json.loads(secrets_client.get_secret_value(SecretId=secret_name)["SecretString"])
    client_id = secret_value["client_id"]
    client_secret = secret_value["client_secret"]

    # --- STEP 1: Get OAuth token ---
    auth_url = "https://api.example.com/oauth/token"
    auth_response = requests.post(
        auth_url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }
    )
    auth_response.raise_for_status() # Raises an exception if the API returns an HTTP error, rather than continuing silently
    access_token = auth_response.json()["access_token"]

    # --- STEP 2: Determine yesterday in Pacific Time ---
    pacific = ZoneInfo("America/Los_Angeles")
    today_pt = datetime.now(tz=pacific).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_pt = today_pt - timedelta(days=1)

    # --- STEP 3: Prepare API call for yesterday's calendar date ---
    params = {
        "start_time": yesterday_pt.isoformat(),
        "end_time": today_pt.isoformat(),
    }

    headers = {"Authorization": f"Bearer {access_token}"}
    data_url = "https://api.example.com/orders"

    response = requests.get(data_url, headers=headers, params=params)
    response.raise_for_status()
    orders = response.json()

    # --- STEP 4:
    # Convert to NDJSON (newline-delimited JSON) for S3
    # json.dumps() to convert json object to a python string and [] to keep it a list
    lines = [json.dumps(order) for order in orders]
    payload = "\n".join(lines)

    # --- STEP 4: Upload to S3 ---
    s3 = boto3.client("s3", region_name=region)
    key = f"raw/orders/extract_date={datetime.utcnow().strftime('%Y-%m-%d')}/orders.json" # Name the file
    s3.put_object(Bucket=bucket, Key=key, Body=payload.encode("utf-8"))

    print(f"✅ Extracted {len(orders)} records to s3://{bucket}/{key}")
