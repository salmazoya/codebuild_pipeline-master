import os
import json
import boto3
import pandas as pd
import requests
import datetime
from io import StringIO
import logging

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment Variables (Configured in Lambda)
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'pvh-kareem')
S3_FOLDER_PATH = os.getenv('S3_FOLDER_PATH', 'calendly/')
SECRET_NAME = os.getenv('CALENDLY_SECRET_NAME', 'calendly-api-key')
REGION_NAME = os.getenv('AWS_REGION', 'us-east-1')

# Initialize AWS Clients
secrets_client = boto3.client('secretsmanager', region_name=REGION_NAME)
s3_client = boto3.client('s3')

# Generate Timestamp for File Naming
timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
S3_CALENDLY_PATH = f"{S3_FOLDER_PATH}calendly_scheduled_calls_{timestamp}.csv"
S3_METRICS_PATH = f"{S3_FOLDER_PATH}campaign_metrics_{timestamp}.csv"


def get_calendly_api_key():
    """Fetch Calendly API key from Secrets Manager."""
    try:
        response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        secret = json.loads(response['SecretString'])
        return secret.get('calendly-api-key')
    except Exception as e:
        logger.error(f"Error fetching API key from Secrets Manager: {e}")
        raise


def upload_to_s3(df, s3_path):
    """Upload DataFrame to S3."""
    if df.empty:
        logger.info(f"No data to upload for {s3_path}")
        return
    
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_path,
        Body=csv_buffer.getvalue()
    )
    
    logger.info(f"Uploaded {s3_path} to S3")


def get_calendly_org_uri(api_key):
    url = "https://api.calendly.com/users/me"
    headers = {"Authorization": f"Bearer {api_key}"}
    
    response = requests.get(url=url, headers=headers)
    if response.status_code == 200:
        org_uri = response.json().get("resource", {}).get("current_organization", "")
        logger.info(f"Calendly Organization URI: {org_uri}")
        return org_uri
    else:
        logger.error(f"Error fetching Calendly Organization URI: {response.status_code}, {response.text}")
        return None


def get_event_types(api_key, org_uri):
    url = f"https://api.calendly.com/event_types?organization={org_uri}"
    headers = {"Authorization":f"Bearer {api_key}"}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        event_types = response.json().get("collection", [])
        logger.info(f"Event Types: {event_types}")
        return [event["uri"] for event in event_types]
    else:
        logger.error(f"Error fetching event types: {response.status_code}, {response.text}")
        return []


def fetch_calendly_scheduled_calls(api_key):
    org_uri = get_calendly_org_uri(api_key)
    if not org_uri:
        logger.error("Failed to retrieve Calendly organization URI. Cannot proceed.")
        return pd.DataFrame()

    event_types = get_event_types(api_key, org_uri)
    if not event_types:
        logger.error("No event types found. Cannot proceed.")
        return pd.DataFrame()

    all_events = []

    for event_type in event_types:
        url = f"https://api.calendly.com/scheduled_events?event_type={event_type}&organization={org_uri}"
        headers = {"Authorization": f"Bearer {api_key}"}

        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            for event in data.get("collection", []):
                all_events.append({
                    "event_id": event.get("uri", ""),
                    "event_type": event.get("event_type", ""),
                    "start_time": event.get("start_time", ""),
                    "end_time": event.get("end_time", ""),
                    "status": event.get("status", "N/A"),
                    "invitee_email": event.get("location", {}).get("email", "N/A")
                })
        else:
            logger.error(f"Error fetching events for type {event_type}: {response.status_code}, {response.text}")

    return pd.DataFrame(all_events)


def calculate_metrics(calendly_df):
    total_scheduled_calls = len(calendly_df)
    completed_calls = calendly_df[calendly_df["status"] == "completed"].shape[0]
    completed_calls_percentage = (completed_calls / total_scheduled_calls) * 100 if total_scheduled_calls > 0 else 0

    metrics_data = {
        "timestamp": [timestamp],
        "total_scheduled_calls": [total_scheduled_calls],
        "completed_calls": [completed_calls],
        "completed_calls_percentage": [round(completed_calls_percentage, 2)]
    }

    return pd.DataFrame(metrics_data)


def lambda_handler(event, context):
    logger.info("Lambda execution started")

    try:
        api_key = get_calendly_api_key()
        print(api_key)

        # Fetch Calendly Data
        calendly_df = fetch_calendly_scheduled_calls(api_key)

        # Upload Raw Data to S3
        upload_to_s3(calendly_df, S3_CALENDLY_PATH)

        # Calculate and Upload Metrics
        metrics_df = calculate_metrics(calendly_df)
        upload_to_s3(metrics_df, S3_METRICS_PATH)

        logger.info("Lambda execution completed successfully")

        return {
            'statusCode': 200,
            'body': json.dumps("Lambda execution completed successfully")
        }

    except Exception as e:
        logger.error(f"Error during Lambda execution: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Lambda execution failed: {e}")
        }

