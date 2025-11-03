import json
import boto3
from botocore.config import Config
from botocore import UNSIGNED
from urllib3 import PoolManager

s3_private = boto3.client('s3')

s3_public = boto3.client('s3',
                         config=Config(signature_version=UNSIGNED),
                         region_name='us-east-1'
                         )

http = PoolManager()


def lambda_handler(event, context):
    print(event)

    for record in event['Records']:
        try:
            sqs_body_str = record['body']
            s3_notification = json.loads(sqs_body_str)

            raw_bucket = s3_notification['Records'][0]['s3']['bucket']['name']
            raw_key = s3_notification['Records'][0]['s3']['object']['key']

            # Get raw lead data
            raw_lead_obj = s3_private.get_object(Bucket=raw_bucket, Key=raw_key)
            print(f"Raw lead object: {raw_lead_obj}")
            raw_lead_data_str = raw_lead_obj['Body'].read().decode('utf-8')
            print(f"Raw lead data string: {raw_lead_data_str}")
            raw_lead_data = json.loads(raw_lead_data_str)
            print(f"Raw lead data: {raw_lead_data}")
            lead_id = raw_lead_data.get('event').get('lead_id')

            if not lead_id:
                print(f"No lead_id found in record. Skipping.")
                continue

            print(f"Processing lead: {lead_id}")

            # Get owner data
            bucket_name = 'dea-lead-owner'
            file_key = f"{lead_id}.json"

            owner_data_obj = s3_public.get_object(Bucket=bucket_name, Key=file_key)
            owner_data = owner_data_obj['Body'].read().decode('utf-8')

            # Send to Slack
            SLACK_WEBHOOK_URL = REDACTED

            try:
                payload = {
                    "text": owner_data,
                    "blocks": [
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": owner_data
                            }
                        }
                    ]
                }
                encoded_msg = json.dumps(payload).encode('utf-8')
                resp = http.request(
                    'POST',
                    SLACK_WEBHOOK_URL,
                    body=encoded_msg,
                    headers={'Content-Type': 'application/json'}
                )
                print(f"Slack notification sent, status: {resp.status}")
            except Exception as e:
                print(f"Failed to send Slack notification: {e}")

        except Exception as e:
            print(f"Error processing record: {e}")
            # Continue processing other records instead of returning
            continue

    return {
        'statusCode': 200,
        'body': json.dumps('Processed all records')
    }