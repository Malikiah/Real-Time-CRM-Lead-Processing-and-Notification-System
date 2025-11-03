import json
import boto3
import uuid
import datetime
import json


S3_BUCKET_NAME = 'real-time-crm-dea-project'
s3_client = boto3.client('s3')
sqs = boto3.client('sqs')
QUEUE_URL = os.environ.get('MY_SQS_QUEUE_URL')


def lambda_handler(event, context):
    try:
        raw_body_string = event.get('body')

        if not raw_body_string:
            print("Received request with no body. Returning 400.")
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Request body is missing.'})
            }

        try:
            webhook_data = json.loads(raw_body_string)
        except json.JSONDecodeError:
            print("Body is not valid JSON. Storing as plain text file.")
            webhook_data = raw_body_string

    except Exception as e:
        print(f"Error extracting body from event: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Failed to process request data.'})
        }

    lead_id = webhook_data.get('event', {}).get('lead_id', 'unknown')

    print(f"Received webhook for lead ID: {lead_id}")

    s3_key = f"webhooks/crm_event_{lead_id}.json"

    s3_file_content = json.dumps(webhook_data, indent=2) if isinstance(webhook_data, dict) else raw_body_string
    content_type = 'application/json' if isinstance(webhook_data, dict) else 'text/plain'

    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=s3_file_content.encode('utf-8'),
            ContentType=content_type
        )
        print(f"Successfully uploaded webhook data to s3://{S3_BUCKET_NAME}/{s3_key}")

        message_to_send = {
            'lead_id': lead_id,
            'status': 'new',
            'received_at': context.aws_request_id
        }

    except Exception as e:
        print(f"ERROR: Could not upload to S3. Check bucket name and Lambda IAM permissions. Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Internal Server Error during S3 upload.'})
        }

    return {
        'statusCode': 200,
        'body': json.dumps(f"Received webhook for lead ID: {lead_id}")
    }
