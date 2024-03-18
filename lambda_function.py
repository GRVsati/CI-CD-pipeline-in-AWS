import json
import boto3
import pandas as pd

def lambda_handler(event, context):
    # Extract bucket and key from the S3 event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # Initialize S3 clients for both input and output buckets
    s3 = boto3.client('s3')
    
    # Read JSON file from S3
    response = s3.get_object(Bucket=bucket, Key=key)
    json_data = response['Body'].read().decode('utf-8')
    
    # Convert JSON string to DataFrame
    df = pd.read_json(json_data)
    
    # Filter the records that have status 'delivered'
    delivered_data = df[df['status'] == 'delivered']
    
    # Convert filtered data to JSON
    json_response = delivered_data.to_json(orient='records')
    
    # Put JSON response into another S3 bucket
    output_bucket = 'doordash-target-zn-1' 
    
    output_key = "doordash-target-zn-1/filtered_data/.json"
    
    output_file = key.replace('.json', '_filtered.json')  
    s3.put_object(Bucket=output_bucket, Key=output_key, Body=json_response)
    
    return {
        'statusCode': 200,
        'body': 'Filtered JSON data stored in S3'
    }
