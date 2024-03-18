import json
import boto3
import pandas

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # Initialize S3 client
    s3 = boto3.client('s3')
    
    
    # Read JSON file from S3
    response = s3.get_object(Bucket=bucket, Key=key)
    json_data = response['Body'].read().decode('utf-8')
        
        # Convert JSON string to DataFrame
    print("*******Dataframe********")
    df = pd.read_json(json_data)
    
    #filter the records that have status delivered
    delivered_data = df[df['status']== 'delivered']
       
    return delivered_data

