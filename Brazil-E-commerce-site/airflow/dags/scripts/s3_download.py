#using boto3 to get the data from aws s3
from xmlrpc import client
from boto3.s3.transfer import S3Transfer
import boto3
from dotenv import load_dotenv
import os



#we try fetching environmental variables from .env file

try:
    print("Accessing env file ... ")
    load_dotenv = load_dotenv()
    
    print("Fetching env variables ... ")
    access_key=os.getenv('AWS_ACCESS_KEY_ID')
    secret_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    s3_bucket_name=os.getenv('AWS_S3_BUCKET_NAME')
    print("s3_bucket_name: ",s3_bucket_name)
    s3_filename='brazil_data.zip'
    download_path='/opt/airflow/data'
    
    print("Environmental variables fetched successfully")
    
except Exception as e:
    print("Error in fetching env variables")
    exit(1)

try:
    print("Accessing aws cloud storage ...")
    client=boto3.client('s3',aws_access_key_id=access_key,aws_secret_access_key=secret_key)
    print("Connection established with object {}...".format(client))
except Exception as e:
    print("Error: ",e)
    exit(1)
    
print("Downloading file ...")
for key in client.list_objects(Bucket=s3_bucket_name)['Contents']:
    print(key)
client.download_file(s3_bucket_name,s3_filename,download_path)
