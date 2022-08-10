import os
from boto3.s3.transfer import S3Transfer
import boto3
from dotenv import load_dotenv
import os


# access_key=''
# secret_key=''
# s3_bucket_name=''
# s3_filename=''
# file_path=''

try:
    print("Accessing env file ... ")
    load_dotenv = load_dotenv()
    
    print("Fetching env variables ... ")
    access_key=os.getenv('AWS_ACCESS_KEY_ID')
    secret_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    s3_bucket_name=os.getenv('AWS_S3_BUCKET_NAME')
    print("s3_bucket_name: ",s3_bucket_name)
    
    file_path='/opt/airflow/data/late_deliveries'
    
    print("Environmental variables fetched successfully")
    
except Exception as e:
    print("Error in fetching env variables")
    exit(1)
    





try:
    print("Accessing aws cloud storage ...")
    client=boto3.client('s3',aws_access_key_id=access_key,aws_secret_access_key=secret_key)
    print("Connection established ...")
    print("Connection object {}: ".format(client))
    
except Exception as e:
    print("Error: ",e)
    exit(1)
    
print("Uploading file ...")
transfer=S3Transfer(client)

print("transfer - "+ s3_bucket_name)

#upload the whole spark directory with the data 
def uploadDirectory(filepath, s3_bucket_name):
    for root,dirs,files in os.walk(filepath):
        for file in files:
            #Transfer only the csv
            if file.endswith('csv'):
                print("Uploading file: "+file)
                transfer.upload_file(os.path.join(root,file), s3_bucket_name,"Clean_Data/"+ file)
                print("File uploaded: "+file)
                
                
uploadDirectory(file_path, s3_bucket_name)
