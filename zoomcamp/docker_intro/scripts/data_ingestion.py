'''
This script takes in ny_taxi data and ingests them into a postgress database

'''
try:
    import pandas as pd
    from sqlalchemy import create_engine
    import argparse
    import os
    print("Successfully loaded libraries")
    
except ModuleNotFoundError as me:
    print("Module not found: {}".format(me))
    exit(1)
    
    
def main(parameters):
    user=parameters.user
    password=parameters.password
    host=parameters.host
    database=parameters.database
    table_name=parameters.table_name
    url=parameters.url
    csv_name="../data/yellow_tripdata_2014-01.parquet"
    
    try:
        # os.system(f"curl.exe {url} -o {csv_name}")
        os.system(f"wget {url} -O {csv_name}")
        print("Successfully downloaded data ...")
        
    except Exception as e:
        print("Error: {}".format(e))
        exit(1) 
    
    try:
        data=pd.read_parquet(csv_name, engine='pyarrow')
    
    
    except FileNotFoundError as fe:
        print("File not found: {}".format(fe))
        exit(1)
        
    #generate sql alchemy engine connection
    engine=create_engine(f'postgresql://{user}:{password}@{host}:5432/{database}')

    print("Testing connection to database ...")
    engine.connect()
    print("Connection established")


    print("Converting data to DDL equivalents ...")
    #converting to ddl
    pd.io.sql.get_schema(data,name='yellow_taxi_data',con=engine)


    #insert data into our postgresql database

    print("Inserting first 900,000 rows to database ...")

    data[900040:900059].to_sql(name=table_name,con=engine, if_exists='append',index=False)

    print("Done ...")

    #create query to fetch dataframe the database
    print("Fetching first 20 rows of data ..")
    query='''

    SELECT * FROM yellow_taxi_data
    LIMIT 20;
    ''' 

    print(pd.read_sql(query, con=engine))

    print("Data Ingestion done ...")
        

if __name__ == "__main__":
    
    parser=argparse.ArgumentParser(description="Ingest data into postgres") 

    parser.add_argument("--user",help="user name for postgres")
    parser.add_argument("--password",help="password for postgres")
    parser.add_argument("--host",help="host for postgres")
    parser.add_argument("--database",help="database for postgres")
    parser.add_argument("--table_name",help="table name to write results to")
    parser.add_argument("--url",help="url of csv file")
    
    args=parser.parse_args()
    print("The following arguments have been entered: {}".format(args))

    main(args)

# URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2014-01.parquet"

# python data_ingestion.py --user=root --password=root --host=localhost --database=ny_taxi --table_name=yellow_taxi_data2 --url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2014-01.parquet"