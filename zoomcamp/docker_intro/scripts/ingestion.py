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
    
    csv_name="data/yellow_tripdata_2021-01.parquet"
    metadata_path="data/taxi+_zone_lookup.csv"
    
    
    
    try:
        data=pd.read_parquet(csv_name, engine='pyarrow')
        lookup=pd.read_csv(metadata_path)
    
    
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

    print("Inserting data rows to database ...")

    data.to_sql(name=table_name,con=engine, if_exists='replace',index=False)

    print("Done ...")
    
    pd.io.sql.get_schema(lookup,name='lookup_table',con=engine)
    
    print("inserting metadata into lookup table")
    lookup.to_sql(name="lookup_data",con=engine,if_exists='replace',index=False)
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
   
    
    args=parser.parse_args()
    print("The following arguments have been entered: {}".format(args))

    main(args)

# URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2014-01.parquet"

# python data_ingestion.py --user=root --password=root --host=localhost --database=ny_taxi --table_name=yellow_taxi_data2 --url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2014-01.parquet"