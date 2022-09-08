FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2-binary pyarrow


WORKDIR /app

COPY /scripts/ingestion.py data_ingestion.py
RUN mkdir data
RUN wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet -P data/
RUN wget https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv -P data/

ENTRYPOINT ["python","data_ingestion.py"]



# docker run -it -e POSTGRES_USER="postgres" -e POSTGRES_PASSWORD="root" -e POSTGRES_DB="ny_taxi" -v c:/users/blais/desktop/masters/ml-track/data-eng/zoomcamp/docker_intro/ny_taxi_postgres_data:/var/lib/postgresql/data  -p 5432:5432  --network=pd-admin --name pg-database2 postgres:13


# docker run -it -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" -e PGADMIN_DEFAULT_PASSWORD="root" -p 8080:80 dpage/pgadmin 

# ##create network first
# docker network create pd-admin

# ## connecting docker containers using docker network

# docker run -it -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" -e PGADMIN_DEFAULT_PASSWORD="root" -p 8080:80 --network=pd-admin --name pgadmin  page/pgadmin 

# python data_ingestion.py --user=root --password=root --host=localhost --database=ny_taxi --table_name=yellow_taxi_data2 --url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2014-01.parquet"

# #run this first
# docker built -t taxi_ingest:01
# docker -it --network=pd-admin taxi_ingest:01 --user=root --password=root --host=pg-admin --database=ny_taxi --table_name=yellow_taxi_data2 --url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2014-01.parquet"