FROM python:3.9

RUN pip install pandas

WORKDIR /app

COPY pipeline.py pipeline.py

ENTRYPOINT ["python","pipeline.py"]



docker run -it -e POSTGRES_USER="postgres" -e POSTGRES_PASSWORD="root" -e POSTGRES_DB="ny_taxi" -v c:/users/blais/desktop/masters/ml-track/data-eng/zoomcamp/docker_intro/ny_taxi_postgres_data:/var/lib/postgresql/data  -p 5432:5432  --network=pd-admin --name pg-database2 postgres:13


docker run -it -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" -e PGADMIN_DEFAULT_PASSWORD="root" -p 8080:80 dpage/pgadmin 

##create network first
docker network create pd-admin

## connecting docker containers using docker network

docker run -it -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" -e PGADMIN_DEFAULT_PASSWORD="root" -p 8080:80 --network=pd-admin --name pgadmin  page/pgadmin 
