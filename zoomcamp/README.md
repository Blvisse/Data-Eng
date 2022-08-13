# Introduction to Docker

This project takes a dive into the popular containerization platform DOCKER. The project comes from a beginner status and builds all the way up

## Prerequisites

To successfully run this project, you will need to have:

1. Docker installed and running.
2. Your preferred OS
3. A working internet connection
4. An IDE ( I used Visual Studio)
5. postgres database

## Areas Covered

1. Creating a simple docker image and including system variables
2. Running postgres on docker
3. Reading ny_taxi data from [datalink] and ingesting it to postgres database
4. Dockerising the ingestion script

## 2. Running postgres on docker

:warning: Ensure you have docker installed before proceeding.

We pul the official postgres:13 image from docker hub and create a user and connect it to a database using cmd.

```

docker run -it
 -e POSTGRES_USER="postgres" \
 -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v /path/ to/ store db/:/var/lib/postgresql/ \
  pg-database2 postgres:13


```

This creates a docker container with a postgres:13 image inside it we use the postgres environment variable to create a user(postgres) with password: root and a new db ny_taxi




## 3. Reading ny_taxi data

Download the data from this link [datalink]. and extract it to the data directory.

The data is in parquet format and thus we use the below code snippet to read from it

```
pd.read_parquet("/path/to/data/ny_taxi.parquet",engine='pyflow');
```


Having installed postgres you can follow the [notebook] to create a connection to the postgres db and transfer data into the database.



## 3.2 Creating docker network and using pgcli

install pgcli using the commands
```
pip install pgcli
```

pgcli is a command line tool to access postgres database. We shall use it to confirm that our data has been uploaded to our database as it should be

:warning: Ensure you have pgcli installed before proceeding.

```
pgcli -h localhost -U postgres -W root -d ny_taxi root
```

This creates a connection to a host: localhost with credentials ; user:postgres and password: root accessing the ny_taxi database.

:bulb:
```
pgcli --help
```
:bulb: Run the above code for further help and documentation.

To access postgres's admin panel we need to use pg-admin. This allows us to use a gui to monitor our databases. 

To do so in docker we pull the official pg-admin image from docker hub. 

:construction: Not so fast

postgres and pg-admin are two different images and we need to connect them in order for them to communicate to each other. This can be accomplished by using docker networks.

### Docker network

A docker network enables images in the same container to communicate to each other by creating a platform/ network to facilitate it. The docker network

To create one simply type in

```
docker network create <name_of_network>

```

### Linking images

We therefore add another parameter when creating the images so as to specify the networks they shall be communicating on 


:rotating_light: Ensure you run these commands on different terminal windows 


```
docker run -it
 -e POSTGRES_USER="postgres" \
 -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v /path/ to/ store db/:/var/lib/postgresql/ \
  --network <name_of_network> \
  --name pg-database

  pg-database2 postgres:13


```

This links our postgres image to the network we created in the previous step and we give it an identifying name of pg-database


As for our pg-admin image 

```
docker run -it \
-e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
-e PGADMIN_DEFAULT_PASSWORD="root" \
-p 8080:80 \
--network=<name_of_network> \
--name pgadmin  \
page/pgadmin4 



```

This command pulls the image page/pgadmin4 from dockerhub the following parameters: 

default email:admin@admin.com

with password: root and 

binding it to port 8080:80 

we link this image to the network we had created earlier and give it an identifier name. 

You will be now able to access pgadmin on localhost:8080 with username and password being the email and password provided above. 
Set up the server and voila you have the database from the  postgres container 

Congrats on making it this far you are a true data engineer :clap:


## 4. Dockerising the ingestion script

Instead of running the docker images on seperate containers we can create a docker-compose file to have them in the same container and in the same network. 
To do this we have created a docker-compose.yml file in the root directory of the folder. To run all you have to do is type in the command 

```
docker-compose up
```

After that you can access pgadmin4 on ``localhost:8080`` with username and password as same as before.


[datalink]:(https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
[notebook]:docker_intro/notebooks/migrate_data.ipynb



