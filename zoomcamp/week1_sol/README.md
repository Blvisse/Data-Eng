# Solution
## Dockerizing Ingestion script
First part of the solution will be to dockerize the ingestion script into its indipendent docker container. This is done by the Dockerfile in this folder

:warning: But first esnure you have both postgres and pgcli running on your local machine to facilitate communication

### Step 1
traverese to the docker_intro folder and run the following command

```bash
docker-compose up -it
```

This will spin up a docker image of both postgres and pgcli.
For more info you can check out the docker file to understand their inner workings

### Step 2
Identify the network that the image is connected to. That can be achieved by running the following command.
```bash
docker network ls
```

The output will be something similar to this

```bash

NETWORK ID     NAME                   DRIVER    SCOPE
308e250e2327   bridge                 bridge    local
70f9b6a870f9   docker_intro_default   bridge    local
d7c609853e07   host                   host      local
3bc203026108   none                   null      local



```

:bulb: The network name is similar to the directory in which the image is running on

In our case the network that our postgres and pgcli is running on is docker_intro_default.

### Step 3
Build our docker image with our ingestion script

We now build our docker file located in this directory which will host the ingestion script

The command will be as follows.

```bash
docker build -t <insert your tag name> .
```

:warning: don't forget the fullstop at the end since it symbolizes that the Dockerfile to build from is in this directory.

### step 4
Run and link our docker image to the network 

Using the command below you run the docker image on interactive mode and link it to the same network as the pgcli and database. 

```bash

docker run -it \
--network=<network name> \
<name tag of your ingestion script image> \
--user=root \
--password=root \
--host=pgdatabase \
--database=ny_taxi \
--table_name=jan_trips


```

### Step 5
Accessing pgcli to confirm that the ingestion has worked

```bash
pgcli -h localhost -U root ny_taxi
```

And check if a new table has been added


## Solutions to quries

Solutions to various queries have been saved to query_sol.sql
