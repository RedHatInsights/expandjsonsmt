# Kafka connect SMT to expand JSON string
This java lib implements Kafka connect SMT (Single Message Transformation) to
extract JSON object from input string field.

## Config
Use it in connector config file like this:
~~~json
...
"transforms": "expand",
"transforms.expand.type": "com.redhat.insights.expandjsonsmt.ExpandJSON$Value",
"transforms.expand.sourceField": "metadata",
"transforms.expand.jsonTemplate": "{\"country\":\"\",\"address\":{\"city\":\"\",\"code\":0}}",
"transforms.expand.outputField": "metadata_obj"
...
~~~

## Install to Kafka Connect
After build copy file `target/kafka-connect-smt-expandjsonsmt-1.0-SNAPSHOT-jar-with-dependencies.jar`
to Kafka Connect container `` copying to its docker image or so.

It can be done adding this line to Dockerfile:
~~~Dockerfile
COPY ./kafka-connect-smt-expandjsonsmt-1.0-SNAPSHOT-jar-with-dependencies.jar $KAFKA_CONNECT_PLUGINS_DIR
~~~

## Example
~~~bash
# build jar file and store to target directory
mvn package

# start example containers (kafka, postgres, elasticsearch, ...)
docker-compose up --build

# when containers started run in separate terminal:
cd dev
./connect.sh # init postgres and elasticsearch connectors
./show_topics.sh # check created topic 'dbserver1.public.hosts' in kafka
./show_es.sh # check transformed documents imported from postgres to elasticsearch

# ... stop containers
docker-compose down
~~~
