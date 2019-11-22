# Kafka connect SMT to expand JSON string
This java lib implements Kafka connect SMT (Single Message Transformation) to
extract JSON object from input string field.

## Config
Use it in connector config file like this:
~~~json
...
"transforms": "expand",
"transforms.expand.type": "com.redhat.insights.expandjsonsmt.ExpandJSON$Value",
"transforms.expand.sourceFields": "metadata"
...
~~~

Use dot notation for deeper fields (e. g. `level1.level2`).

## Install to Kafka Connect
After build copy file `target/kafka-connect-smt-expandjsonsmt-0.0.5-assemble-all.jar`
to Kafka Connect container `` copying to its docker image or so.

It can be done adding this line to Dockerfile:
~~~Dockerfile
COPY ./target/kafka-connect-smt-expandjsonsmt-0.0.5-assemble-all.jar $KAFKA_CONNECT_PLUGINS_DIR
~~~

Or download current release:
~~~Dockerfile
RUN curl -fSL -o /tmp/plugin.tar.gz \
    https://github.com/RedHatInsights/expandjsonsmt/releases/download/0.0.5/kafka-connect-smt-expandjsonsmt-0.0.5.tar.gz && \
    tar -xzf /tmp/plugin.tar.gz -C $KAFKA_CONNECT_PLUGINS_DIR && \
    rm -f /tmp/plugin.tar.gz;
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
