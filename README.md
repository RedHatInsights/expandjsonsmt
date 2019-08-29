
## Build
~~~bash
mvn package
~~~

## Install to Kafka Connect
After build copy file `target/kafka-connect-smt-expandjsonsmt-1.0-SNAPSHOT.jar`
to Kafka Connect container `` copying to its docker image or so.

It can be done adding this line to Dockerfile:
~~~Dockerfile
COPY ./kafka-connect-smt-expandjsonsmt-1.0-SNAPSHOT.jar /etc/landoop/jars/lib
~~~

## Usage
Use it in connector config file like this:
~~~json
...
"transforms": "ReplaceField",
"transforms.ReplaceField.type": "com.redhat.insights.expandjsonsmt.ReplaceField$Value",
"transforms.ReplaceField.blacklist": "field_to_replace"
...
~~~
