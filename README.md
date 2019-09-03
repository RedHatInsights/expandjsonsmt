
## Build
~~~bash
mvn package
~~~

## Install to Kafka Connect
After build copy file `target/kafka-connect-smt-expandjsonsmt-1.0-SNAPSHOT-jar-with-dependencies.jar`
to Kafka Connect container `` copying to its docker image or so.

It can be done adding this line to Dockerfile:
~~~Dockerfile
COPY ./kafka-connect-smt-expandjsonsmt-1.0-SNAPSHOT-jar-with-dependencies.jar /etc/landoop/jars/lib
~~~

## Usage
Use it in connector config file like this:
~~~json
...
"transforms": "expandJsonField",
"transforms.expandJsonField.type": "com.redhat.insights.expandjsonsmt.ExpandJSON$Value",
"transforms.expandJsonField.sourceField": "info",
"transforms.expandJsonField.jsonTemplate": "{\"country\":\"\",\"address\":{\"city\":\"\",\"code\":0}}",
"transforms.expandJsonField.outputField": "info_obj"
...
~~~
