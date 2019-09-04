#!/bin/bash

docker-compose exec kafka ./bin/kafka-topics.sh --list --zookeeper zookeeper:2181
