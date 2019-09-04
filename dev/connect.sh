#!/bin/bash

# Connect source postgres database.
curl -i -X POST -H "Accept:application/json" \
                -H "Content-Type:application/json" \
                 http://localhost:8083/connectors/ -d @register_postgres.json

# Connect output elasticsearch.
curl -i -X POST -H "Accept:application/json" \
                -H "Content-Type:application/json" \
                 http://localhost:8083/connectors/ -d @register_es.json
