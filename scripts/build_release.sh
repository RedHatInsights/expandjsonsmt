#!/bin/bash

# ./scripts/build_release.sh 0.0.3

set -e
VERSION=$1

mvn package

echo "Building release ${VERSION}"

INPUT=kafka-connect-smt-expandjsonsmt-${VERSION}-assemble-all.jar
OUTPUT=kafka-connect-smt-expandjsonsmt-${VERSION}.tar.gz

cd target
tar cfz $OUTPUT $INPUT
printf "Input file:\n$(realpath ${INPUT})\nBuilt into:\n$(realpath ${OUTPUT})"
cd ..
