#!/bin/bash
cd "$(dirname ${BASH_SOURCE[0]})"
cd ..

source ./bin/get_java.sh

$DEC_JAVA -classpath ./config/extract.properties:./lib/dataExtraction-2.0.2-SNAPSHOT-jar-with-dependencies.jar:./target/dataExtraction-2.0.2-SNAPSHOT-jar-with-dependencies.jar com.guidewire.tools.benchmarking.DataExtractionRunner -onlineupdate $@
