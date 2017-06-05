#!/bin/bash
sudo rm -r target/
mvn package -DskipTests
storm-yarn launch /opt/storm-1.0.1/conf/storm.yaml
