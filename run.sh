#!/bin/bash
exec mvn exec:java > /mnt/cassandra-logs/log 2>&1
