#!/bin/bash
exec sudo mvn exec:java > /mnt/cassandra-logs/log 2>&1
