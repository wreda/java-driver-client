#~/bin/sh
sudo rm -rf /mnt/cassandra-logs
sudo mkdir /mnt/cassandra-logs
sudo mount -t tmpfs -o size=512m tmpfs /mnt/cassandra-logs
