docker exec -it kafka /opt/bitnami/bin/kafka-topics.sh \ 
--create \ 
--zookeeper zookeeper:2181 \ 
--replication-factor 1 \ 
--partitions 1 \ 
--topic answer