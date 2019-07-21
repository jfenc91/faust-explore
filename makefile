DOCKER_MACHINE=192.168.99.103
test:
	tox
start:
	docker run -d --name zookeeper -p 2181:2181 confluent/zookeeper
	sleep 1
	docker run -d --name kafka -p 9092:9092 --link zookeeper:zookeeper -e KAFKA_ZOOKEEPER_CONNECT=zookeeper -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INSIDE:PLAINTEXT,OUTSIDE:PLAINTEXT -e KAFKA_LISTENERS=INSIDE://localhost:9090,OUTSIDE://:9092  -e  KAFKA_ADVERTISED_LISTENERS=INSIDE://localhost:9090,OUTSIDE://${DOCKER_MACHINE}:9092 -e KAFKA_INTER_BROKER_LISTENER_NAME=OUTSIDE wurstmeister/kafka:2.11-0.11.0.3
stop:
	docker rm -f zookeeper | true
	docker rm -f kafka | true

