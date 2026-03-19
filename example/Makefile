run_example:
	docker-compose up -d
	@make send_msg > /dev/null # this part blocks until the broker is up
	@echo "----"
	@echo "kafka is running"
	@echo "you can execute 'go run .' to see the application consuming messages"
	@echo "----"
	@while true; do make send_msg; sleep 5; done;

send_msg:
	docker exec -i kafka bash -lc 'echo "{\"Content\":\"hello\"}" | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test-users'
