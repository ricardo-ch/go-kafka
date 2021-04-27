run_example:
	docker compose up -d
	@make send_msg > /dev/null # this part blocks until the broker is up
	@echo "----"
	@echo "kafka is running"
	@echo "you execute 'go run .' to see the application consuming messages"
	@echo "----"
	@while true; do make send_msg; sleep 5; done;

send_msg:
	docker exec -it kafka bash -c 'echo "{\"Content\":\"hello\"}" | kafka-console-producer --bootstrap-server localhost:9092 --topic test-users'