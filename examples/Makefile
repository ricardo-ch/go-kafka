# W3C Trace Context traceparent (version-trace_id-parent_id-flags) for demo messages.
# Matches what OpenTelemetry DefaultTracing extracts from Kafka message headers.
TRACEPARENT ?= 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01

KAFKA_PRODUCER := /opt/kafka/bin/kafka-console-producer.sh
KAFKA_PRODUCER_OPTS := --bootstrap-server localhost:9092 --topic test-users --property parse.headers=true

# Format: header_key:header_value<TAB>payload (see KIP-798 / parse.headers)
define SEND_JSON
	docker exec -i -e TRACEPARENT="$(TRACEPARENT)" -e PAYLOAD='$(1)' kafka bash -lc 'printf "traceparent:%s\t%s\n" "$$TRACEPARENT" "$$PAYLOAD" | $(KAFKA_PRODUCER) $(KAFKA_PRODUCER_OPTS)'
endef

run_example:
	docker-compose up -d
	@make send_msg > /dev/null # this part blocks until the broker is up
	@echo "----"
	@echo "kafka is running"
	@echo "you can execute 'go run .' to see the application consuming messages"
	@echo "----"
	@while true; do make send_msg; sleep 5; done;

send_msg:
	$(call SEND_JSON,{"Content":"hello"})

send_msg_test_retry:
	$(call SEND_JSON,{"Content":"retry"})

send_msg_test_omit:
	$(call SEND_JSON,{"Content":"omit"})

send_msg_test_deadletter:
	$(call SEND_JSON,{"Content":"deadletter"})
