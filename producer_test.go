package kafka

import (
	"testing"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/stretchr/testify/assert"
)

type mockedSyncProducer struct {
	partition int32
	offset    int64
}

func (m mockedSyncProducer) Close() error {
	return nil
}

func (m mockedSyncProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	return m.partition, m.offset, nil
}

func (m mockedSyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	return nil
}

func Test_NewProducer_Should_Return_Error_When_No_Broker_Provided(t *testing.T) {
	// Act
	p, err := NewProducer([]string{})
	// Assert
	assert.Error(t, err)
	assert.Nil(t, p)
}

func Test_newProducerFromClient_Should_Return_Error_When_No_Client_Provided(t *testing.T) {
	// Act
	p, err := newProducerFromClient(nil)
	// Assert
	assert.Error(t, err)
	assert.Nil(t, p)
}

func TestSendMessage(t *testing.T) {
	type args struct {
		producer Producer
	}

	tests := []struct {
		name          string
		args          args
		wantPartition int32
		wantOffset    int64
		wantErr       bool
	}{
		{
			name: "failure : no producer",
			args: args{
				producer: producer{
					syncProducer: nil,
				},
			},
			wantPartition: 0,
			wantOffset:    0,
			wantErr:       true,
		},
		{
			name: "success",
			args: args{
				producer: producer{
					syncProducer: mockedSyncProducer{
						partition: 1,
						offset:    24,
					},
				},
			},
			wantPartition: 1,
			wantOffset:    24,
			wantErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			partition, offset, err := tt.args.producer.SendMessage([]byte{}, []byte{}, "")
			if (err != nil) != tt.wantErr {
				t.Errorf("SendMessage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if partition != tt.wantPartition {
				t.Errorf("SendMessage() partition = %v, want %v", partition, tt.wantPartition)
			}
			if offset != tt.wantOffset {
				t.Errorf("SendMessage() offset = %v, want %v", offset, tt.wantOffset)
			}
		})
	}
}

func Test_NewProducer_Return_Working_Producer(t *testing.T) {
	leaderBroker := sarama.NewMockBroker(t, 1)

	metadataResponse := new(sarama.MetadataResponse)
	metadataResponse.AddBroker(leaderBroker.Addr(), leaderBroker.BrokerID())
	metadataResponse.AddTopicPartition("topic-test", 0, leaderBroker.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	leaderBroker.Returns(metadataResponse)

	prodSuccess := new(sarama.ProduceResponse)
	prodSuccess.AddTopicPartition("topic-test", 0, sarama.ErrNoError)
	leaderBroker.Returns(prodSuccess)

	prod, err := NewProducer([]string{leaderBroker.Addr()})
	assert.Nil(t, err)

	_, _, err = prod.SendMessage([]byte("test-key"), []byte("test-message"), "topic-test")
	assert.Nil(t, err)
}

func Test_newProducerFromClient_Return_Working_Producer(t *testing.T) {
	leaderBroker := sarama.NewMockBroker(t, 1)

	metadataResponse := new(sarama.MetadataResponse)
	metadataResponse.AddBroker(leaderBroker.Addr(), leaderBroker.BrokerID())
	metadataResponse.AddTopicPartition("topic-test", 0, leaderBroker.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	leaderBroker.Returns(metadataResponse)

	prodSuccess := new(sarama.ProduceResponse)
	prodSuccess.AddTopicPartition("topic-test", 0, sarama.ErrNoError)
	leaderBroker.Returns(prodSuccess)

	client, err := cluster.NewClient([]string{leaderBroker.Addr()}, Config)
	if err != nil {
		assert.Fail(t, "We should be able to create a new client")
	}

	prod, err := newProducerFromClient(client)
	assert.Nil(t, err)

	_, _, err = prod.SendMessage([]byte("test-key"), []byte("test-message"), "topic-test")
	assert.Nil(t, err)
}
