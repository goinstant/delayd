package main

import (
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

type MockAmqpConnection struct {
	mock.Mock
}

func (m *MockAmqpConnection) Close() (e error) {
	return nil
}

func (m *MockAmqpConnection) Channel() (c *amqp.Channel, e error) {
	return c, nil
}

func (m *MockAmqpChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (q amqp.Queue, e error) {
	return q, nil
}

func (m *MockAmqpChannel) Consume(name string, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (c <-chan amqp.Delivery, e error) {
	return c, e
}

func (m *MockAmqpChannel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	return nil
}

type MockAmqpChannel struct {
	mock.Mock
}

func TestClose(t *testing.T) {
	mockConnection := new(MockAmqpConnection)
	mockChannel := new(MockAmqpChannel)

	a := new(AmqpBase)
	a.connection = mockConnection
	a.channel = mockChannel

	a.Close()

	// mockConnection.AssertNumberOfCalls(t, "Close", 1)
}
