using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Orders.FraudService.Models;
using Orders.Shared.Serializers;

namespace Orders.FraudService
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Fraud Service Started");

            var consumer = CreateConsumer();
            consumer.Subscribe("orders-order-created");
            var cancelationToken = ConfigureCancelationToken();

            try
            {
                while (true)
                {
                    try
                    {
                        var result = consumer.Consume(cancelationToken.Token);
                        var correlationIdHeader = result.Message.Headers.First(header => header.Key == "X-Correlation-ID");
                        var correlationId = Encoding.ASCII.GetString(correlationIdHeader.GetValueBytes());
                        var key = result.Message.Key;
                        var order = result.Message.Value;

                        Console.WriteLine("-- Message Received ---------------------------------");
                        Console.WriteLine($"Correlation Id: {correlationId}");
                        Console.WriteLine($"Key: {key}");
                        Console.WriteLine($"Value: {order}");
                        Console.WriteLine("Processing...");

                        Thread.Sleep(5000);

                        if (order.Price <= 10000)
                        {
                            order.Validate();
                            Console.WriteLine("Order is valid");
                            await ProduceEventOrderValid(correlationId, key, order);
                        }
                        else
                        {
                            Console.WriteLine("CAUTION: Fraud detection in order");
                            await ProduceEventFraudDetected(correlationId, key, order);
                        }

                        Console.WriteLine("-----------------------------------------------------");

                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occured: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                consumer.Close();
                Console.WriteLine("Fraud Service Terminated");
            }
        }

        // Private Methods

        private static async Task ProduceEventFraudDetected(string correlationId, string key, Order order)
        {
            using var producer = CreateProducer();

            var message = CreateMessage(correlationId, key, order);
            var result = await producer.ProduceAsync("orders-fraud-detected", message);
        }

        private static async Task ProduceEventOrderValid(string correlationId, string key, Order order)
        {
            using var producer = CreateProducer();

            var message = CreateMessage(correlationId, key, order);
            var result = await producer.ProduceAsync("orders-order-validated", message);
        }

        private static CancellationTokenSource ConfigureCancelationToken()
        {
            var cancelationToken = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cancelationToken.Cancel();
            };
            return cancelationToken;
        }

        private static IConsumer<string, Order> CreateConsumer()
        {
            return new ConsumerBuilder<string, Order>(GetConsumerConfig())
                .SetValueDeserializer(new KafkaDeserializer<Order>())
                .Build();
        }

        private static IEnumerable<KeyValuePair<string, string>> GetConsumerConfig()
        {
            var config = new ConsumerConfig
            {
                GroupId = "orders-fraud-service-group",
                BootstrapServers = "localhost:9091,localhost:9092,localhost:9093",
                AutoOffsetReset = AutoOffsetReset.Latest
            };

            return config;
        }

        private static IProducer<string, Order> CreateProducer()
        {
            return new ProducerBuilder<string, Order>(GetProducerConfig())
                .SetValueSerializer(new KafkaSerializer<Order>())
                .Build();
        }

        private static Message<string, Order> CreateMessage(string correlationId, string key, Order value)
        {
            var message = new Message<string, Order>
            {
                Key = key,
                Value = value
            };

            message.Headers = new Headers
            {
                { "X-Correlation-ID", Encoding.ASCII.GetBytes(correlationId) }
            };

            return message;
        }

        private static IEnumerable<KeyValuePair<string, string>> GetProducerConfig()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9091,localhost:9092,localhost:9093"
            };

            return config;
        }
    }
}
