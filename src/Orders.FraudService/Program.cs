using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

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
                        var key = result.Message.Key;
                        var value = result.Message.Value;

                        Console.WriteLine("-- Message Received ---------------------------------");
                        Console.WriteLine($"Key: {key}");
                        Console.WriteLine($"Value: {value}");
                        Console.WriteLine("Processing...");

                        System.Threading.Thread.Sleep(2000);
                        value = value + ";Status1";

                        Console.WriteLine("Order is valid");
                        Console.WriteLine("-----------------------------------------------------");

                        await ProduceEventOrderValid(key, value);
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

        private static async Task ProduceEventOrderValid(string key, string value)
        {
            using var producer = CreateProducer();

            var message = CreateMessage(key, value);
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

        private static IConsumer<string, string> CreateConsumer()
        {
            return new ConsumerBuilder<string, string>(GetConsumerConfig())
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

        private static IProducer<string, string> CreateProducer()
        {
            return new ProducerBuilder<string, string>(GetProducerConfig())
                .Build();
        }

        private static Message<string, string> CreateMessage(string key, string value)
        {
            var message = new Message<string, string>
            {
                Key = key,
                Value = value
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
