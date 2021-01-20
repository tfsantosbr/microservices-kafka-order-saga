using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Orders.EmailService
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("E-mail Service Started");

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
                        var correlationIdHeader = result.Message.Headers.First(header => header.Key == "X-Correlation-ID");
                        var correlationId = Encoding.ASCII.GetString(correlationIdHeader.GetValueBytes());

                        Console.WriteLine("-- Message Received ---------------------------------");
                        Console.WriteLine($"Correlation Id: {correlationId}");
                        Console.WriteLine($"Key: {key}");
                        Console.WriteLine($"Value: {value}");
                        Console.WriteLine("Processing...");
                        System.Threading.Thread.Sleep(1000);
                        Console.WriteLine("E-mail sent");
                        Console.WriteLine("-----------------------------------------------------");

                        await ProduceEventEmailSent(correlationId, key, value);

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
                Console.WriteLine("E-mail Service Terminated");
            }
        }

        private static async Task ProduceEventEmailSent(string correlationId, string key, string value)
        {
            using var producer = CreateProducer();

            var message = CreateMessage(correlationId, key, value);
            var result = await producer.ProduceAsync("orders-initial-email-sent", message);
        }

        // Private Methods
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
                GroupId = "orders-email-service-group",
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

        private static Message<string, string> CreateMessage(string correlationId, string key, string value)
        {
            var message = new Message<string, string>
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
