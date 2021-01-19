using System;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;
using System.Linq;
using System.Text;

namespace Orders.LogService
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Log Service Started");

            var consumer = CreateConsumer();
            consumer.Subscribe("^orders-.*");
            var cancelationToken = ConfigureCancelationToken();

            try
            {
                while (true)
                {
                    try
                    {
                        var result = consumer.Consume(cancelationToken.Token);
                        var topic = result.Topic;
                        var key = result.Message.Key;
                        var value = result.Message.Value;
                        var correlationIdHeader = result.Message.Headers.First(header => header.Key == "X-Correlation-ID");
                        var correlationId = Encoding.ASCII.GetString(correlationIdHeader.GetValueBytes());

                        Console.WriteLine("-- Message Received ---------------------------------");
                        Console.WriteLine($"TOPIC: {topic.ToUpperInvariant()}");
                        Console.WriteLine($"Correlation Id: {correlationId}");
                        Console.WriteLine($"Key: {key}");
                        Console.WriteLine($"Value: {value}");
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
                Console.WriteLine("Log Service Terminated");
            }
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
            return new ConsumerBuilder<string, string>(GetConfig())
                .Build();
        }

        private static IEnumerable<KeyValuePair<string, string>> GetConfig()
        {
            var config = new ConsumerConfig
            {
                GroupId = "orders-log-service-group",
                BootstrapServers = "localhost:9091,localhost:9092,localhost:9093",
                AutoOffsetReset = AutoOffsetReset.Latest
            };

            return config;
        }
    }
}
