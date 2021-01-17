using System;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;

namespace Orders.FraudService
{
    class Program
    {
        static void Main(string[] args)
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

                        Console.WriteLine($"Mensagem Received -> {key} | {value}");
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
                GroupId = "orders-fraud-service-group",
                BootstrapServers = "localhost:9091,localhost:9092,localhost:9093",
                AutoOffsetReset = AutoOffsetReset.Latest
            };

            return config;
        }
    }
}
