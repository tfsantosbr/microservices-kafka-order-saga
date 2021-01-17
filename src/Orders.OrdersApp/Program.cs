using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Orders.OrdersApp
{
    class Program
    {
        static async Task Main(string[] args)
        {
            using var producer = CreateProducer();

            var message = CreateMessage(Guid.NewGuid(), "Order1;Product1;2;5.50");
            var result = await producer.ProduceAsync("orders-order-created", message);

            Console.WriteLine($"[Enviada] -> {result.Key} | {result.Message.Value}");
        }

        // Private Methods

        private static IProducer<string, string> CreateProducer()
        {
            return new ProducerBuilder<string, string>(GetConfig())
                .Build();
        }

        private static Message<string, string> CreateMessage(Guid key, string value)
        {
            var message = new Message<string, string>
            {
                Key = key.ToString(),
                Value = value
            };

            return message;
        }

        private static IEnumerable<KeyValuePair<string, string>> GetConfig()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9091,localhost:9092,localhost:9093"
            };

            return config;
        }
    }
}
