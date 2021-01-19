using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Orders.OrdersApp.Models;
using Orders.Shared.Serializers;

namespace Orders.OrdersApp
{
    class Program
    {
        static async Task Main(string[] args)
        {
            using var producer = CreateProducer();

            var order = new Order(105, 3, 5000m);
            var message = CreateMessage(order.Id.ToString(), order);
            var correlationIdHeader = message.Headers.First(header => header.Key == "X-Correlation-ID");
            var correlationId = Encoding.ASCII.GetString(correlationIdHeader.GetValueBytes());
            var result = await producer.ProduceAsync("orders-order-created", message);

            Console.WriteLine($"[Enviada] -> Correlation Id: {correlationId} | Key: {result.Key} | Message: {result.Message.Value}");
        }

        // Private Methods

        private static IProducer<string, Order> CreateProducer()
        {
            return new ProducerBuilder<string, Order>(GetProducerConfig())
                .SetValueSerializer(new KafkaSerializer<Order>())
                .Build();
        }

        private static Message<string, Order> CreateMessage(string key, Order value)
        {
            var message = new Message<string, Order>
            {
                Key = key,
                Value = value
            };

            message.Headers = new Headers
            {
                { "X-Correlation-ID", Encoding.ASCII.GetBytes(Guid.NewGuid().ToString()) }
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
