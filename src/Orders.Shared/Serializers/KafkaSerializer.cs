using System.Text;
using System.Text.Json;
using Confluent.Kafka;

namespace Orders.Shared.Serializers
{
    public class KafkaSerializer<T> : ISerializer<T>
    {
        public byte[] Serialize(T data, SerializationContext context)
        {
            return Encoding.ASCII.GetBytes(JsonSerializer.Serialize(data));
        }
    }
}