using System;
using System.Text.Json;
using Confluent.Kafka;

namespace Orders.Shared.Serializers
{
    public class KafkaDeserializer<T> : IDeserializer<T>
    {
        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            return isNull ? default(T) : JsonSerializer.Deserialize<T>(data);
        }
    }
}