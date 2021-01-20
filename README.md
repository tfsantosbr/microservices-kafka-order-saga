# Microservices Kafka Order Saga

Exemplo de implementação de uma SAGA de Pedido em um e-commerce

## Tecnologias utilizadas

- NET Core
- Kafka

## Tópicos

```bash
kafka-topics --create --bootstrap-server kafka1:19091 --replication-factor 3 --partitions 3 --topic orders-order-created
kafka-topics --create --bootstrap-server kafka1:19091 --replication-factor 3 --partitions 3 --topic orders-order-validated
kafka-topics --create --bootstrap-server kafka1:19091 --replication-factor 3 --partitions 3 --topic orders-initial-email-sent
kafka-topics --create --bootstrap-server kafka1:19091 --replication-factor 3 --partitions 3 --topic orders-fraud-detected

```

## Estratégia Fire And Forget

EnableDeliveryReports = false
producer.Flush();

## Links

- [Formação Apacha Kafka](https://cursos.alura.com.br/formacao-kafka)
