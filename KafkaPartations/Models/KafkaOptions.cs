using Confluent.Kafka;

namespace POCKafkaWorker.Models;

public class KafkaOptions
{
    public required string? BootstrapServers { get; set; }
    public required ProducerConfig? ProducerConfig { get; set; }
    public required ConsumerConfig? ConsumerConfig { get; set; }
    public required string? Topic { get; set; }
}