using Confluent.Kafka;

namespace KafkaPartations;

public class KafkaPartitionConsumer 
{
    private readonly ConsumerConfig _config;
    private readonly string _topic;
    private readonly int _partition;

    public KafkaPartitionConsumer(string bootstrapServers, string groupId, string topic, int partition, PartitionAssignmentStrategy partitionAssignmentStrategy)
    {
        _config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            PartitionAssignmentStrategy = partitionAssignmentStrategy
        };
        _topic = topic;
        _partition = partition;
    }

    public async Task ConsumeMessagesAsync(CancellationToken cancellationToken)
    {
        using IConsumer<Ignore, string>? consumer = new ConsumerBuilder<Ignore, string>(_config).Build();
        consumer.Assign(new TopicPartition(_topic, new Partition(_partition)));

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                ConsumeResult<Ignore, string>? consumeResult  = consumer.Consume(cancellationToken);
                Console.WriteLine($"Consumed message '{consumeResult.Message.Value}' from: '{consumeResult.TopicPartitionOffset}'.");
                consumer.Commit();
            }
        }
        catch (OperationCanceledException)
        {
            consumer.Close();
        }
    }
}