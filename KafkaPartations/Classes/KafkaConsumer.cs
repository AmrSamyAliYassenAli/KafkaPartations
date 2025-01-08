using Confluent.Kafka;
using Microsoft.Extensions.Options;
using POCKafkaWorker.Interfaces;
using POCKafkaWorker.Models;

namespace POCKafkaWorker.Classes;

public class KafkaConsumer : IKafkaConsumer
{
    private readonly ConsumerConfig _config;
    private readonly IAdminClientKafka _adminClient;
    private readonly string _topic;

    public KafkaConsumer(IOptions<KafkaOptions> options, IAdminClientKafka adminClient)
    {
        _config = options?.Value?.ConsumerConfig!;
        _topic = options?.Value?.Topic!;
        _adminClient = adminClient;
    }

    public void ConsumeMessagesAsync(CancellationToken cancellationToken)
    {
        int partitions = _adminClient.GetNumberOfPartitions(topic: _topic);

        List<TopicPartition>? partitionList = Enumerable
                                                .Range(start: 0, count: partitions)
                                                .Select(selector: partition => new TopicPartition(topic: _topic, partition: new Partition(partition)))
                                                .ToList();

        Parallel.ForEach(partitionList, (partition, state) =>
        {
            using IConsumer<Ignore, string> consumer = new ConsumerBuilder<Ignore, string>(_config).Build();
            consumer.Assign(partition);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    ConsumeResult<Ignore, string> consumeResult = consumer.Consume(cancellationToken);
                    Console.WriteLine(value: $"Consumed partition{partition} message '{consumeResult.Message.Value}' from: '{consumeResult.TopicPartitionOffset}'.");
                    consumer.Commit();
                }
            }
            catch (OperationCanceledException)
            {
                consumer.Close();
            }
        });
    }
}