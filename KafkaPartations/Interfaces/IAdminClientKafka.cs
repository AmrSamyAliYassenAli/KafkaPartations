namespace POCKafkaWorker.Interfaces;

public interface IAdminClientKafka
{
    Task CreateAsync(string? topicName, int numPartitions, short replicationFactor = 1);

    int GetNumberOfPartitions(string topic);

    Task EnsureTopicPartitionCountAsync(string topicName, int numberOfPartitions);
}