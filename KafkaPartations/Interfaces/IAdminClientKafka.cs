namespace POCKafkaWorker.Interfaces;

public interface IAdminClientKafka
{
    Task Create(string? bootstrapServers, string? topicName, int numPartitions, short replicationFactor = 1);

    int GetNumberOfPartitions(string? bootstrapServers, string topic);

    Task AddPartitionsAsync(string? bootstrapServers, string? topicName, int newPartitionCount);
}