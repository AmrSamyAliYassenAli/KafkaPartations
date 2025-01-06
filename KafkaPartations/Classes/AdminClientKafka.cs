using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaPartations.Interfaces;

public class AdminClientKafka : IAdminClientKafka
{
    public async Task Create(string? bootstrapServers, string? topicName, int numPartitions, short replicationFactor = 1)
    {
        using IAdminClient? adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();

        try
        {
            // Create the topic
            await adminClient.CreateTopicsAsync(topics: new List<TopicSpecification>
            {
                new TopicSpecification
                {
                    Name = topicName,
                    NumPartitions = numPartitions,
                    ReplicationFactor = replicationFactor,
                    // ReplicasAssignments = new Dictionary<int, List<int>>
                    // {

                    // },
                    // Configs = new Dictionary<string, string>
                    // {
                    //     { "cleanup.policy", "compact" },
                    //     { "retention.ms", "60000" }
                    // }
                }
            });

            Console.WriteLine(value: $"Topic '{topicName}' created with {numPartitions} partitions.");
        }
        catch (CreateTopicsException e)
        {
            Console.WriteLine(value: $"An error occurred creating topic {topicName}: {e.Results[0].Error.Reason}");
        }
    }

    public int GetNumberOfPartitions(string? bootstrapServers, string topic)
    {
        using IAdminClient? adminClient = new AdminClientBuilder(config: new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();

        try
        {
            Metadata? metadata = adminClient.GetMetadata(topic, timeout: TimeSpan.FromSeconds(10));
            TopicMetadata? topicMetadata = metadata.Topics.FirstOrDefault( predicate: t => t.Topic == topic);

            if (topicMetadata is null)
            {
                throw new Exception(message: $"Topic '{topic}' not found.");
            }

            return topicMetadata.Partitions.Count;
        }
        catch (KafkaException ex)
        {
            Console.WriteLine(value: $"An error occurred: {ex.Error.Reason}");
            throw;
        }
    }

    public async Task AddPartitionsAsync(string? bootstrapServers, string? topicName, int newPartitionCount)
    {
        try
        {
            using IAdminClient? adminClient = new AdminClientBuilder(config: new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();

            await adminClient.CreatePartitionsAsync(partitionsSpecifications: new List<PartitionsSpecification> { new ()
            {
                Topic = topicName,
                IncreaseTo = newPartitionCount
            }});

        }
        catch (CreatePartitionsException ex)
        {
            
            throw;
        }
    }
}