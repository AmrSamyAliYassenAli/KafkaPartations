using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Options;
using POCKafkaWorker.Interfaces;
using POCKafkaWorker.Models;

namespace POCKafkaWorker.Classes;

public class AdminClientKafka : IAdminClientKafka
{
    private readonly string _bootstrapServers;
    public AdminClientKafka(IOptions<KafkaOptions> options)
    {
        _bootstrapServers = options.Value.BootstrapServers!;
    }

    public async Task CreateAsync(string? topicName, int numPartitions, short replicationFactor = 1)
    {
        using IAdminClient? adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _bootstrapServers }).Build();

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
            throw;
        }
    }

    public int GetNumberOfPartitions(string topic)
    {
        using IAdminClient? adminClient = new AdminClientBuilder(config: new AdminClientConfig { BootstrapServers = _bootstrapServers }).Build();

        try
        {
            Metadata? metadata = adminClient.GetMetadata(topic, timeout: TimeSpan.FromSeconds(10));
            TopicMetadata? topicMetadata = metadata.Topics.FirstOrDefault(predicate: t => t.Topic == topic);

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

    public async Task EnsureTopicPartitionCountAsync(string topicName, int numberOfPartitions)
    {
        using IAdminClient adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _bootstrapServers }).Build();

        try
        {
            // Get metadata for the topic
            Metadata? metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));

            // Case 1: Topic is not created
            if (!metadata.Topics.Exists(t => t.Topic == topicName))
            {
                await CreateTopicAsync(topicName, numberOfPartitions);
            }
            else
            {
                TopicMetadata? topicMetadata = metadata.Topics.First(t => t.Topic == topicName);
                int currentPartitionCount = topicMetadata.Partitions.Count;

                // Case 2: Created with different number of partitions
                if (currentPartitionCount != numberOfPartitions)
                {
                    await ModifyTopicPartitionsAsync(topicName, numberOfPartitions);
                }
                // Case 3: Created with the needed number of partitions
                else
                {
                    Console.WriteLine($"Topic '{topicName}' already has {numberOfPartitions} partitions.");
                }
            }
        }
        catch (KafkaException e)
        {
            Console.WriteLine($"An error occurred: {e.Message}");
        }
    }

    private async Task CreateTopicAsync(string topicName, int numberOfPartitions)
    {
        TopicSpecification? topicSpecifications = new ()
        {
            Name = topicName,
            NumPartitions = numberOfPartitions,
            ReplicationFactor = 1 // Change if needed
        };
        using IAdminClient? adminClient = new AdminClientBuilder(config: new AdminClientConfig { BootstrapServers = _bootstrapServers }).Build();
        await adminClient.CreateTopicsAsync(new[] { topicSpecifications });
        Console.WriteLine($"Created topic '{topicName}' with {numberOfPartitions} partitions.");
    }

    private async Task ModifyTopicPartitionsAsync(string? topicName, int newPartitionCount)
    {
        try
        {
            using IAdminClient? adminClient = new AdminClientBuilder(config: new AdminClientConfig { BootstrapServers = _bootstrapServers }).Build();

            await adminClient.CreatePartitionsAsync(partitionsSpecifications: new List<PartitionsSpecification> { new ()
            {
                Topic = topicName,
                IncreaseTo = newPartitionCount
            }});

        }
        catch (CreatePartitionsException ex)
        {
            // log  
            throw;
        }
    }

    // private async Task ModifyTopicPartitionsAsync(IAdminClient adminClient, string topicName, int numberOfPartitions)
    // {
    //     var partitionsSpecifications = new List<PartitionMetadata>();
    //     for (int i = 0; i < numberOfPartitions; i++)
    //     {
    //         partitionsSpecifications.Add(new PartitionMetadata { });
    //     }

    //     await adminClient.CreatePartitionsAsync(new Dictionary<string, int>
    //     {
    //         { topicName, numberOfPartitions }
    //     });

    //     Console.WriteLine($"Modified topic '{topicName}' to have {numberOfPartitions} partitions.");
    // }

}