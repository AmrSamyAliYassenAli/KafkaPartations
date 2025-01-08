using System.Reactive.Linq;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using POCKafkaWorker.Interfaces;
using POCKafkaWorker.Models;

namespace POCKafkaWorker.Classes;

public class KafkaProducer : IKafkaProducer
{
    private readonly string _topic;
    private readonly IAdminClientKafka _adminClient;
    private readonly ProducerConfig _producerConfig;
    public KafkaProducer(IAdminClientKafka adminClient, IOptions<KafkaOptions> options)
    {
        _adminClient = adminClient;
        _producerConfig = options?.Value?.ProducerConfig!;
        _topic = options?.Value?.Topic!;
    }

    public async Task ProduceBatchesAsync(IEnumerable<DataModel> dataList, int batchSize)
    {
        List<List<DataModel>> batches = dataList
            .Select(selector: (data, index) => new { data, index })
            .GroupBy(keySelector: x => x.index / batchSize)
            .Select(selector: g => g.Select(x => x.data).ToList())
            .ToList();

        int partitionCount = batches.Count;

// check if topic is created with the number of partations needed else create if it is created but with differnt number of partaions update it

        await _adminClient.CreateAsync(_producerConfig.BootstrapServers, _topic, partitionCount, 1);

        int maxDegreeOfParallelism = (Environment.ProcessorCount < partitionCount)? Environment.ProcessorCount : partitionCount;

        await Parallel.ForEachAsync(
            source: batches,
            parallelOptions: new ParallelOptions { MaxDegreeOfParallelism = maxDegreeOfParallelism },
            body: async (batch, token) =>
            {
                for (int i = 0; i < partitionCount; i++)
                {
                    if (token.IsCancellationRequested)
                        break;

                    try
                    {
                        await ProduceBatchAsync(batch, i); // Process the batch on the specified partition
                        Console.WriteLine($"Batch produced on partition {i}.");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error producing batch on partition {i}: {ex.Message}");
                    }
                }
            });

        Console.WriteLine("All batches produced.");
    }

    private async Task ProduceBatchAsync(List<DataModel> batch, int partition)
    {
        using (IProducer<Null, string> producer = new ProducerBuilder<Null, string>(_producerConfig).Build())
        {
            foreach (DataModel data in batch)
            {
                var message = new Message<Null, string> { Value = JsonSerializer.Serialize(data) };

                await producer.ProduceAsync(new TopicPartition(_topic, new Partition(partition)), message);
            }

            producer.Flush(TimeSpan.FromSeconds(10));
        }
    }

    #region 
    // disable create kafka topic from ProduceAsync only create topic from AdminClient
    // server.properties => auto.create.topics.enable=false

    // #Idempotent Producer: [default EnableIdempotence = true from Kafka V3] if Kafka Version is 2.8 or lower EnableIdempotence = false by default 
    // we should apply this configurations
    // The Producer can introduce duplicate Messages in Kafka due a message send to kafka and kafka commit this message and when it send ack Network Errors is happen and ack don't be reached
    // retry Mechanizm will resend this Message that is already reached to kafka and its ack is faild to reach what will dublicate this Message
    // Idempotent Producer will fix this issue

    // public async Task ProduceAsync(string message, int partition, Acks acks, bool enableIdempotence)
    // {
    //     ProducerConfig config = new()
    //     {
    //         BootstrapServers = _bootstrapServers,
    //         Acks = acks,
    //         EnableIdempotence = enableIdempotence,
    //         MessageTimeoutMs = 30000, // Total timeout for message delivery
    //         RetryBackoffMs = 100, // Wait time between retries
    //         MessageSendMaxRetries = 10, // Number of retries for failed sends
    //         // DeliveryTimeoutMs = 120000, // Optional: Total timeout including retries
    //     };

    //     using (IProducer<Null, string> producer = new ProducerBuilder<Null, string>(config).Build())
    //     {
    //         try
    //         {
    //             DeliveryResult<Null, string> result = await producer.ProduceAsync(
    //                 new TopicPartition(_topic, new Partition(partition)),
    //                 new Message<Null, string> { Value = message }
    //             );

    //             Console.WriteLine($"Message '{message}' sent to partition {result.Partition} with offset {result.Offset}");
    //         }
    //         catch (ProduceException<Null, string> e)
    //         {
    //             producer.Dispose();
    //             Console.WriteLine($"Delivery failed: {e.Error.Reason}");
    //         }
    //     }
    // }

    // public async Task ProduceBatchesAsync(string topic, List<string> messages, int batchSize = 10)
    // {
    //     List<List<string>>? messageBatches = SplitMessagesIntoBatches(messages, batchSize);

    //     ProducerConfig? config = new()
    //     {
    //         BootstrapServers = "192.168.20.91:30094",
    //         Acks = Acks.All, // Wait for all replicas to acknowledge
    //         LingerMs = 100,  // Time to wait before sending the batch, to allow more messages to accumulate
    //         BatchSize = 100000, // Maximum batch size in bytes
    //         QueueBufferingMaxMessages = 100000, // Maximum number of messages in the queue.
    //         EnableIdempotence = true     // Ensure exactly-once delivery (useful for multi-partition).
    //     };
    //     using (IProducer<string, string>? producer = new ProducerBuilder<string, string>(config).Build())
    //     {
    //         foreach (List<string>? batch in messageBatches)
    //         {
    //             try
    //             {
    //                 foreach (string message in batch)
    //                 {
    //                     int partitionKey = messageBatches.Count();  // Optionally implement custom partitioning

    //                     Message<string, string>? kafkaMessage = new()
    //                     {
    //                         Key = partitionKey.ToString(),
    //                         Value = message
    //                     };

    //                     await producer.ProduceAsync(topic, kafkaMessage);
    //                 }

    //                 // Flush after sending each batch
    //                 producer.Flush(TimeSpan.FromSeconds(10));
    //                 Console.WriteLine($"Batch of {batch.Count} messages sent.");
    //             }
    //             catch (ProduceException<Null, string> e)
    //             {
    //                 producer.Dispose();
    //                 Console.WriteLine($"Delivery failed: {e.Error.Reason}");
    //             }
    //         }
    //     }

    // }

    // private List<List<string>> SplitMessagesIntoBatches(List<string> messages, int batchSize)
    //     => messages
    //         .Select((message, index) => new { message, index })
    //         .GroupBy(x => x.index / batchSize)
    //         .Select(group => group.Select(x => x.message).ToList())
    //         .ToList();
    #endregion
}