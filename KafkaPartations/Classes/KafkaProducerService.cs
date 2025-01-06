using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaPartations.Models;

namespace KafkaPartations.Partitioning;

public class KafkaProducerService
{
    private readonly IProducer<Null, string> _producer;
    private readonly string _topic;
    private readonly IAdminClient _adminClient;
    private readonly string _bootstrapServers;
    public KafkaProducerService(string bootstrapServers, string topic)
    {
        _bootstrapServers = bootstrapServers;
        var config = new ProducerConfig { BootstrapServers = bootstrapServers };
        _producer = new ProducerBuilder<Null, string>(config).Build();
        _topic = topic;

        var adminConfig = new AdminClientConfig { BootstrapServers = bootstrapServers };
        _adminClient = new AdminClientBuilder(adminConfig).Build();
    }

    public async Task CreatePartitionsAsync(int numberOfPartitions)
    {
        try
        {
            await _adminClient.CreatePartitionsAsync(new List<PartitionsSpecification>
            {
                new PartitionsSpecification
                {
                    Topic = _topic,
                    IncreaseTo = numberOfPartitions
                }
            });
            
            Console.WriteLine($"Created {numberOfPartitions} partitions for topic {_topic}.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error creating partitions: {ex.Message}");
        }
    }

    public async Task ProduceBatchesAsync(IEnumerable<DataModel> dataList, int batchSize)
    {
        List<List<DataModel>>? batches = dataList
            .Select(selector: (DataModel data, int index) => new { data, index })
            .GroupBy(keySelector: x => x.index / batchSize)
            .Select(selector: g => g.Select(x => x.data).ToList())
            .ToList();

        IObservable<List<DataModel>>? observableBatches = batches.ToObservable();

        await observableBatches
            .Select(selector: batch => Observable.FromAsync(() => ProduceBatchAsync(batch)))
            .Merge()
            .Do(
                onNext: _ => Console.WriteLine("Batch produced."),
                onError: ex => Console.WriteLine($"Error: {ex.Message}"),
                onCompleted: () => Console.WriteLine("All batches produced.")
            )
            .ToTask();
    }

    private async Task ProduceBatchAsync(List<DataModel> batch)
    {
        AdminClientKafka adminClientKafka= new AdminClientKafka();
        int partitionCount = adminClientKafka.GetNumberOfPartitions(_bootstrapServers, _topic);
        var random = new Random();

        foreach (var data in batch)
        {
            var partition = new Partition(random.Next(partitionCount));
            var message = new Message<Null, string> { Value = JsonSerializer.Serialize(data)};

            await _producer.ProduceAsync(new TopicPartition(_topic, partition), message);
        }

        _producer.Flush(TimeSpan.FromSeconds(10));
    }
}