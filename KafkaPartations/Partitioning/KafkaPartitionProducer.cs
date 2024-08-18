using Confluent.Kafka;

namespace KafkaPartations;

public class KafkaPartitionProducer
{
    private readonly string _bootstrapServers;
    private readonly string _topic;

    public KafkaPartitionProducer(string bootstrapServers, string topic)
    {
        _bootstrapServers = bootstrapServers;
        _topic = topic;
    }
// disable create kafka topic from ProduceAsync only create topic from AdminClient
// server.properties => auto.create.topics.enable=false
    public async Task ProduceAsync(string message, int partition, Acks acks)
    {
        ProducerConfig config = new () 
        { 
            BootstrapServers = _bootstrapServers,
            Acks = acks
        };

        using (IProducer<Null, string> producer = new ProducerBuilder<Null, string>(config).Build())
        {
            try
            {
                DeliveryResult<Null,string> result = await producer.ProduceAsync(
                    new TopicPartition(_topic, new Partition(partition)),
                    new Message<Null, string> { Value = message }
                );
                
                Console.WriteLine($"Message '{message}' sent to partition {result.Partition} with offset {result.Offset}");
            }
            catch (ProduceException<Null, string> e)
            {
                Console.WriteLine($"Delivery failed: {e.Error.Reason}");
            }
        }
    }   
}