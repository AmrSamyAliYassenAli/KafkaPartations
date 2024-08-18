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

// #Idempotent Producer: [default EnableIdempotence = true from Kafka V3] if Kafka Version is 2.8 or lower EnableIdempotence = false by default 
        // we should apply this configurations
        // The Producer can introduce duplicate Messages in Kafka due a message send to kafka and kafka commit this message and when it send ack Network Errors is happen and ack don't be reached
        // retry Mechanizm will resend this Message that is already reached to kafka and its ack is faild to reach what will dublicate this Message
        // Idempotent Producer will fix this issue
        
    public async Task ProduceAsync(string message, int partition, Acks acks, bool enableIdempotence)
    {
        ProducerConfig config = new () 
        { 
            BootstrapServers = _bootstrapServers,
            Acks = acks,
            EnableIdempotence = enableIdempotence,
            MessageTimeoutMs = 30000, // Total timeout for message delivery
            RetryBackoffMs = 100, // Wait time between retries
            MessageSendMaxRetries = 10, // Number of retries for failed sends
            // DeliveryTimeoutMs = 120000, // Optional: Total timeout including retries
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