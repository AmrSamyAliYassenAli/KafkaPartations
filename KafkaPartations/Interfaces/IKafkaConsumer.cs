namespace POCKafkaWorker.Interfaces;

public interface IKafkaConsumer
{
    void ConsumeMessagesAsync(CancellationToken cancellationToken);
}