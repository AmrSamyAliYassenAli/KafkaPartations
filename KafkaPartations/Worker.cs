using POCKafkaWorker.Interfaces;
using POCKafkaWorker.Models;

namespace POCKafkaWorker;
public class Worker : BackgroundService
{
    private readonly IKafkaProducer _kafkaProducer;
    private readonly IKafkaConsumer _kafkaConsumer;
    public Worker(IKafkaProducer kafkaProducer, IKafkaConsumer kafkaConsumer)
    {
        _kafkaProducer = kafkaProducer;
        _kafkaConsumer = kafkaConsumer;
    }

    

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            int seadCount = 100;
            int batchSize = 10;

            // Generate List of data 
            List<DataModel> data = DataModel.GetSeading(seadCount);

            // Run the producer in a separate thread
            Task? producerTask = Task.Run(async () =>
            {
                await _kafkaProducer.ProduceBatchesAsync(data, batchSize);
            }, stoppingToken);

            // Run the consumer in a separate thread
            // Task? consumerTask = Task.Run(() =>
            // {
            //     _kafkaConsumer.ConsumeMessagesAsync(stoppingToken);
            // }, stoppingToken);

            // Wait for both tasks to complete
            await Task.WhenAll(producerTask);
            // , consumerTask);

            // Optionally, you can handle cancellation or other task completion logic here
            Console.WriteLine("Producer and Consumer tasks have completed.");
        }
    }
}