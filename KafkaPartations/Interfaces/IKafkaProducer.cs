using POCKafkaWorker.Models;

namespace POCKafkaWorker.Interfaces;

public interface IKafkaProducer
{
    Task ProduceBatchesAsync(IEnumerable<DataModel> dataList, int batchSize);
}