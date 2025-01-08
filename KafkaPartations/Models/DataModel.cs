namespace POCKafkaWorker.Models;

public class DataModel
{
    public int Id { get; set; }
    public string? Name { get; set; }
    public string? Value { get; set; }

    public static List<DataModel> GetSeading(int count)
    {
        List<DataModel> data = new();

        for (int i = 1; i <= count; i++)
        {
            data.Add(new()
            {
                Id = i,
                Name = "Amr",
                Value = "Samy"
            });
        }   

        return data;
    }
}