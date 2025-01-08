using POCKafkaWorker.Models;

namespace KafkaPartations.Utilities;
public static class OptionsUtility
{
    public static IServiceCollection AddOptionsUtility(this IServiceCollection services, IConfiguration configuration)
    {
        services
        .AddOptions<KafkaOptions>()
        .Bind(config: configuration.GetSection("Kafka"))
        .Validate(
            validation: kafka => string.IsNullOrEmpty(kafka.BootstrapServers),
            failureMessage: "BootstrapServers cannot be null or empty")
        .Validate(
            validation: kafka => kafka.ProducerConfig is null,
            failureMessage: "ProducerConfig can't be null")
        .Validate(
            validation: kafka => kafka.ConsumerConfig is null,
            failureMessage: "ConsumerConfig can't be null")
        .Validate(
            validation: kafka => string.IsNullOrEmpty(kafka.Topic),
            failureMessage: "Topic can't be null")
        .ValidateOnStart();

        return services;
    }
}