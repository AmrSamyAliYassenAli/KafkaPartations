using POCKafkaWorker.Classes;
using POCKafkaWorker.Interfaces;

namespace KafkaPartations.Utilities;
public static class InjectionUtility
{
    public static IServiceCollection AddInjectionUtility(this IServiceCollection services)
        => services
        .AddSingleton<IAdminClientKafka, AdminClientKafka>()
        .AddSingleton<IKafkaProducer, KafkaProducer>()
        .AddSingleton<IKafkaConsumer, KafkaConsumer>();
}