using POCKafkaWorker;
using POCKafkaWorker.Classes;
using POCKafkaWorker.Interfaces;
using POCKafkaWorker.Models;

HostApplicationBuilder? builder = Host.CreateApplicationBuilder(args);
IServiceCollection? services = builder.Services;
ConfigurationManager? configuration = builder.Configuration;

services.AddSingleton<IAdminClientKafka, AdminClientKafka>();
services.AddSingleton<IKafkaConsumer, KafkaConsumer>();
services.AddSingleton<IKafkaProducer, KafkaProducer>();

services
    .AddOptions<KafkaOptions>() 
    .Bind(config: builder.Configuration.GetSection("Kafka"))
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

services.AddHostedService<Worker>();

IHost? host = builder.Build();

host.Run();