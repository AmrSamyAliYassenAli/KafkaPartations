using KafkaPartations.Utilities;
using POCKafkaWorker;

HostApplicationBuilder? builder = Host.CreateApplicationBuilder(args);
IServiceCollection? services = builder.Services;
ConfigurationManager? configuration = builder.Configuration;

services.AddInjectionUtility();

services.AddOptionsUtility(configuration);

services.AddHostedService<Worker>();

IHost? host = builder.Build();

host.Run();