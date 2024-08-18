﻿using Confluent.Kafka;
using KafkaPartations;

string? bootstrapServers = "192.168.20.91:30094";
string? topicName = "kafkaPartitionProducer";
string? groupId = "group1";
int numberOfPartitions;

Func<Task>? KafkaTopicHandlerCreation = async () =>
{
    AdminClientKafka kafkaTopicHandler = new AdminClientKafka();
    await kafkaTopicHandler.Create(bootstrapServers, topicName, 3);
    numberOfPartitions = kafkaTopicHandler.GetNumberOfPartitions(bootstrapServers, topicName);
};

Func<Task>? ProduceOnPartation = async () =>
{
    try
    {
        KafkaPartitionProducer kafkaPartitionProducer = new KafkaPartitionProducer(bootstrapServers, topicName);

        await kafkaPartitionProducer.ProduceAsync("Message1", 0);
        await kafkaPartitionProducer.ProduceAsync("ServiceNow Inc.", 1);
        await kafkaPartitionProducer.ProduceAsync("kafkaPartitionProducer", 2);
    }
    catch (Exception ex)
    {
        Console.WriteLine(ex.Message);
        throw;
    }
};

Func<Task>? KafkaPartitionConsumers = async () =>
{
    CancellationTokenSource cancellationTokenSource= new CancellationTokenSource();
    KafkaPartitionConsumer kafkaPartitionConsumer = new(bootstrapServers, groupId, topicName, partition: 1, PartitionAssignmentStrategy.CooperativeSticky);
    await kafkaPartitionConsumer.ConsumeMessagesAsync(cancellationTokenSource.Token);
};

await KafkaTopicHandlerCreation();

await ProduceOnPartation();

await KafkaPartitionConsumers();

// 