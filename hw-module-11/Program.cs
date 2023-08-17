using Confluent.Kafka;
using Confluent.Kafka.Admin;

// See https://aka.ms/new-console-template for more information
Console.WriteLine("Hello, World!");

const string servers = "localhost:9092";
var topics = new[] { "topic1", "topic2" };

Console.WriteLine("Recreate topics");

var adminClientConfig = new AdminClientConfig
{
    BootstrapServers = servers
};

using (var adminClient = new AdminClientBuilder(adminClientConfig).Build())
{
    try
    {
        await adminClient.DeleteTopicsAsync(topics);
    }
    catch (Exception)
    {
        // skip
    }

    var specs = topics
        .Select(topic => new TopicSpecification
        {
            Name = topic,
            NumPartitions = 1,
            ReplicationFactor = 1
        })
        .ToArray();

    while (true)
    {
        try
        {
            await adminClient.CreateTopicsAsync(specs, new CreateTopicsOptions
            {
                ValidateOnly = true
            });

            break;
        }
        catch (KafkaException ex)
        {
            if (ex.Error.Code != ErrorCode.TopicAlreadyExists)
            {
                throw;
            }

            await Task.Delay(100);
        }
    }

    await adminClient.CreateTopicsAsync(specs);
}

Console.WriteLine("Start consumer loop");

var cts = new CancellationTokenSource();
Task consumerTask = Task.Run(() =>
{
    using var consumer = new ConsumerBuilder<int, int>(new ConsumerConfig
    {
        BootstrapServers = servers,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        GroupId = "hw-module-11-group",
        IsolationLevel = IsolationLevel.ReadCommitted
    })
    .SetKeyDeserializer(Deserializers.Int32)
    .SetValueDeserializer(Deserializers.Int32)
    .Build();

    consumer.Subscribe(topics);

    while (!cts.Token.IsCancellationRequested)
    {
        var cr = consumer.Consume(1000);
        if (cr == null)
        {
            continue;
        }
        Console.WriteLine($"[Consumer]: {cr.Topic}({cr.Partition.Value}:{cr.Offset.Value}) Key: {cr.Message.Key} Value: {cr.Message.Value}");
    }

    consumer.Close();
}, cts.Token);

Console.WriteLine("Start producer");
using var producer = new ProducerBuilder<int, int>(
    new ProducerConfig
    {
        BootstrapServers = servers,
        TransactionalId = "hw-module-11-producer-id"
    })
    .SetKeySerializer(Serializers.Int32)
    .SetValueSerializer(Serializers.Int32)
    .Build();

producer.InitTransactions(TimeSpan.FromSeconds(30));

producer.BeginTransaction();
for (int i = 2; i < 7; i++)
{
    _ = producer.ProduceAsync("topic1", new Message<int, int> { Key = i, Value = 100 + i });
    _ = producer.ProduceAsync("topic2", new Message<int, int> { Key = i, Value = 200 + i });
}
producer.CommitTransaction();

producer.BeginTransaction();
for (int i = 7; i < 9; i++)
{
    _ = producer.ProduceAsync("topic1", new Message<int, int> { Key = i, Value = 1100 + i });
    _ = producer.ProduceAsync("topic2", new Message<int, int> { Key = i, Value = 2200 + i });
}
producer.AbortTransaction();

producer.Dispose();

Console.WriteLine("Close producer");


Console.ReadKey(true);
cts.Cancel();
consumerTask.Wait();