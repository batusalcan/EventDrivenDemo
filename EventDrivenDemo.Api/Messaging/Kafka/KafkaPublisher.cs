using Confluent.Kafka;
using EventDrivenDemo.Shared.Interfaces;
using System.Text.Json;

namespace EventDrivenDemo.Api.Messaging.Kafka;

public class KafkaPublisher : IMessagePublisher, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaPublisher> _logger;

    public KafkaPublisher(IConfiguration configuration, ILogger<KafkaPublisher> logger)
    {
        _logger = logger;

        var bootstrapServers = configuration["Kafka:BootstrapServers"]
            ?? throw new InvalidOperationException("Kafka:BootstrapServers is not configured.");

        var config = new ProducerConfig { BootstrapServers = bootstrapServers };
        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task PublishAsync<T>(string topicName, T message)
    {
        var payload = JsonSerializer.Serialize(message);
        var key = Guid.NewGuid().ToString();

        var kafkaMessage = new Message<string, string> { Key = key, Value = payload };

        var result = await _producer.ProduceAsync(topicName, kafkaMessage);

        _logger.LogInformation(
            "[Kafka] Published to topic '{Topic}' | Partition: {Partition} | Offset: {Offset} | Payload: {Payload}",
            result.Topic, result.Partition.Value, result.Offset.Value, payload);
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }
}
