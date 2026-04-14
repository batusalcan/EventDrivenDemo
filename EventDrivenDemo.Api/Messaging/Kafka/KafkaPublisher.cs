using Confluent.Kafka;
using EventDrivenDemo.Shared.Interfaces;
using EventDrivenDemo.Shared.Models;
using System.Text;
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

    public async Task PublishAsync<T>(string topicName, T message, MessageHeaders? headers = null)
    {
        var payload = JsonSerializer.Serialize(message);
        var key = Guid.NewGuid().ToString();

        var kafkaMessage = new Message<string, string> { Key = key, Value = payload };

        if (headers is not null)
        {
            kafkaMessage.Headers = new Headers();
            foreach (var (headerKey, headerValue) in headers)
                kafkaMessage.Headers.Add(headerKey, Encoding.UTF8.GetBytes(headerValue));
        }

        var result = await _producer.ProduceAsync(topicName, kafkaMessage);

        var headersSummary = headers is not null && headers.Count > 0
            ? string.Join(", ", headers.Select(h => $"{h.Key}={h.Value}"))
            : "none";

        _logger.LogInformation(
            "[Kafka] Published to topic '{Topic}' | Partition: {Partition} | Offset: {Offset} | Headers: [{Headers}] | Payload: {Payload}",
            result.Topic, result.Partition.Value, result.Offset.Value, headersSummary, payload);
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }
}
