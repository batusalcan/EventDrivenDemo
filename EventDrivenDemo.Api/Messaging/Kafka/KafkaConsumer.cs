using Confluent.Kafka;
using EventDrivenDemo.Api.Services;
using EventDrivenDemo.Shared.Models;
using System.Text.Json;

namespace EventDrivenDemo.Api.Messaging.Kafka;

public class KafkaConsumer : BackgroundService
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<KafkaConsumer> _logger;
    private readonly EventLogStore _eventLog;

    public KafkaConsumer(IConfiguration configuration, ILogger<KafkaConsumer> logger, EventLogStore eventLog)
    {
        _configuration = configuration;
        _logger = logger;
        _eventLog = eventLog;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Run blocking Kafka consume loop on a background thread
        return Task.Run(() => ConsumeLoop(stoppingToken), stoppingToken);
    }

    private void ConsumeLoop(CancellationToken stoppingToken)
    {
        var bootstrapServers = _configuration["Kafka:BootstrapServers"] ?? "localhost:29092";
        var topic = _configuration["Kafka:TopicName"] ?? "order-events";
        var groupId = _configuration["Kafka:ConsumerGroupId"] ?? "order-consumer-group";

        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(topic);

        _logger.LogInformation("[Kafka] Consumer started. Listening on topic '{Topic}'...", topic);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(stoppingToken);
                var payload = result.Message.Value;

                _logger.LogInformation(
                    "[Kafka] Received message | Topic: {Topic} | Partition: {Partition} | Offset: {Offset} | Payload: {Payload}",
                    result.Topic, result.Partition.Value, result.Offset.Value, payload);

                var order = JsonSerializer.Deserialize<OrderCreatedEvent>(payload);
                if (order is not null)
                {
                    _eventLog.Add($"[Kafka] OrderCreated received — OrderId: {order.OrderId}, Customer: {order.CustomerId}, Amount: {order.Amount:C}");
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[Kafka] Error while consuming message.");
            }
        }

        consumer.Close();
        _logger.LogInformation("[Kafka] Consumer stopped.");
    }
}
