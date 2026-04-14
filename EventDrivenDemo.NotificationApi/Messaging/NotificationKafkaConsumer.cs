using Confluent.Kafka;
using EventDrivenDemo.NotificationApi.Services;
using EventDrivenDemo.Shared.Models;
using System.Text.Json;

namespace EventDrivenDemo.NotificationApi.Messaging;

public class NotificationKafkaConsumer : BackgroundService
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<NotificationKafkaConsumer> _logger;
    private readonly NotificationEventLogStore _eventLog;

    public NotificationKafkaConsumer(IConfiguration configuration, ILogger<NotificationKafkaConsumer> logger, NotificationEventLogStore eventLog)
    {
        _configuration = configuration;
        _logger = logger;
        _eventLog = eventLog;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(() => ConsumeLoop(stoppingToken), stoppingToken);
    }

    private void ConsumeLoop(CancellationToken stoppingToken)
    {
        var bootstrapServers = _configuration["Kafka:BootstrapServers"] ?? "localhost:29092";
        var topic = _configuration["Kafka:TopicName"] ?? "order-events";
        var groupId = _configuration["Kafka:ConsumerGroupId"] ?? "notification-consumer-group";

        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(topic);

        _logger.LogInformation("[NotificationApi] Consumer started. Listening on topic '{Topic}'...", topic);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(stoppingToken);
                var payload = result.Message.Value;

                // Read CustomerTier header without deserializing the full payload
                var customerTier = "Standard";
                var tierHeader = result.Message.Headers.FirstOrDefault(h => h.Key == "CustomerTier");
                if (tierHeader is not null)
                    customerTier = System.Text.Encoding.UTF8.GetString(tierHeader.GetValueBytes());

                var order = JsonSerializer.Deserialize<OrderCreatedEvent>(payload);
                if (order is null) continue;

                var channel = customerTier == "VIP" ? "SMS + Email" : "Email";

                _logger.LogInformation(
                    "[NotificationApi] Notification sent | Channel: {Channel} | OrderId: {OrderId} | Customer: {CustomerId} | Tier: {Tier}",
                    channel, order.OrderId, order.CustomerId, customerTier);

                _eventLog.Add($"Notification sent via {channel} — OrderId: {order.OrderId} | Customer: {order.CustomerId} | Tier: {customerTier} | Amount: {order.Amount:C}");
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[NotificationApi] Error while consuming message.");
            }
        }

        consumer.Close();
        _logger.LogInformation("[NotificationApi] Consumer stopped.");
    }
}
