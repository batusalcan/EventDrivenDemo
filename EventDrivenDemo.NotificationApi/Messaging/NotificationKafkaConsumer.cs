using Confluent.Kafka;
using EventDrivenDemo.NotificationApi.Hubs;
using EventDrivenDemo.NotificationApi.Services;
using EventDrivenDemo.Shared.Models;
using Microsoft.AspNetCore.SignalR;
using System.Text;
using System.Text.Json;

namespace EventDrivenDemo.NotificationApi.Messaging;

public class NotificationKafkaConsumer : BackgroundService
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<NotificationKafkaConsumer> _logger;
    private readonly NotificationEventLogStore _eventLog;
    private readonly IHubContext<EventHub> _hubContext;

    public NotificationKafkaConsumer(
        IConfiguration configuration,
        ILogger<NotificationKafkaConsumer> logger,
        NotificationEventLogStore eventLog,
        IHubContext<EventHub> hubContext)
    {
        _configuration = configuration;
        _logger = logger;
        _eventLog = eventLog;
        _hubContext = hubContext;
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

                var customerTier = "Standard";
                var tierHeader = result.Message.Headers.FirstOrDefault(h => h.Key == "CustomerTier");
                if (tierHeader is not null)
                    customerTier = Encoding.UTF8.GetString(tierHeader.GetValueBytes());

                var order = JsonSerializer.Deserialize<OrderCreatedEvent>(payload);
                if (order is null) continue;

                var channel = customerTier == "VIP" ? "SMS + Email" : "Email";

                _logger.LogInformation(
                    "[NotificationApi] Notification sent | Channel: {Channel} | OrderId: {OrderId} | Customer: {CustomerId} | Tier: {Tier}",
                    channel, order.OrderId, order.CustomerId, customerTier);

                var entry = $"[NotificationApi] Notification sent via {channel} — OrderId: {order.OrderId} | Customer: {order.CustomerId} | Tier: {customerTier} | Amount: {order.Amount:C}";

                _eventLog.Add(entry);
                _hubContext.Clients.All.SendAsync("ReceiveEvent", entry, stoppingToken);
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
