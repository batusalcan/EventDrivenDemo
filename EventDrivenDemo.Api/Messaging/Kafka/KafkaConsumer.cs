using Confluent.Kafka;
using EventDrivenDemo.Api.Hubs;
using EventDrivenDemo.Api.Services;
using EventDrivenDemo.Shared.Models;
using Microsoft.AspNetCore.SignalR;
using System.Text;
using System.Text.Json;

namespace EventDrivenDemo.Api.Messaging.Kafka;

public class KafkaConsumer : BackgroundService
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<KafkaConsumer> _logger;
    private readonly EventLogStore _eventLog;
    private readonly IHubContext<EventHub> _hubContext;

    public KafkaConsumer(
        IConfiguration configuration,
        ILogger<KafkaConsumer> logger,
        EventLogStore eventLog,
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

                var customerTier = "Standard";
                var tierHeader = result.Message.Headers.FirstOrDefault(h => h.Key == "CustomerTier");
                if (tierHeader is not null)
                    customerTier = Encoding.UTF8.GetString(tierHeader.GetValueBytes());

                _logger.LogInformation(
                    "[Kafka] Received message | Topic: {Topic} | Partition: {Partition} | Offset: {Offset} | Tier: {Tier} | Payload: {Payload}",
                    result.Topic, result.Partition.Value, result.Offset.Value, customerTier, payload);

                var order = JsonSerializer.Deserialize<OrderCreatedEvent>(payload);
                if (order is null) continue;

                var entry = $"[OrderApi] OrderCreated received — OrderId: {order.OrderId} | Customer: {order.CustomerId} | Tier: {customerTier} | Amount: {order.Amount:C}";

                _eventLog.Add(entry);
                _hubContext.Clients.All.SendAsync("ReceiveEvent", entry, stoppingToken);
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
