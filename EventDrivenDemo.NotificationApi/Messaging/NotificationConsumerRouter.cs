using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Confluent.Kafka;
using EventDrivenDemo.NotificationApi.Hubs;
using EventDrivenDemo.NotificationApi.Services;
using EventDrivenDemo.Shared.Enums;
using EventDrivenDemo.Shared.Models;
using EventDrivenDemo.Shared.Services;
using Microsoft.AspNetCore.SignalR;
using System.Text;
using System.Text.Json;

namespace EventDrivenDemo.NotificationApi.Messaging;

/// <summary>
/// Single BackgroundService that routes to the correct broker consumer at runtime.
/// When ActiveBrokerState changes (via POST /api/system/switch-broker), the current
/// consume loop is cancelled and restarted for the new broker — no restart required.
/// </summary>
public class NotificationConsumerRouter : BackgroundService
{
    private readonly ActiveBrokerState _brokerState;
    private readonly IConfiguration _configuration;
    private readonly ILogger<NotificationConsumerRouter> _logger;
    private readonly NotificationEventLogStore _eventLog;
    private readonly IHubContext<EventHub> _hubContext;

    public NotificationConsumerRouter(
        ActiveBrokerState brokerState,
        IConfiguration configuration,
        ILogger<NotificationConsumerRouter> logger,
        NotificationEventLogStore eventLog,
        IHubContext<EventHub> hubContext)
    {
        _brokerState = brokerState;
        _configuration = configuration;
        _logger = logger;
        _eventLog = eventLog;
        _hubContext = hubContext;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var currentBroker = _brokerState.Current;

            using var brokerCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

            _ = WatchBrokerChangeAsync(currentBroker, brokerCts, stoppingToken);

            _logger.LogInformation("[NotificationConsumerRouter] Starting consumer for broker: {Broker}", currentBroker);

            try
            {
                switch (currentBroker)
                {
                    case BrokerType.Kafka:
                        await RunKafkaConsumerAsync(brokerCts.Token);
                        break;
                    case BrokerType.Aws:
                        await RunSqsConsumerAsync(brokerCts.Token);
                        break;
                    case BrokerType.Gcp:
                        _logger.LogInformation("[NotificationConsumerRouter] GCP Pub/Sub not yet implemented. Waiting for broker change...");
                        await Task.Delay(Timeout.Infinite, brokerCts.Token);
                        break;
                }
            }
            catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("[NotificationConsumerRouter] Broker changed to {NewBroker}. Restarting consumer...", _brokerState.Current);
            }
        }
    }

    private async Task WatchBrokerChangeAsync(BrokerType initial, CancellationTokenSource cts, CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested && _brokerState.Current == initial)
            await Task.Delay(500, stoppingToken);

        if (!stoppingToken.IsCancellationRequested)
            cts.Cancel();
    }

    // ── Kafka ──────────────────────────────────────────────────────────────

    private void RunKafkaConsumer(CancellationToken token)
    {
        var bootstrapServers = _configuration["Kafka:BootstrapServers"] ?? "localhost:29092";
        var topic            = _configuration["Kafka:TopicName"]        ?? "order-events";
        var groupId          = _configuration["Kafka:ConsumerGroupId"]  ?? "notification-consumer-group";

        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId          = groupId,
            AutoOffsetReset  = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(topic);

        _logger.LogInformation("[NotificationApi/Kafka] Consumer started. Listening on topic '{Topic}'...", topic);

        while (!token.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(token);
                var payload = result.Message.Value;

                var customerTier = "Standard";
                var tierHeader = result.Message.Headers.FirstOrDefault(h => h.Key == "CustomerTier");
                if (tierHeader is not null)
                    customerTier = Encoding.UTF8.GetString(tierHeader.GetValueBytes());

                var order = JsonSerializer.Deserialize<OrderCreatedEvent>(payload);
                if (order is null) continue;

                var channel = customerTier == "VIP" ? "SMS + Email" : "Email";

                _logger.LogInformation(
                    "[NotificationApi/Kafka] Notification sent | Channel: {Channel} | OrderId: {OrderId} | Customer: {CustomerId} | Tier: {Tier}",
                    channel, order.OrderId, order.CustomerId, customerTier);

                var entry = $"[NotificationApi/Kafka] Notification sent via {channel} — OrderId: {order.OrderId} | Customer: {order.CustomerId} | Tier: {customerTier} | Amount: {order.Amount:C}";
                _eventLog.Add(entry);
                _hubContext.Clients.All.SendAsync("ReceiveEvent", entry, token);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex) { _logger.LogError(ex, "[NotificationApi/Kafka] Error consuming message."); }
        }

        consumer.Close();
        _logger.LogInformation("[NotificationApi/Kafka] Consumer stopped.");
    }

    private Task RunKafkaConsumerAsync(CancellationToken token)
        => Task.Run(() => RunKafkaConsumer(token), token);

    // ── AWS SQS ────────────────────────────────────────────────────────────

    private async Task RunSqsConsumerAsync(CancellationToken token)
    {
        var queueUrl  = _configuration["Aws:NotificationQueueUrl"] ?? throw new InvalidOperationException("Aws:NotificationQueueUrl is not configured.");
        var region    = _configuration["Aws:Region"]               ?? throw new InvalidOperationException("Aws:Region is not configured.");
        var accessKey = _configuration["Aws:AccessKeyId"]          ?? throw new InvalidOperationException("Aws:AccessKeyId is not configured.");
        var secretKey = _configuration["Aws:SecretAccessKey"]      ?? throw new InvalidOperationException("Aws:SecretAccessKey is not configured.");

        var credentials = new BasicAWSCredentials(accessKey, secretKey);
        using var sqsClient = new AmazonSQSClient(credentials, RegionEndpoint.GetBySystemName(region));

        _logger.LogInformation("[NotificationApi/AWS] SQS consumer started. Polling queue: {QueueUrl}", queueUrl);

        while (!token.IsCancellationRequested)
        {
            try
            {
                var response = await sqsClient.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl              = queueUrl,
                    MaxNumberOfMessages   = 10,
                    WaitTimeSeconds       = 20,
                    MessageAttributeNames = ["All"]
                }, token);

                foreach (var msg in response.Messages)
                    await ProcessSqsMessageAsync(sqsClient, queueUrl, msg, token);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[NotificationApi/AWS] Error polling SQS.");
                await Task.Delay(TimeSpan.FromSeconds(5), token);
            }
        }

        _logger.LogInformation("[NotificationApi/AWS] SQS consumer stopped.");
    }

    private async Task ProcessSqsMessageAsync(AmazonSQSClient sqsClient, string queueUrl, Amazon.SQS.Model.Message msg, CancellationToken token)
    {
        try
        {
            var order = UnwrapSnsEnvelope(msg.Body);
            if (order is null)
            {
                _logger.LogWarning("[NotificationApi/AWS] Could not deserialize message. Deleting to avoid poison. Body: {Body}", msg.Body);
                await DeleteSqsMessageAsync(sqsClient, queueUrl, msg.ReceiptHandle);
                return;
            }

            // CustomerTier is inside the SNS envelope JSON, not in SQS MessageAttributes.
            var customerTier = ExtractSnsAttribute(msg.Body, "CustomerTier") ?? "Standard";

            var channel = customerTier == "VIP" ? "SMS + Email" : "Email";

            _logger.LogInformation(
                "[NotificationApi/AWS] Notification sent | Channel: {Channel} | OrderId: {OrderId} | Customer: {CustomerId} | Tier: {Tier}",
                channel, order.OrderId, order.CustomerId, customerTier);

            var entry = $"[NotificationApi/AWS] Notification sent via {channel} — OrderId: {order.OrderId} | Customer: {order.CustomerId} | Tier: {customerTier} | Amount: {order.Amount:C}";
            _eventLog.Add(entry);
            await _hubContext.Clients.All.SendAsync("ReceiveEvent", entry, token);

            // CRITICAL: Must delete after processing. Without this, SQS re-delivers after Visibility Timeout.
            await DeleteSqsMessageAsync(sqsClient, queueUrl, msg.ReceiptHandle);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[NotificationApi/AWS] Failed to process message {MessageId}. Will be redelivered after Visibility Timeout.", msg.MessageId);
        }
    }

    private static async Task DeleteSqsMessageAsync(AmazonSQSClient sqsClient, string queueUrl, string receiptHandle)
        => await sqsClient.DeleteMessageAsync(new DeleteMessageRequest { QueueUrl = queueUrl, ReceiptHandle = receiptHandle });

    private static OrderCreatedEvent? UnwrapSnsEnvelope(string rawBody)
    {
        using var doc = JsonDocument.Parse(rawBody);
        var root = doc.RootElement;
        if (!root.TryGetProperty("Message", out var messageElement))
            return JsonSerializer.Deserialize<OrderCreatedEvent>(rawBody);
        var innerJson = messageElement.GetString();
        return string.IsNullOrWhiteSpace(innerJson) ? null : JsonSerializer.Deserialize<OrderCreatedEvent>(innerJson);
    }

    // SNS embeds message attributes inside the envelope JSON body under "MessageAttributes".
    // Structure: { "MessageAttributes": { "CustomerTier": { "Type": "String", "Value": "VIP" } } }
    private static string? ExtractSnsAttribute(string rawBody, string attributeName)
    {
        try
        {
            using var doc = JsonDocument.Parse(rawBody);
            var root = doc.RootElement;
            if (!root.TryGetProperty("MessageAttributes", out var attrs)) return null;
            if (!attrs.TryGetProperty(attributeName, out var attr)) return null;
            return attr.TryGetProperty("Value", out var value) ? value.GetString() : null;
        }
        catch { return null; }
    }
}
