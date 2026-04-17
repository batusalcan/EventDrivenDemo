using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Confluent.Kafka;
using EventDrivenDemo.InvoiceApi.Hubs;
using EventDrivenDemo.InvoiceApi.Services;
using EventDrivenDemo.Shared.Enums;
using EventDrivenDemo.Shared.Models;
using EventDrivenDemo.Shared.Services;
using Google.Cloud.PubSub.V1;
using Microsoft.AspNetCore.SignalR;
using System.Text.Json;


namespace EventDrivenDemo.InvoiceApi.Messaging;

/// <summary>
/// Single BackgroundService that routes to the correct broker consumer at runtime.
/// When ActiveBrokerState changes (via POST /api/system/switch-broker), the current
/// consume loop is cancelled and restarted for the new broker — no restart required.
/// </summary>
public class InvoiceConsumerRouter : BackgroundService
{
    private readonly ActiveBrokerState _brokerState;
    private readonly ConsumerControlState _controlState;
    private readonly IConfiguration _configuration;
    private readonly ILogger<InvoiceConsumerRouter> _logger;
    private readonly InvoiceEventLogStore _eventLog;
    private readonly IHubContext<EventHub> _hubContext;

    public InvoiceConsumerRouter(
        ActiveBrokerState brokerState,
        ConsumerControlState controlState,
        IConfiguration configuration,
        ILogger<InvoiceConsumerRouter> logger,
        InvoiceEventLogStore eventLog,
        IHubContext<EventHub> hubContext)
    {
        _brokerState = brokerState;
        _controlState = controlState;
        _configuration = configuration;
        _logger = logger;
        _eventLog = eventLog;
        _hubContext = hubContext;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            // Fault tolerance demo: if paused, disconnect from broker and wait.
            // Messages accumulate in the broker during this time.
            if (_controlState.IsPaused)
            {
                _logger.LogInformation("[InvoiceConsumerRouter] Consumer paused — disconnected from broker. Messages are queuing...");
                while (_controlState.IsPaused && !stoppingToken.IsCancellationRequested)
                    await Task.Delay(500, stoppingToken);

                if (stoppingToken.IsCancellationRequested) break;
                _logger.LogInformation("[InvoiceConsumerRouter] Consumer resumed — reconnecting and catching up on missed messages...");
            }

            var currentBroker = _brokerState.Current;

            // A linked token we cancel as soon as the broker changes OR consumer is paused.
            using var restartCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

            _ = WatchForRestartAsync(currentBroker, restartCts, stoppingToken);

            _logger.LogInformation("[InvoiceConsumerRouter] Starting consumer for broker: {Broker}", currentBroker);

            try
            {
                switch (currentBroker)
                {
                    case BrokerType.Kafka:
                        await RunKafkaConsumerAsync(restartCts.Token);
                        break;
                    case BrokerType.Aws:
                        await RunSqsConsumerAsync(restartCts.Token);
                        break;
                    case BrokerType.Gcp:
                        await RunGcpConsumerAsync(restartCts.Token);
                        break;
                }
            }
            catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("[InvoiceConsumerRouter] Consumer interrupted (broker change or pause). Restarting...");
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogError(ex, "[InvoiceConsumerRouter] Consumer error on broker {Broker}. Retrying in 10s...", currentBroker);
                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
            }
        }
    }

    // Polls every 500ms. Cancels the inner token when broker changes OR consumer is paused.
    private async Task WatchForRestartAsync(BrokerType initial, CancellationTokenSource cts, CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested && _brokerState.Current == initial && !_controlState.IsPaused)
            await Task.Delay(500, stoppingToken);

        if (!stoppingToken.IsCancellationRequested)
            cts.Cancel();
    }

    // ── Kafka ──────────────────────────────────────────────────────────────

    private void RunKafkaConsumer(CancellationToken token)
    {
        var bootstrapServers = _configuration["Kafka:BootstrapServers"] ?? "localhost:29092";
        var topic            = _configuration["Kafka:TopicName"]        ?? "order-events";
        var groupId          = _configuration["Kafka:ConsumerGroupId"]  ?? "invoice-consumer-group";

        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId          = groupId,
            AutoOffsetReset  = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(topic);

        _logger.LogInformation("[InvoiceApi/Kafka] Consumer started. Listening on topic '{Topic}'...", topic);

        while (!token.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(token);
                var payload = result.Message.Value;

                var customerTier = "Standard";
                var tierHeader = result.Message.Headers.FirstOrDefault(h => h.Key == "CustomerTier");
                if (tierHeader is not null)
                    customerTier = System.Text.Encoding.UTF8.GetString(tierHeader.GetValueBytes());

                var order = JsonSerializer.Deserialize<OrderCreatedEvent>(payload);
                if (order is null) continue;

                var invoiceNumber = $"INV-{order.OrderId.ToString()[..8].ToUpper()}";

                _logger.LogInformation(
                    "[InvoiceApi/Kafka] Invoice generated | Invoice#: {InvoiceNumber} | Customer: {CustomerId} | Tier: {Tier} | Amount: {Amount:C}",
                    invoiceNumber, order.CustomerId, customerTier, order.Amount);

                var entry = $"[InvoiceApi/Kafka] Invoice generated — {invoiceNumber} | Customer: {order.CustomerId} | Tier: {customerTier} | Amount: {order.Amount:C} | Items: {string.Join(", ", order.Items)}";
                _eventLog.Add(entry);
                _hubContext.Clients.All.SendAsync("ReceiveEvent", entry, token);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex) { _logger.LogError(ex, "[InvoiceApi/Kafka] Error consuming message."); }
        }

        consumer.Close();
        _logger.LogInformation("[InvoiceApi/Kafka] Consumer stopped.");
    }

    private Task RunKafkaConsumerAsync(CancellationToken token)
        => Task.Run(() => RunKafkaConsumer(token), token);

    // ── GCP Pub/Sub ────────────────────────────────────────────────────────
    // GCP uses an event-driven handler model: the SDK calls our handler for each message
    // and we return Reply.Ack (processed) or Reply.Nack (retry). This is architecturally
    // different from Kafka's offset-tracking loop and AWS's explicit DeleteMessage polling.

    private async Task RunGcpConsumerAsync(CancellationToken token)
    {
        var projectId       = _configuration["Gcp:ProjectId"]             ?? throw new InvalidOperationException("Gcp:ProjectId is not configured.");
        var subscriptionId  = _configuration["Gcp:InvoiceSubscriptionId"] ?? throw new InvalidOperationException("Gcp:InvoiceSubscriptionId is not configured.");
        var credentialsPath = _configuration["Gcp:CredentialsPath"]       ?? throw new InvalidOperationException("Gcp:CredentialsPath is not configured.");

        var subscriptionName = SubscriptionName.FromProjectSubscription(projectId, subscriptionId);

        var subscriberClient = await new SubscriberClientBuilder
        {
            SubscriptionName = subscriptionName,
            CredentialsPath  = credentialsPath.Trim()
        }.BuildAsync(token);

        _logger.LogInformation("[InvoiceApi/GCP] Pub/Sub subscriber started. Listening on subscription '{Sub}'...", subscriptionId);

        // When the cancellation token fires (broker change or pause), stop the subscriber.
        // StopAsync causes StartAsync below to complete its returned Task.
        using var _ = token.Register(() => subscriberClient.StopAsync(CancellationToken.None));

        // StartAsync is event-driven: the SDK calls our handler for each message.
        // Returning Reply.Ack removes the message from the subscription.
        // Returning Reply.Nack causes the broker to redeliver it after the ack deadline.
        await subscriberClient.StartAsync(async (message, ct) =>
        {
            try
            {
                var payload      = message.Data.ToStringUtf8();
                var customerTier = message.Attributes.TryGetValue("CustomerTier", out var tier) ? tier : "Standard";

                var order = JsonSerializer.Deserialize<OrderCreatedEvent>(payload);
                if (order is null)
                {
                    _logger.LogWarning("[InvoiceApi/GCP] Could not deserialize message {MessageId}. Acking to avoid poison.", message.MessageId);
                    return SubscriberClient.Reply.Ack;
                }

                var invoiceNumber = $"INV-{order.OrderId.ToString()[..8].ToUpper()}";

                _logger.LogInformation(
                    "[InvoiceApi/GCP] Invoice generated | Invoice#: {InvoiceNumber} | Customer: {CustomerId} | Tier: {Tier} | Amount: {Amount:C}",
                    invoiceNumber, order.CustomerId, customerTier, order.Amount);

                var entry = $"[InvoiceApi/GCP] Invoice generated — {invoiceNumber} | Customer: {order.CustomerId} | Tier: {customerTier} | Amount: {order.Amount:C} | Items: {string.Join(", ", order.Items)}";
                _eventLog.Add(entry);
                await _hubContext.Clients.All.SendAsync("ReceiveEvent", entry, ct);

                return SubscriberClient.Reply.Ack;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[InvoiceApi/GCP] Error processing message {MessageId}. Nacking for redelivery.", message.MessageId);
                return SubscriberClient.Reply.Nack;
            }
        });

        _logger.LogInformation("[InvoiceApi/GCP] Pub/Sub subscriber stopped.");
    }

    // ── AWS SQS ────────────────────────────────────────────────────────────

    private async Task RunSqsConsumerAsync(CancellationToken token)
    {
        var queueUrl  = _configuration["Aws:InvoiceQueueUrl"]  ?? throw new InvalidOperationException("Aws:InvoiceQueueUrl is not configured.");
        var region    = _configuration["Aws:Region"]           ?? throw new InvalidOperationException("Aws:Region is not configured.");
        var accessKey = _configuration["Aws:AccessKeyId"]      ?? throw new InvalidOperationException("Aws:AccessKeyId is not configured.");
        var secretKey = _configuration["Aws:SecretAccessKey"]  ?? throw new InvalidOperationException("Aws:SecretAccessKey is not configured.");

        var credentials = new BasicAWSCredentials(accessKey, secretKey);
        using var sqsClient = new AmazonSQSClient(credentials, RegionEndpoint.GetBySystemName(region));

        _logger.LogInformation("[InvoiceApi/AWS] SQS consumer started. Polling queue: {QueueUrl}", queueUrl);

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
                _logger.LogError(ex, "[InvoiceApi/AWS] Error polling SQS.");
                await Task.Delay(TimeSpan.FromSeconds(5), token);
            }
        }

        _logger.LogInformation("[InvoiceApi/AWS] SQS consumer stopped.");
    }

    private async Task ProcessSqsMessageAsync(AmazonSQSClient sqsClient, string queueUrl, Amazon.SQS.Model.Message msg, CancellationToken token)
    {
        try
        {
            var order = UnwrapSnsEnvelope(msg.Body);
            if (order is null)
            {
                _logger.LogWarning("[InvoiceApi/AWS] Could not deserialize message. Deleting to avoid poison. Body: {Body}", msg.Body);
                await DeleteSqsMessageAsync(sqsClient, queueUrl, msg.ReceiptHandle);
                return;
            }

            // CustomerTier is inside the SNS envelope JSON, not in SQS MessageAttributes.
            var customerTier = ExtractSnsAttribute(msg.Body, "CustomerTier") ?? "Standard";

            var invoiceNumber = $"INV-{order.OrderId.ToString()[..8].ToUpper()}";

            _logger.LogInformation(
                "[InvoiceApi/AWS] Invoice generated | Invoice#: {InvoiceNumber} | Customer: {CustomerId} | Tier: {Tier} | Amount: {Amount:C}",
                invoiceNumber, order.CustomerId, customerTier, order.Amount);

            var entry = $"[InvoiceApi/AWS] Invoice generated — {invoiceNumber} | Customer: {order.CustomerId} | Tier: {customerTier} | Amount: {order.Amount:C} | Items: {string.Join(", ", order.Items)}";
            _eventLog.Add(entry);
            await _hubContext.Clients.All.SendAsync("ReceiveEvent", entry, token);

            // CRITICAL: Must delete after processing. Without this, SQS re-delivers after Visibility Timeout.
            await DeleteSqsMessageAsync(sqsClient, queueUrl, msg.ReceiptHandle);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[InvoiceApi/AWS] Failed to process message {MessageId}. Will be redelivered after Visibility Timeout.", msg.MessageId);
            // Do NOT delete — let SQS redeliver for a natural retry.
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
