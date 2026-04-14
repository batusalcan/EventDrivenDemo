using Confluent.Kafka;
using EventDrivenDemo.InvoiceApi.Services;
using EventDrivenDemo.Shared.Models;
using System.Text;
using System.Text.Json;

namespace EventDrivenDemo.InvoiceApi.Messaging;

public class InvoiceKafkaConsumer : BackgroundService
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<InvoiceKafkaConsumer> _logger;
    private readonly InvoiceEventLogStore _eventLog;

    public InvoiceKafkaConsumer(IConfiguration configuration, ILogger<InvoiceKafkaConsumer> logger, InvoiceEventLogStore eventLog)
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
        var groupId = _configuration["Kafka:ConsumerGroupId"] ?? "invoice-consumer-group";

        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(topic);

        _logger.LogInformation("[InvoiceApi] Consumer started. Listening on topic '{Topic}'...", topic);

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
                    customerTier = Encoding.UTF8.GetString(tierHeader.GetValueBytes());

                var order = JsonSerializer.Deserialize<OrderCreatedEvent>(payload);
                if (order is null) continue;

                var invoiceNumber = $"INV-{order.OrderId.ToString()[..8].ToUpper()}";

                _logger.LogInformation(
                    "[InvoiceApi] Invoice generated | Invoice#: {InvoiceNumber} | OrderId: {OrderId} | Customer: {CustomerId} | Tier: {Tier} | Amount: {Amount:C}",
                    invoiceNumber, order.OrderId, order.CustomerId, customerTier, order.Amount);

                _eventLog.Add($"Invoice generated — {invoiceNumber} | Customer: {order.CustomerId} | Tier: {customerTier} | Amount: {order.Amount:C} | Items: {string.Join(", ", order.Items)}");
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[InvoiceApi] Error while consuming message.");
            }
        }

        consumer.Close();
        _logger.LogInformation("[InvoiceApi] Consumer stopped.");
    }
}
