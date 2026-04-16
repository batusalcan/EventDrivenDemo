using Amazon;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using EventDrivenDemo.Shared.Interfaces;
using EventDrivenDemo.Shared.Models;
using System.Text.Json;

namespace EventDrivenDemo.Api.Messaging.Aws;

public class AwsSnsPublisher : IMessagePublisher, IDisposable
{
    private readonly AmazonSimpleNotificationServiceClient _snsClient;
    private readonly ILogger<AwsSnsPublisher> _logger;
    private readonly string _topicArn;

    public AwsSnsPublisher(IConfiguration configuration, ILogger<AwsSnsPublisher> logger)
    {
        _logger = logger;

        var region     = configuration["Aws:Region"]          ?? throw new InvalidOperationException("Aws:Region is not configured.");
        var topicArn   = configuration["Aws:TopicArn"]        ?? throw new InvalidOperationException("Aws:TopicArn is not configured.");
        var accessKey  = configuration["Aws:AccessKeyId"]     ?? throw new InvalidOperationException("Aws:AccessKeyId is not configured.");
        var secretKey  = configuration["Aws:SecretAccessKey"] ?? throw new InvalidOperationException("Aws:SecretAccessKey is not configured.");

        _topicArn = topicArn;

        var credentials = new BasicAWSCredentials(accessKey, secretKey);
        _snsClient = new AmazonSimpleNotificationServiceClient(credentials, RegionEndpoint.GetBySystemName(region));
    }

    public async Task PublishAsync<T>(string topicName, T message, MessageHeaders? headers = null)
    {
        var payload = JsonSerializer.Serialize(message);

        var request = new PublishRequest
        {
            TopicArn = _topicArn,
            Message  = payload,
            MessageAttributes = BuildMessageAttributes(headers)
        };

        var response = await _snsClient.PublishAsync(request);

        var headersSummary = headers is not null && headers.Count > 0
            ? string.Join(", ", headers.Select(h => $"{h.Key}={h.Value}"))
            : "none";

        _logger.LogInformation(
            "[AWS SNS] Published to topic ARN '{TopicArn}' | MessageId: {MessageId} | Headers: [{Headers}] | Payload: {Payload}",
            _topicArn, response.MessageId, headersSummary, payload);
    }

    // Maps our generic MessageHeaders dict to SNS MessageAttribute objects.
    // SNS requires a DataType for each attribute — "String" covers all our header values.
    private static Dictionary<string, MessageAttributeValue> BuildMessageAttributes(MessageHeaders? headers)
    {
        var attributes = new Dictionary<string, MessageAttributeValue>();

        if (headers is null || headers.Count == 0)
            return attributes;

        foreach (var (key, value) in headers)
        {
            attributes[key] = new MessageAttributeValue
            {
                DataType    = "String",
                StringValue = value
            };
        }

        return attributes;
    }

    public void Dispose() => _snsClient.Dispose();
}
