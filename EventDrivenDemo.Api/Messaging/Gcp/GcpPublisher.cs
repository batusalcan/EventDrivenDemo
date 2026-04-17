using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using EventDrivenDemo.Shared.Interfaces;
using EventDrivenDemo.Shared.Models;
using System.Text.Json;

namespace EventDrivenDemo.Api.Messaging.Gcp;

/// <summary>
/// GCP Pub/Sub publisher. Publishes messages to a topic using the Google Cloud Pub/Sub SDK.
/// Message attributes (e.g. CustomerTier) are attached as PubsubMessage.Attributes — these
/// are first-class metadata on the message, readable by subscribers without deserializing
/// the payload. This is the GCP equivalent of Kafka headers and SNS MessageAttributes.
/// </summary>
public class GcpPublisher : IMessagePublisher, IDisposable
{
    private readonly PublisherClient _publisherClient;
    private readonly ILogger<GcpPublisher> _logger;
    private readonly string _topicId;

    public GcpPublisher(IConfiguration configuration, ILogger<GcpPublisher> logger)
    {
        _logger = logger;

        var projectId       = configuration["Gcp:ProjectId"]       ?? throw new InvalidOperationException("Gcp:ProjectId is not configured.");
        _topicId            = configuration["Gcp:TopicId"]         ?? throw new InvalidOperationException("Gcp:TopicId is not configured.");
        var credentialsPath = configuration["Gcp:CredentialsPath"] ?? throw new InvalidOperationException("Gcp:CredentialsPath is not configured.");

        var topicName = TopicName.FromProjectTopic(projectId, _topicId);

        _publisherClient = new PublisherClientBuilder
        {
            TopicName       = topicName,
            CredentialsPath = credentialsPath.Trim()
        }.Build();
    }

    public async Task PublishAsync<T>(string topicName, T message, MessageHeaders? headers = null)
    {
        var payload = JsonSerializer.Serialize(message);

        var pubsubMessage = new PubsubMessage
        {
            Data = ByteString.CopyFromUtf8(payload)
        };

        // Attach headers as Pub/Sub message attributes.
        // Subscribers can filter on these without deserializing the payload.
        if (headers is not null)
            foreach (var (key, value) in headers)
                pubsubMessage.Attributes[key] = value;

        var messageId = await _publisherClient.PublishAsync(pubsubMessage);

        var headersSummary = headers is not null && headers.Count > 0
            ? string.Join(", ", headers.Select(h => $"{h.Key}={h.Value}"))
            : "none";

        _logger.LogInformation(
            "[GCP Pub/Sub] Published to topic '{Topic}' | MessageId: {MessageId} | Headers: [{Headers}] | Payload: {Payload}",
            _topicId, messageId, headersSummary, payload);
    }

    public void Dispose() => _publisherClient.ShutdownAsync(CancellationToken.None).GetAwaiter().GetResult();
}
