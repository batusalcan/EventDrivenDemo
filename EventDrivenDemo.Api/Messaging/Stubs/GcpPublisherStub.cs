using EventDrivenDemo.Shared.Interfaces;
using EventDrivenDemo.Shared.Models;
using System.Text.Json;

namespace EventDrivenDemo.Api.Messaging.Stubs;

public class GcpPublisherStub : IMessagePublisher
{
    private readonly ILogger<GcpPublisherStub> _logger;

    public GcpPublisherStub(ILogger<GcpPublisherStub> logger)
    {
        _logger = logger;
    }

    public Task PublishAsync<T>(string topicName, T message, MessageHeaders? headers = null)
    {
        var payload = JsonSerializer.Serialize(message);
        var headersSummary = headers is not null && headers.Count > 0
            ? string.Join(", ", headers.Select(h => $"{h.Key}={h.Value}"))
            : "none";

        _logger.LogInformation(
            "[GCP Pub/Sub] (Stub) Would publish to topic '{Topic}' | Headers: [{Headers}] | Payload: {Payload}",
            topicName, headersSummary, payload);

        return Task.CompletedTask;
    }
}
