using EventDrivenDemo.Shared.Interfaces;
using System.Text.Json;

namespace EventDrivenDemo.Api.Messaging.Stubs;

public class GcpPublisherStub : IMessagePublisher
{
    private readonly ILogger<GcpPublisherStub> _logger;

    public GcpPublisherStub(ILogger<GcpPublisherStub> logger)
    {
        _logger = logger;
    }

    public Task PublishAsync<T>(string topicName, T message)
    {
        var payload = JsonSerializer.Serialize(message);
        _logger.LogInformation("[GCP Pub/Sub] (Stub) Would publish to topic '{Topic}' | Payload: {Payload}", topicName, payload);
        return Task.CompletedTask;
    }
}
