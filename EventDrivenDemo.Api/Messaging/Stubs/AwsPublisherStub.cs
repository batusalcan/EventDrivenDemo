using EventDrivenDemo.Shared.Interfaces;
using System.Text.Json;

namespace EventDrivenDemo.Api.Messaging.Stubs;

public class AwsPublisherStub : IMessagePublisher
{
    private readonly ILogger<AwsPublisherStub> _logger;

    public AwsPublisherStub(ILogger<AwsPublisherStub> logger)
    {
        _logger = logger;
    }

    public Task PublishAsync<T>(string topicName, T message)
    {
        var payload = JsonSerializer.Serialize(message);
        _logger.LogInformation("[AWS SNS] (Stub) Would publish to topic '{Topic}' | Payload: {Payload}", topicName, payload);
        return Task.CompletedTask;
    }
}
