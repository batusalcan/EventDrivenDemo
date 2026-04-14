using EventDrivenDemo.Shared.Interfaces;
using EventDrivenDemo.Shared.Models;
using System.Text.Json;

namespace EventDrivenDemo.Api.Messaging.Stubs;

public class AwsPublisherStub : IMessagePublisher
{
    private readonly ILogger<AwsPublisherStub> _logger;

    public AwsPublisherStub(ILogger<AwsPublisherStub> logger)
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
            "[AWS SNS] (Stub) Would publish to topic '{Topic}' | Headers: [{Headers}] | Payload: {Payload}",
            topicName, headersSummary, payload);

        return Task.CompletedTask;
    }
}
