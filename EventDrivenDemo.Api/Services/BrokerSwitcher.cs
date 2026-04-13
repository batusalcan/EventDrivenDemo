using EventDrivenDemo.Api.Messaging.Kafka;
using EventDrivenDemo.Api.Messaging.Stubs;
using EventDrivenDemo.Shared.Enums;
using EventDrivenDemo.Shared.Interfaces;

namespace EventDrivenDemo.Api.Services;

public class BrokerSwitcher : IMessagePublisher
{
    private IMessagePublisher _current;
    private BrokerType _activeBrokerType;
    private readonly object _lock = new();

    public BrokerSwitcher(IConfiguration configuration, ILoggerFactory loggerFactory)
    {
        var brokerTypeName = configuration["ActiveBroker"] ?? "Kafka";
        _activeBrokerType = Enum.Parse<BrokerType>(brokerTypeName, ignoreCase: true);
        _current = CreatePublisher(_activeBrokerType, configuration, loggerFactory);
    }

    public BrokerType ActiveBroker => _activeBrokerType;

    public void SwitchTo(BrokerType brokerType, IConfiguration configuration, ILoggerFactory loggerFactory)
    {
        lock (_lock)
        {
            if (_current is IDisposable disposable)
                disposable.Dispose();

            _activeBrokerType = brokerType;
            _current = CreatePublisher(brokerType, configuration, loggerFactory);
        }
    }

    public Task PublishAsync<T>(string topicName, T message)
    {
        lock (_lock)
        {
            return _current.PublishAsync(topicName, message);
        }
    }

    private static IMessagePublisher CreatePublisher(BrokerType brokerType, IConfiguration configuration, ILoggerFactory loggerFactory) =>
        brokerType switch
        {
            BrokerType.Kafka => new KafkaPublisher(configuration, loggerFactory.CreateLogger<KafkaPublisher>()),
            BrokerType.Aws   => new AwsPublisherStub(loggerFactory.CreateLogger<AwsPublisherStub>()),
            BrokerType.Gcp   => new GcpPublisherStub(loggerFactory.CreateLogger<GcpPublisherStub>()),
            _ => throw new ArgumentOutOfRangeException(nameof(brokerType))
        };
}
