using EventDrivenDemo.Shared.Models;

namespace EventDrivenDemo.Shared.Interfaces;

public interface IMessagePublisher
{
    Task PublishAsync<T>(string topicName, T message, MessageHeaders? headers = null);
}
