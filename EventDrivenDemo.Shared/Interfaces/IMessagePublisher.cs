namespace EventDrivenDemo.Shared.Interfaces;

public interface IMessagePublisher
{

    Task PublishAsync<T>(string topicName, T message);
}