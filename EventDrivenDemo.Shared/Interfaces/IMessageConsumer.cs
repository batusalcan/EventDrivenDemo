namespace EventDrivenDemo.Shared.Interfaces;

public interface IMessageConsumer
{
    Task ConsumeAsync<T>(string topicName, Func<T, Task> handler, CancellationToken cancellationToken);
}
