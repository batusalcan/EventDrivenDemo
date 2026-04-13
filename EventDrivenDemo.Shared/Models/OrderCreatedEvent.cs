namespace EventDrivenDemo.Shared.Models;

public class OrderCreatedEvent
{
    public Guid OrderId { get; init; } = Guid.NewGuid();
    public string CustomerId { get; init; } = string.Empty;
    public decimal Amount { get; init; }
    public List<string> Items { get; init; } = new();
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
}
