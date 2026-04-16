using EventDrivenDemo.Api.Hubs;
using EventDrivenDemo.Api.Services;
using EventDrivenDemo.Shared.Interfaces;
using EventDrivenDemo.Shared.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;

namespace EventDrivenDemo.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class OrdersController : ControllerBase
{
    private readonly IMessagePublisher _publisher;
    private readonly ILogger<OrdersController> _logger;
    private readonly IConfiguration _configuration;
    private readonly EventLogStore _eventLog;
    private readonly IHubContext<EventHub> _hubContext;

    public OrdersController(
        IMessagePublisher publisher,
        ILogger<OrdersController> logger,
        IConfiguration configuration,
        EventLogStore eventLog,
        IHubContext<EventHub> hubContext)
    {
        _publisher = publisher;
        _logger = logger;
        _configuration = configuration;
        _eventLog = eventLog;
        _hubContext = hubContext;
    }

    [HttpPost]
    public async Task<IActionResult> PlaceOrder([FromBody] PlaceOrderRequest request)
    {
        var orderEvent = new OrderCreatedEvent
        {
            CustomerId = request.CustomerId,
            Amount = request.Amount,
            Items = request.Items
        };

        var customerTier = request.IsVip ? "VIP" : "Standard";
        var headers = MessageHeaders.For("CustomerTier", customerTier);
        var topic = _configuration["Kafka:TopicName"] ?? "order-events";

        _logger.LogInformation(
            "Placing order for customer '{CustomerId}' | Tier: {Tier} | Amount: {Amount}",
            orderEvent.CustomerId, customerTier, orderEvent.Amount);

        await _publisher.PublishAsync(topic, orderEvent, headers);

        // Log directly to SignalR so [OrderApi] always appears in the monitor
        // regardless of which broker is active. Previously this relied on the
        // Kafka consumer echoing the message back, which only worked in Kafka mode.
        var entry = $"[OrderApi] Order published — OrderId: {orderEvent.OrderId} | Customer: {orderEvent.CustomerId} | Tier: {customerTier} | Amount: {orderEvent.Amount:C}";
        _eventLog.Add(entry);
        await _hubContext.Clients.All.SendAsync("ReceiveEvent", entry);

        return Ok(new
        {
            orderEvent.OrderId,
            CustomerTier = customerTier,
            Message = "Order placed and event published."
        });
    }
}

public record PlaceOrderRequest(string CustomerId, decimal Amount, List<string> Items, bool IsVip = false);
