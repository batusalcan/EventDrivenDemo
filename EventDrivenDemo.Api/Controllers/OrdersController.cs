using EventDrivenDemo.Shared.Interfaces;
using EventDrivenDemo.Shared.Models;
using Microsoft.AspNetCore.Mvc;

namespace EventDrivenDemo.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class OrdersController : ControllerBase
{
    private readonly IMessagePublisher _publisher;
    private readonly ILogger<OrdersController> _logger;
    private readonly IConfiguration _configuration;

    public OrdersController(IMessagePublisher publisher, ILogger<OrdersController> logger, IConfiguration configuration)
    {
        _publisher = publisher;
        _logger = logger;
        _configuration = configuration;
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

        return Ok(new
        {
            orderEvent.OrderId,
            CustomerTier = customerTier,
            Message = "Order placed and event published."
        });
    }
}

public record PlaceOrderRequest(string CustomerId, decimal Amount, List<string> Items, bool IsVip = false);
