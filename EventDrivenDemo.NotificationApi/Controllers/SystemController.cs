using EventDrivenDemo.Shared.Enums;
using EventDrivenDemo.Shared.Services;
using Microsoft.AspNetCore.Mvc;

namespace EventDrivenDemo.NotificationApi.Controllers;

[ApiController]
[Route("api/[controller]")]
public class SystemController : ControllerBase
{
    private readonly ActiveBrokerState _brokerState;
    private readonly ILogger<SystemController> _logger;

    public SystemController(ActiveBrokerState brokerState, ILogger<SystemController> logger)
    {
        _brokerState = brokerState;
        _logger = logger;
    }

    [HttpGet("active-broker")]
    public IActionResult GetActiveBroker()
    {
        return Ok(new { ActiveBroker = _brokerState.Current.ToString() });
    }

    [HttpPost("switch-broker")]
    public IActionResult SwitchBroker([FromBody] SwitchBrokerRequest request)
    {
        if (!Enum.TryParse<BrokerType>(request.BrokerType, ignoreCase: true, out var brokerType))
            return BadRequest(new { Message = $"Unknown broker type '{request.BrokerType}'. Valid values: Kafka, Aws, Gcp." });

        _brokerState.SwitchTo(brokerType);

        _logger.LogInformation("[NotificationApi] Active broker switched to {BrokerType}", brokerType);

        return Ok(new { Message = $"Active broker switched to {brokerType}.", ActiveBroker = brokerType.ToString() });
    }
}

public record SwitchBrokerRequest(string BrokerType);
