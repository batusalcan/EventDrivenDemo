using EventDrivenDemo.InvoiceApi.Services;
using EventDrivenDemo.Shared.Enums;
using EventDrivenDemo.Shared.Services;
using Microsoft.AspNetCore.Mvc;

namespace EventDrivenDemo.InvoiceApi.Controllers;

[ApiController]
[Route("api/[controller]")]
public class SystemController : ControllerBase
{
    private readonly ActiveBrokerState _brokerState;
    private readonly ConsumerControlState _controlState;
    private readonly ILogger<SystemController> _logger;

    public SystemController(ActiveBrokerState brokerState, ConsumerControlState controlState, ILogger<SystemController> logger)
    {
        _brokerState = brokerState;
        _controlState = controlState;
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

        _logger.LogInformation("[InvoiceApi] Active broker switched to {BrokerType}", brokerType);

        return Ok(new { Message = $"Active broker switched to {brokerType}.", ActiveBroker = brokerType.ToString() });
    }

    [HttpGet("consumer-status")]
    public IActionResult GetConsumerStatus()
    {
        return Ok(new { IsPaused = _controlState.IsPaused });
    }

    [HttpPost("pause-consumer")]
    public IActionResult PauseConsumer()
    {
        _controlState.Pause();
        _logger.LogInformation("[InvoiceApi] Consumer paused for fault tolerance demo.");
        return Ok(new { Message = "Invoice consumer paused. Messages are now queuing in the broker." });
    }

    [HttpPost("resume-consumer")]
    public IActionResult ResumeConsumer()
    {
        _controlState.Resume();
        _logger.LogInformation("[InvoiceApi] Consumer resumed. Catching up on missed messages...");
        return Ok(new { Message = "Invoice consumer resumed. Catching up on missed messages." });
    }
}

public record SwitchBrokerRequest(string BrokerType);
