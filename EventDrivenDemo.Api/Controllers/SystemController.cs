using EventDrivenDemo.Api.Services;
using EventDrivenDemo.Shared.Enums;
using Microsoft.AspNetCore.Mvc;

namespace EventDrivenDemo.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class SystemController : ControllerBase
{
    private readonly BrokerSwitcher _brokerSwitcher;
    private readonly IConfiguration _configuration;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<SystemController> _logger;

    public SystemController(BrokerSwitcher brokerSwitcher, IConfiguration configuration, ILoggerFactory loggerFactory, ILogger<SystemController> logger)
    {
        _brokerSwitcher = brokerSwitcher;
        _configuration = configuration;
        _loggerFactory = loggerFactory;
        _logger = logger;
    }

    [HttpGet("active-broker")]
    public IActionResult GetActiveBroker()
    {
        return Ok(new { ActiveBroker = _brokerSwitcher.ActiveBroker.ToString() });
    }

    [HttpPost("switch-broker")]
    public IActionResult SwitchBroker([FromBody] SwitchBrokerRequest request)
    {
        if (!Enum.TryParse<BrokerType>(request.BrokerType, ignoreCase: true, out var brokerType))
        {
            return BadRequest(new { Message = $"Unknown broker type '{request.BrokerType}'. Valid values: Kafka, Aws, Gcp." });
        }

        _brokerSwitcher.SwitchTo(brokerType, _configuration, _loggerFactory);

        _logger.LogInformation("[System] Active broker switched to {BrokerType}", brokerType);

        return Ok(new { Message = $"Active broker switched to {brokerType}.", ActiveBroker = brokerType.ToString() });
    }
}

public record SwitchBrokerRequest(string BrokerType);
