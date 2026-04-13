using EventDrivenDemo.Api.Services;
using Microsoft.AspNetCore.Mvc;

namespace EventDrivenDemo.Api.Controllers;

[ApiController]
[Route("api/events")]
public class EventLogController : ControllerBase
{
    private readonly EventLogStore _eventLog;

    public EventLogController(EventLogStore eventLog)
    {
        _eventLog = eventLog;
    }

    [HttpGet("logs")]
    public IActionResult GetLogs()
    {
        return Ok(_eventLog.GetAll());
    }

    [HttpDelete("logs")]
    public IActionResult ClearLogs()
    {
        _eventLog.Clear();
        return Ok(new { Message = "Event log cleared." });
    }
}
