using EventDrivenDemo.NotificationApi.Services;
using Microsoft.AspNetCore.Mvc;

namespace EventDrivenDemo.NotificationApi.Controllers;

[ApiController]
[Route("api/[controller]")]
public class NotificationController : ControllerBase
{
    private readonly NotificationEventLogStore _eventLog;

    public NotificationController(NotificationEventLogStore eventLog)
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
        return Ok(new { Message = "Notification event log cleared." });
    }
}
