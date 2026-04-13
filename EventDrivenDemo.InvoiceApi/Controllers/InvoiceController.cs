using EventDrivenDemo.InvoiceApi.Services;
using Microsoft.AspNetCore.Mvc;

namespace EventDrivenDemo.InvoiceApi.Controllers;

[ApiController]
[Route("api/[controller]")]
public class InvoiceController : ControllerBase
{
    private readonly InvoiceEventLogStore _eventLog;

    public InvoiceController(InvoiceEventLogStore eventLog)
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
        return Ok(new { Message = "Invoice event log cleared." });
    }
}
