using Microsoft.AspNetCore.SignalR;

namespace EventDrivenDemo.InvoiceApi.Hubs;

public class EventHub : Hub
{
    public override async Task OnConnectedAsync()
    {
        await Clients.Caller.SendAsync("Connected", $"[InvoiceApi] Connected to live event stream. ConnectionId: {Context.ConnectionId}");
        await base.OnConnectedAsync();
    }
}
