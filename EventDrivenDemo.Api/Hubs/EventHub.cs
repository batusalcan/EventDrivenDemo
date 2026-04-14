using Microsoft.AspNetCore.SignalR;

namespace EventDrivenDemo.Api.Hubs;

public class EventHub : Hub
{
    // Clients connect here and receive real-time broadcasts.
    // Server pushes via IHubContext<EventHub> injected into consumers.
    public override async Task OnConnectedAsync()
    {
        await Clients.Caller.SendAsync("Connected", $"[OrderApi] Connected to live event stream. ConnectionId: {Context.ConnectionId}");
        await base.OnConnectedAsync();
    }
}
