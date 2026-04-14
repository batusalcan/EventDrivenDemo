using Microsoft.AspNetCore.SignalR;

namespace EventDrivenDemo.NotificationApi.Hubs;

public class EventHub : Hub
{
    public override async Task OnConnectedAsync()
    {
        await Clients.Caller.SendAsync("Connected", $"[NotificationApi] Connected to live event stream. ConnectionId: {Context.ConnectionId}");
        await base.OnConnectedAsync();
    }
}
