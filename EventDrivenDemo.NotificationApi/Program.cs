using EventDrivenDemo.NotificationApi.Hubs;
using EventDrivenDemo.NotificationApi.Messaging;
using EventDrivenDemo.NotificationApi.Services;
using EventDrivenDemo.Shared.Enums;
using EventDrivenDemo.Shared.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddOpenApi();

// CORS — allow React frontend origin
builder.Services.AddCors(options =>
{
    options.AddPolicy("FrontendPolicy", policy =>
        policy.WithOrigins("http://localhost:5173", "http://localhost:3000")
              .AllowAnyHeader()
              .AllowAnyMethod()
              .AllowCredentials());
});

// SignalR
builder.Services.AddSignalR();

// In-memory notification event log (singleton — shared between consumer and controller)
builder.Services.AddSingleton<NotificationEventLogStore>();

// Active broker state — read initial value from config, can be changed at runtime
// via POST /api/system/switch-broker without a restart.
var initialBroker = Enum.Parse<BrokerType>(
    builder.Configuration["ActiveBroker"] ?? "Kafka", ignoreCase: true);
builder.Services.AddSingleton(new ActiveBrokerState(initialBroker));

// Single consumer router — watches ActiveBrokerState and hot-swaps the active
// consume loop when the broker changes. No restart required.
builder.Services.AddHostedService<NotificationConsumerRouter>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
    app.MapOpenApi();

if (app.Environment.IsDevelopment())
    app.UseHttpsRedirection();

app.UseCors("FrontendPolicy");
app.UseAuthorization();
app.MapControllers();

// SignalR hub endpoint
app.MapHub<EventHub>("/hubs/events");

app.Run();
