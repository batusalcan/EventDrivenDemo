using EventDrivenDemo.Api.Hubs;
using EventDrivenDemo.Api.Messaging.Kafka;
using EventDrivenDemo.Api.Services;
using EventDrivenDemo.Shared.Interfaces;

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
              .AllowCredentials()); // required for SignalR WebSockets
});

// SignalR
builder.Services.AddSignalR();

// In-memory event log (singleton — shared between consumer and controller)
builder.Services.AddSingleton<EventLogStore>();

// BrokerSwitcher is the single IMessagePublisher registered in DI.
builder.Services.AddSingleton<BrokerSwitcher>();
builder.Services.AddSingleton<IMessagePublisher>(sp => sp.GetRequiredService<BrokerSwitcher>());

// Kafka consumer background service
builder.Services.AddHostedService<KafkaConsumer>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

if (app.Environment.IsDevelopment())
    app.UseHttpsRedirection();

app.UseCors("FrontendPolicy");
app.UseAuthorization();
app.MapControllers();

// SignalR hub endpoint
app.MapHub<EventHub>("/hubs/events");

app.Run();
