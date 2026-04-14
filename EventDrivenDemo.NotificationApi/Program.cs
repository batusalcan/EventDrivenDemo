using EventDrivenDemo.NotificationApi.Hubs;
using EventDrivenDemo.NotificationApi.Messaging;
using EventDrivenDemo.NotificationApi.Services;

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

// Kafka consumer background service — listens to order-events topic
builder.Services.AddHostedService<NotificationKafkaConsumer>();

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
