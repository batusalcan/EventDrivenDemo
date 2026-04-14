using EventDrivenDemo.NotificationApi.Messaging;
using EventDrivenDemo.NotificationApi.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddOpenApi();

// In-memory notification event log (singleton — shared between consumer and controller)
builder.Services.AddSingleton<NotificationEventLogStore>();

// Kafka consumer background service — listens to order-events topic
builder.Services.AddHostedService<NotificationKafkaConsumer>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();
app.UseAuthorization();
app.MapControllers();

app.Run();
