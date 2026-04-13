using EventDrivenDemo.Api.Messaging.Kafka;
using EventDrivenDemo.Api.Services;
using EventDrivenDemo.Shared.Interfaces;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddOpenApi();

// In-memory event log (singleton — shared between consumer and controller)
builder.Services.AddSingleton<EventLogStore>();

// Active message publisher — Kafka by default
builder.Services.AddSingleton<IMessagePublisher, KafkaPublisher>();

// Kafka consumer background service
builder.Services.AddHostedService<KafkaConsumer>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();
app.UseAuthorization();
app.MapControllers();

app.Run();
