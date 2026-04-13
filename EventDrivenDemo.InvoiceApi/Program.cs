using EventDrivenDemo.InvoiceApi.Messaging;
using EventDrivenDemo.InvoiceApi.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddOpenApi();

// In-memory invoice event log (singleton — shared between consumer and controller)
builder.Services.AddSingleton<InvoiceEventLogStore>();

// Kafka consumer background service — listens to order-events topic
builder.Services.AddHostedService<InvoiceKafkaConsumer>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();
app.UseAuthorization();
app.MapControllers();

app.Run();
