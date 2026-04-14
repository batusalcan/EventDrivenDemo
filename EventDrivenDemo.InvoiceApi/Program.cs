using EventDrivenDemo.InvoiceApi.Hubs;
using EventDrivenDemo.InvoiceApi.Messaging;
using EventDrivenDemo.InvoiceApi.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddOpenApi();

// CORS — allow React frontend origin
builder.Services.AddCors(options =>
{
    options.AddPolicy("FrontendPolicy", policy =>
        policy.WithOrigins("http://localhost:3000")
              .AllowAnyHeader()
              .AllowAnyMethod()
              .AllowCredentials());
});

// SignalR
builder.Services.AddSignalR();

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
app.UseCors("FrontendPolicy");
app.UseAuthorization();
app.MapControllers();

// SignalR hub endpoint
app.MapHub<EventHub>("/hubs/events");

app.Run();
