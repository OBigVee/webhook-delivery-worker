using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using WebhookDelivery.Worker;

var builder = Host.CreateApplicationBuilder(args);
var brokerUri = new Uri(builder.Configuration["RabbitMq:Uri"] ?? "amqp://guest:guest@localhost:5672/");
var destination = new Uri(builder.Configuration["Webhook:Url"] ?? "http://127.0.0.1:5090/webhooks");
if (destination.Scheme is not ("http" or "https"))
{
    throw new InvalidOperationException("Webhook:Url must use HTTP or HTTPS.");
}

builder.Services.AddSingleton(new ConnectionFactory { Uri = brokerUri });
builder.Services.AddSingleton(new WebhookDestination(destination));
builder.Services.AddHttpClient("webhook", client => client.Timeout = TimeSpan.FromSeconds(5))
    .ConfigurePrimaryHttpMessageHandler(() => new HttpClientHandler { AllowAutoRedirect = false });
builder.Services.AddHostedService<WebhookWorker>();
await builder.Build().RunAsync();
