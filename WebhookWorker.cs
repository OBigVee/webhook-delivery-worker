using System.Net.Http.Headers;
using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace WebhookDelivery.Worker;

internal sealed record WebhookDestination(Uri Url);

internal sealed class WebhookWorker(
    ConnectionFactory connectionFactory,
    WebhookDestination destination,
    IHttpClientFactory httpClientFactory,
    ILogger<WebhookWorker> logger) : BackgroundService
{
    private const string Queue = "webhook.delivery";
    private const string DeadLetterQueue = "webhook.delivery.dead";
    private static readonly TimeSpan[] RetryDelays = [TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2)];

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await using var connection = await connectionFactory.CreateConnectionAsync(stoppingToken);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);

        await channel.QueueDeclareAsync(DeadLetterQueue, durable: true, exclusive: false, autoDelete: false,
            cancellationToken: stoppingToken);
        await channel.QueueDeclareAsync(Queue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?>
            {
                ["x-dead-letter-exchange"] = "",
                ["x-dead-letter-routing-key"] = DeadLetterQueue
            }, cancellationToken: stoppingToken);
        await channel.BasicQosAsync(0, prefetchCount: 1, global: false, cancellationToken: stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, delivery) =>
        {
            try
            {
                var delivered = await DeliverAsync(delivery.Body.ToArray(), stoppingToken);
                if (delivered)
                {
                    await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, stoppingToken);
                }
                else
                {
                    await channel.BasicRejectAsync(delivery.DeliveryTag, requeue: false, stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // Closing the channel returns the unacknowledged message to the queue.
            }
        };
        await channel.BasicConsumeAsync(Queue, autoAck: false, consumer, stoppingToken);
        logger.LogInformation("Waiting for messages on {Queue}", Queue);
        await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
    }

    private async Task<bool> DeliverAsync(byte[] body, CancellationToken cancellationToken)
    {
        string? eventId;
        try
        {
            using var document = JsonDocument.Parse(body);
            var root = document.RootElement;
            eventId = root.ValueKind == JsonValueKind.Object
                && root.TryGetProperty("eventId", out var value)
                && value.ValueKind == JsonValueKind.String ? value.GetString() : null;
        }
        catch (JsonException)
        {
            eventId = null;
        }

        if (string.IsNullOrWhiteSpace(eventId))
        {
            logger.LogWarning("Rejecting message without a valid event ID");
            return false;
        }

        using var client = httpClientFactory.CreateClient("webhook");
        for (var attempt = 0; attempt <= RetryDelays.Length; attempt++)
        {
            logger.LogInformation("Delivering event {EventId}, attempt {Attempt}", eventId, attempt + 1);
            try
            {
                using var content = new ByteArrayContent(body);
                content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                using var response = await client.PostAsync(destination.Url, content, cancellationToken);
                var status = (int)response.StatusCode;
                if (response.IsSuccessStatusCode)
                {
                    logger.LogInformation("Delivered event {EventId} with HTTP {Status}", eventId, status);
                    return true;
                }

                logger.LogWarning("Event {EventId} received HTTP {Status}", eventId, status);
                if (status is not (408 or 429) && status < 500)
                {
                    break;
                }
            }
            catch (HttpRequestException error)
            {
                logger.LogWarning(error, "HTTP delivery failed for event {EventId}", eventId);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning("HTTP delivery timed out for event {EventId}", eventId);
            }

            if (attempt < RetryDelays.Length)
            {
                await Task.Delay(RetryDelays[attempt], cancellationToken);
            }
        }

        logger.LogWarning("Rejecting event {EventId} for dead-lettering", eventId);
        return false;
    }
}
