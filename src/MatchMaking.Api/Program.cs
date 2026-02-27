using Confluent.Kafka;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddLogging();

builder.Services.AddSingleton<IConnectionMultiplexer>(_ =>
    ConnectionMultiplexer.Connect("redis:6379"));

builder.Services.AddSingleton<IProducer<string, string>>(_ =>
{
    var config = new ProducerConfig
    {
        BootstrapServers = "kafka:9092",
        Acks = Acks.All,
        EnableIdempotence = true,
        MessageSendMaxRetries = 5,
        RetryBackoffMs = 200
    };

    return new ProducerBuilder<string, string>(config).Build();
});

var app = builder.Build();

app.MapGet("/health", () => Results.Ok("healthy"));

app.MapPost("/match/search/{userId}",
    async Task<Results<NoContent, BadRequest>> (
        string userId,
        IProducer<string, string> producer,
        ILoggerFactory loggerFactory) =>
{
    var logger = loggerFactory.CreateLogger("MatchSearch");

    if (string.IsNullOrWhiteSpace(userId))
        return TypedResults.BadRequest();

    await producer.ProduceAsync("matchmaking.request",
        new Message<string, string> { Key = userId, Value = userId });

    logger.LogInformation("Match search requested for user {UserId}", userId);

    return TypedResults.NoContent();
})
.Produces(StatusCodes.Status204NoContent)
.Produces(StatusCodes.Status400BadRequest);

app.MapGet("/match/{userId}",
    async Task<Results<Ok<MatchResponse>, NotFound, BadRequest>> (
        string userId,
        IConnectionMultiplexer redis) =>
{
    if (string.IsNullOrWhiteSpace(userId))
        return TypedResults.BadRequest();

    var db = redis.GetDatabase();
    var result = await db.StringGetAsync($"match:{userId}");

    if (result.IsNullOrEmpty)
        return TypedResults.NotFound();

    var match = JsonSerializer.Deserialize<MatchResponse>(result!);

    return TypedResults.Ok(match!);
})
.Produces<MatchResponse>(StatusCodes.Status200OK)
.Produces(StatusCodes.Status404NotFound)
.Produces(StatusCodes.Status400BadRequest);

app.Run();

public record MatchResponse(string MatchId, IReadOnlyCollection<string> UserIds);