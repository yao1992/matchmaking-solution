using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System.Text.Json;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddHostedService<Worker>();
builder.Services.AddLogging();

await builder.Build().RunAsync();

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private const string TopicName = "matchmaking.request";
    private const int MatchSize = 3;
    private const string BootstrapServers = "kafka:9092";
    private const string RedisConnection = "redis:6379";

    public Worker(ILogger<Worker> logger)
    {
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await EnsureTopicExists();
        await ConsumeLoop(stoppingToken);
    }

    private async Task EnsureTopicExists()
    {
        using var admin = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = BootstrapServers }).Build();

        try
        {
            await admin.CreateTopicsAsync(new[]
            {
                new TopicSpecification
                {
                    Name = TopicName,
                    NumPartitions = 3,
                    ReplicationFactor = 1
                }
            });

            _logger.LogInformation("Topic {Topic} created", TopicName);
        }
        catch (CreateTopicsException e)
        {
            if (!e.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
                throw;

            _logger.LogInformation("Topic {Topic} already exists", TopicName);
        }
    }

    private async Task ConsumeLoop(CancellationToken stoppingToken)
    {
        using var consumer = CreateConsumer();
        using var redis = ConnectionMultiplexer.Connect(RedisConnection);

        var db = redis.GetDatabase();
        var queue = new List<string>(MatchSize);

        consumer.Subscribe(TopicName);

        _logger.LogInformation("Worker started consuming");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(stoppingToken);

                if (result?.Message == null)
                    continue;

                _logger.LogInformation("Received user {UserId}", result.Message.Value);

                queue.Add(result.Message.Value);

                if (queue.Count == MatchSize)
                {
                    await CreateMatchAsync(queue, db);
                    consumer.Commit();
                    queue.Clear();
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while processing message");
            }
        }

        consumer.Close();
        _logger.LogInformation("Worker stopped");
    }

    private IConsumer<string, string> CreateConsumer()
    {
        return new ConsumerBuilder<string, string>(
            new ConsumerConfig
            {
                BootstrapServers = BootstrapServers,
                GroupId = "worker-group",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            }).Build();
    }

    private async Task CreateMatchAsync(List<string> users, IDatabase db)
    {
        var matchId = Guid.NewGuid().ToString();

        var payload = JsonSerializer.Serialize(new
        {
            matchId,
            userIds = users
        });

        foreach (var user in users)
        {
            await db.StringSetAsync($"match:{user}", payload);
        }

        _logger.LogInformation(
            "Match {MatchId} created for users {Users}",
            matchId,
            string.Join(",", users));
    }
}