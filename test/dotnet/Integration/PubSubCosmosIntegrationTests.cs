using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using StackExchange.Redis;
using Xunit;

namespace Microsoft.Azure.WebJobs.Extensions.Redis.Tests.Integration
{
    [Collection("PubSubTriggerTests")]
    public class PubSubCosmosIntegrationTests
    {

        [Theory]
        [InlineData(nameof(PubSubCosmosIntegrationTestFunctions.SingleChannelWriteBehind), PubSubCosmosIntegrationTestFunctions.pubsubChannel, "testValue single")]
        [InlineData(nameof(PubSubCosmosIntegrationTestFunctions.SingleChannelWriteBehind), PubSubCosmosIntegrationTestFunctions.pubsubChannel, "testValue multi")]
        [InlineData(nameof(PubSubCosmosIntegrationTestFunctions.MultipleChannelWriteBehind), PubSubCosmosIntegrationTestFunctions.pubsubChannel + "suffix", "testSuffix multi")]
        [InlineData(nameof(PubSubCosmosIntegrationTestFunctions.AllChannelsWriteBehind), PubSubCosmosIntegrationTestFunctions.pubsubChannel + "suffix", "testSuffix all")]
        [InlineData(nameof(PubSubCosmosIntegrationTestFunctions.AllChannelsWriteBehind), "prefix" + PubSubCosmosIntegrationTestFunctions.pubsubChannel, "testPrefix all")]
        [InlineData(nameof(PubSubCosmosIntegrationTestFunctions.AllChannelsWriteBehind), "separate", "testSeparate all")]
        [InlineData(nameof(PubSubCosmosIntegrationTestFunctions.SingleChannelWriteThrough), PubSubCosmosIntegrationTestFunctions.pubsubChannel, "testValue single")]
        [InlineData(nameof(PubSubCosmosIntegrationTestFunctions.SingleChannelWriteThrough), PubSubCosmosIntegrationTestFunctions.pubsubChannel, "testValue multi")]
        [InlineData(nameof(PubSubCosmosIntegrationTestFunctions.MultipleChannelWriteThrough), PubSubCosmosIntegrationTestFunctions.pubsubChannel + "suffix", "testSuffix multi")]
        [InlineData(nameof(PubSubCosmosIntegrationTestFunctions.AllChannelsWriteThrough), PubSubCosmosIntegrationTestFunctions.pubsubChannel + "suffix", "testSuffix all")]
        [InlineData(nameof(PubSubCosmosIntegrationTestFunctions.AllChannelsWriteThrough), "prefix" + PubSubCosmosIntegrationTestFunctions.pubsubChannel, "testPrefix all")]
        [InlineData(nameof(PubSubCosmosIntegrationTestFunctions.AllChannelsWriteThrough), "separate", "testSeparate all")]
        public async void PubSubMessageWrite_SuccessfullyWritesToCosmos(string functionName, string channel, string message)
        {

            using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(RedisUtilities.ResolveConnectionString(IntegrationTestHelpers.localsettings, PubSubCosmosIntegrationTestFunctions.localhostSetting)))
            using (Process functionsProcess = IntegrationTestHelpers.StartFunction(functionName, 7079))
            {
                ISubscriber subscriber = multiplexer.GetSubscriber();

                subscriber.Publish(channel, message);
                await Task.Delay(TimeSpan.FromSeconds(1));

                await multiplexer.CloseAsync();
                functionsProcess.Kill();
            };

            string cosmosMessage = null;
            using (CosmosClient client = new CosmosClient(RedisUtilities.ResolveConnectionString(IntegrationTestHelpers.localsettings, PubSubCosmosIntegrationTestFunctions.cosmosDbConnectionSetting)))
            {
                var db = client.GetContainer("DatabaseId", "PSContainerId");
                var queryable = db.GetItemLinqQueryable<PubSubData>();

                //get all entries in the container that contain the missed key
                using FeedIterator<PubSubData> feed = queryable
                    .Where(p => p.channel == channel)
                    .OrderByDescending(p => p.timestamp)
                    .ToFeedIterator();
                var response = await feed.ReadNextAsync();
                var item = response.FirstOrDefault(defaultValue: null);
                cosmosMessage = item?.message;
            }

            Assert.True(message == cosmosMessage, $"Expected \"{message}\" but got \"{cosmosMessage}\"");
            IntegrationTestHelpers.ClearDataFromCosmosDb("DatabaseId", "PSContainerId");
        }


        [Theory]
        [InlineData(nameof(PubSubCosmosIntegrationTestFunctions.WriteThrough), "testKey-1", "testValue1")]
        [InlineData(nameof(PubSubCosmosIntegrationTestFunctions.WriteBehindAsync), "testKey-2", "testValue2")]
        public async void RedisToCosmos_SuccessfullyWritesToCosmos(string functionName, string key, string value)
        { 
            string keyFromCosmos = null;
            string valueFromCosmos = null;
            
            using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(RedisUtilities.ResolveConnectionString(IntegrationTestHelpers.localsettings, PubSubCosmosIntegrationTestFunctions.localhostSetting)))
            using (Process functionsProcess = IntegrationTestHelpers.StartFunction(functionName, 7079))
            {
                var redisDb = multiplexer.GetDatabase();
                await redisDb.StringSetAsync(key, value);
                await Task.Delay(TimeSpan.FromSeconds(5));

                
                using (CosmosClient client = new CosmosClient(RedisUtilities.ResolveConnectionString(IntegrationTestHelpers.localsettings, PubSubCosmosIntegrationTestFunctions.cosmosDbConnectionSetting)))
                {
                    var cosmosDb = client.GetContainer("DatabaseId", "ContainerId");
                    var queryable = cosmosDb.GetItemLinqQueryable<RedisData>();

                //get all entries in the container that contain the missed key
                    using FeedIterator<RedisData> feed = queryable
                        .Where(p => p.key == key)
                        .OrderByDescending(p => p.timestamp)
                        .ToFeedIterator();
                    var response = await feed.ReadNextAsync();
                    await Task.Delay(TimeSpan.FromSeconds(3));

                    var item = response.FirstOrDefault(defaultValue: null);

                    keyFromCosmos = item?.key;
                    valueFromCosmos = item?.value;
                };

                await redisDb.KeyDeleteAsync(key);
                functionsProcess.Kill();
            };

            Assert.True(keyFromCosmos == key, $"Expected \"{key}\" but got \"{keyFromCosmos}\"");
            Assert.True(valueFromCosmos == value, $"Expected \"{value}\" but got \"{valueFromCosmos}\"");
            IntegrationTestHelpers.ClearDataFromCosmosDb("DatabaseId", "ContainerId");
        }

        [Fact]
        public async void WriteAround_SuccessfullyWritesToRedis()
        {
            string functionName = nameof(PubSubCosmosIntegrationTestFunctions.WriteAroundAsync);
            using (CosmosClient client = new CosmosClient(RedisUtilities.ResolveConnectionString(IntegrationTestHelpers.localsettings, PubSubCosmosIntegrationTestFunctions.cosmosDbConnectionSetting)))
            using (Process functionsProcess = IntegrationTestHelpers.StartFunction(functionName, 7081))
            {
                Container cosmosContainer = client.GetContainer("DatabaseId", "ContainerId");
                await Task.Delay(TimeSpan.FromSeconds(5));

                RedisData redisData = new RedisData(
                    id: Guid.NewGuid().ToString(),
                    key: "cosmosKey",
                    value: "cosmosValue",
                    timestamp: DateTime.UtcNow
                );
                
                await cosmosContainer.CreateItemAsync(redisData);
                await Task.Delay(TimeSpan.FromSeconds(10));
                client.Dispose();
                functionsProcess.Kill();

            }


            using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(RedisUtilities.ResolveConnectionString(IntegrationTestHelpers.localsettings, PubSubCosmosIntegrationTestFunctions.localhostSetting)))
            {
                var redisValue = await multiplexer.GetDatabase().StringGetAsync("cosmosKey");
                await Task.Delay(TimeSpan.FromSeconds(10));
                Assert.Equal("cosmosValue", redisValue);
                //await multiplexer.GetDatabase().KeyDeleteAsync("cosmosKey");
                // await Task.Delay(TimeSpan.FromSeconds(3));
                await multiplexer.CloseAsync();
            }
            IntegrationTestHelpers.ClearDataFromCosmosDb("DatabaseId", "ContainerId");
        }


        [Fact]
        public async void WriteAroundMessage_SuccessfullyPublishesToRedis()
        {
            string functionName = nameof(PubSubCosmosIntegrationTestFunctions.WriteAroundMessageAsync);
            using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(RedisUtilities.ResolveConnectionString(IntegrationTestHelpers.localsettings, PubSubCosmosIntegrationTestFunctions.localhostSetting)))
            using (CosmosClient client = new CosmosClient(RedisUtilities.ResolveConnectionString(IntegrationTestHelpers.localsettings, PubSubCosmosIntegrationTestFunctions.cosmosDbConnectionSetting)))
            using (Process functionsProcess = IntegrationTestHelpers.StartFunction(functionName, 7081))
            {
                ISubscriber subscriber = multiplexer.GetSubscriber();
                subscriber.Subscribe("PubSubChannel", (channel, message) =>
                {
                    Assert.Equal("PubSubMessage", message);
                });

                Container cosmosContainer = client.GetContainer("DatabaseId", "PSContainerId");
                //await Task.Delay(TimeSpan.FromSeconds(5));

                PubSubData redisData = new PubSubData(
                    id: Guid.NewGuid().ToString(),
                    channel: "PubSubChannel",
                    message: "PubSubMessage",
                    timestamp: DateTime.UtcNow
                );

                await cosmosContainer.CreateItemAsync(redisData);
                await Task.Delay(TimeSpan.FromSeconds(5));
                client.Dispose();
                functionsProcess.Kill();

                await multiplexer.CloseAsync();
            }
            IntegrationTestHelpers.ClearDataFromCosmosDb("DatabaseId", "PSContainerId");
        }


        [Fact]
        public async void ReadThrough_SuccessfullyWritesToRedis()
        {
            string functionName = nameof(PubSubCosmosIntegrationTestFunctions.ReadThroughAsync);
            using (CosmosClient client = new CosmosClient(RedisUtilities.ResolveConnectionString(IntegrationTestHelpers.localsettings, PubSubCosmosIntegrationTestFunctions.cosmosDbConnectionSetting)))
            {
                Container cosmosContainer = client.GetContainer("DatabaseId", "ContainerId");
                RedisData redisData = new RedisData(
                    id: Guid.NewGuid().ToString(),
                    key: "cosmosKey1",
                    value: "cosmosValue1",
                    timestamp: DateTime.UtcNow
                );
                await cosmosContainer.UpsertItemAsync(redisData);
                await Task.Delay(TimeSpan.FromSeconds(2));
                client.Dispose();
            }

            using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(RedisUtilities.ResolveConnectionString(IntegrationTestHelpers.localsettings, PubSubCosmosIntegrationTestFunctions.localhostSetting)))
            using (Process functionsProcess = IntegrationTestHelpers.StartFunction(functionName, 7082))
            {
                var redisValue = await multiplexer.GetDatabase().StringGetAsync("cosmosKey1");
                Assert.True(redisValue.IsNull, userMessage: "Key already in Redis Cache, test failed");
                await Task.Delay(TimeSpan.FromSeconds(5));
                redisValue = await multiplexer.GetDatabase().StringGetAsync("cosmosKey1");
                
                await Task.Delay(TimeSpan.FromSeconds(3));

                Assert.Equal("cosmosValue1", redisValue);
                await multiplexer.GetDatabase().KeyDeleteAsync("cosmosKey1");
                await Task.Delay(TimeSpan.FromSeconds(2));
                await multiplexer.CloseAsync();
            }
            IntegrationTestHelpers.ClearDataFromCosmosDb("DatabaseId", "ContainerId");
        }

        [Fact]
        public async void ReadThrough_UnsuccessfulWhenKeyNotFoundInCosmos()
        {
            string functionName = nameof(PubSubCosmosIntegrationTestFunctions.ReadThroughAsync);

            Dictionary<string, int> counts = new Dictionary<string, int>
            {
                { $"Executed '{functionName}' (Failed", 1},
                { $"ERROR: Key: \"unknownKey\" not found in Redis or Cosmos DB. Try adding the Key-Value pair to Redis or Cosmos DB.", 1},
            };

            using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(RedisUtilities.ResolveConnectionString(IntegrationTestHelpers.localsettings, PubSubCosmosIntegrationTestFunctions.localhostSetting)))
            using (Process functionsProcess = IntegrationTestHelpers.StartFunction(functionName, 7080))
            {
                functionsProcess.OutputDataReceived += IntegrationTestHelpers.CounterHandlerCreator(counts);

                var redisValue = await multiplexer.GetDatabase().StringGetAsync("unknownKey");
                Assert.True(redisValue.IsNull, userMessage: "Key already in Redis Cache, test failed");

                await Task.Delay(TimeSpan.FromSeconds(1));
                Assert.True(redisValue.IsNull);

                var incorrect = counts.Where(pair => pair.Value != 0);
                Assert.False(incorrect.Any(), JsonSerializer.Serialize(incorrect));

                await multiplexer.CloseAsync();
            }
        }

        [Theory]
        [InlineData(nameof(PubSubCosmosIntegrationTestFunctions.WriteThrough), "testKey1", "testValue1", 10)]
        [InlineData(nameof(PubSubCosmosIntegrationTestFunctions.WriteThrough), "testKey1", "testValue1", 100)]
        [InlineData(nameof(PubSubCosmosIntegrationTestFunctions.WriteThrough), "testKey1", "testValue1", 1000)]
        [InlineData(nameof(PubSubCosmosIntegrationTestFunctions.WriteBehindAsync), "testKey2", "testValue2", 10)]
        [InlineData(nameof(PubSubCosmosIntegrationTestFunctions.WriteBehindAsync), "testKey2", "testValue2", 100)]
        [InlineData(nameof(PubSubCosmosIntegrationTestFunctions.WriteBehindAsync), "testKey2", "testValue2", 1000)]
        public async void RedisToCosmos_MultipleWritesSuccessfully(string functionName, string key, string value, int numberOfWrites)
        {
            string keyFromCosmos = null;
            string valueFromCosmos = null;

            using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(RedisUtilities.ResolveConnectionString(IntegrationTestHelpers.localsettings, PubSubCosmosIntegrationTestFunctions.localhostSetting)))
            using (Process functionsProcess = IntegrationTestHelpers.StartFunction(functionName, 7072))
            {
                var redisDb = multiplexer.GetDatabase();
                //await redisDb.StringSetAsync("Startup", value);
                //await Task.Delay(TimeSpan.FromSeconds(5));

                for (int i = 1; i <= numberOfWrites; i++)
                {
                    await redisDb.StringSetAsync(key + "-" + i, value + "-" + i);

                    //await Task.Delay(TimeSpan.FromSeconds(1));


                    using (CosmosClient client = new CosmosClient(RedisUtilities.ResolveConnectionString(IntegrationTestHelpers.localsettings, PubSubCosmosIntegrationTestFunctions.cosmosDbConnectionSetting)))
                    {
                        var cosmosDb = client.GetContainer("DatabaseId", "ContainerId");
                        var queryable = cosmosDb.GetItemLinqQueryable<RedisData>();

                        //get all entries in the container that contain the missed key
                        using FeedIterator<RedisData> feed = queryable
                            .Where(p => p.key == key + "-"+ i)
                            .OrderByDescending(p => p.timestamp)
                            .ToFeedIterator();
                        var response = await feed.ReadNextAsync();
                        //await Task.Delay(TimeSpan.FromSeconds(3));

                        var item = response.FirstOrDefault(defaultValue: null);

                        keyFromCosmos = item?.key;
                        valueFromCosmos = item?.value;
                    };

                    Assert.True(keyFromCosmos == key + "-" + i, $"Expected \"{key + "-" + i}\" but got \"{keyFromCosmos}\"");
                    Assert.True(valueFromCosmos == value + "-" + i, $"Expected \"{value + "-" + i}\" but got \"{valueFromCosmos}\"");
                }

               //await redisDb.KeyDeleteAsync("Startup");
                for (int i = 1; i <= numberOfWrites; i++) 
                {
                    await redisDb.KeyDeleteAsync(key + "-" + i);
                }
                IntegrationTestHelpers.ClearDataFromCosmosDb("DatabaseId", "ContainerId");
                functionsProcess.Kill();
            };
        }
        [Theory]
        [InlineData(nameof(PubSubCosmosIntegrationTestFunctions.WriteThrough), "testKey1", "testValue1", 10)]
        [InlineData(nameof(PubSubCosmosIntegrationTestFunctions.WriteThrough), "testKey1", "testValue1", 100)]
        [InlineData(nameof(PubSubCosmosIntegrationTestFunctions.WriteThrough), "testKey1", "testValue1", 1000)]
        [InlineData(nameof(PubSubCosmosIntegrationTestFunctions.WriteBehindAsync), "testKey2", "testValue2", 10)]
        [InlineData(nameof(PubSubCosmosIntegrationTestFunctions.WriteBehindAsync), "testKey2", "testValue2", 100)]
        [InlineData(nameof(PubSubCosmosIntegrationTestFunctions.WriteBehindAsync), "testKey2", "testValue2", 1000)]
        public async void RedisToCosmos_MultipleWritesSuccessfullyV2(string functionName, string key, string value, int numberOfWrites)
        {
            string keyFromCosmos = null;
            string valueFromCosmos = null;

            using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(RedisUtilities.ResolveConnectionString(IntegrationTestHelpers.localsettings, PubSubCosmosIntegrationTestFunctions.localhostSetting)))
            using (Process functionsProcess = IntegrationTestHelpers.StartFunction(functionName, 7079))
            {
                var redisDb = multiplexer.GetDatabase();
                //await redisDb.StringSetAsync("Startup", value);
                //await Task.Delay(TimeSpan.FromSeconds(5));

                for (int i = 1; i <= numberOfWrites; i++)
                {
                    await redisDb.StringSetAsync(key + "-" + i, value + "-" + i);

                    //await Task.Delay(TimeSpan.FromSeconds(1));
                }

                await Task.Delay(TimeSpan.FromSeconds(30));
                using (CosmosClient client = new CosmosClient(RedisUtilities.ResolveConnectionString(IntegrationTestHelpers.localsettings, PubSubCosmosIntegrationTestFunctions.cosmosDbConnectionSetting)))
                {
                    var cosmosDb = client.GetContainer("DatabaseId", "ContainerId");
                    var queryable = cosmosDb.GetItemLinqQueryable<RedisData>();
                    for (int i = 1; i <= numberOfWrites; i++)
                    {
                        //get all entries in the container that contain the missed key
                        using FeedIterator<RedisData> feed = queryable
                            .Where(p => p.key == key + "-" + i)
                            .OrderByDescending(p => p.timestamp)
                            .ToFeedIterator();
                        var response = await feed.ReadNextAsync();
                        //await Task.Delay(TimeSpan.FromSeconds(2));

                        var item = response.FirstOrDefault(defaultValue: null);

                        if(item == null)
                        {
                            await Task.Delay(TimeSpan.FromSeconds(5));

                            item = response.FirstOrDefault(defaultValue: null);
                        }
                        keyFromCosmos = item?.key;
                        valueFromCosmos = item?.value;

                        Assert.True(keyFromCosmos == key + "-" + i, $"Expected \"{key + "-" + i}\" but got \"{keyFromCosmos}\"");
                        Assert.True(valueFromCosmos == value + "-" + i, $"Expected \"{value + "-" + i}\" but got \"{valueFromCosmos}\"");
                    }
                };

                for (int i = 1; i <= numberOfWrites; i++)
                {
                    await redisDb.KeyDeleteAsync(key + "-" + i);
                }
                IntegrationTestHelpers.ClearDataFromCosmosDb("DatabaseId", "ContainerId");
                functionsProcess.Kill();
            };
        }
    }
}
