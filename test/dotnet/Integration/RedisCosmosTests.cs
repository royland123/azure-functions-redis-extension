using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using StackExchange.Redis;
using Xunit;

namespace Microsoft.Azure.WebJobs.Extensions.Redis.Tests.Integration
{
    [Collection("PubSubTriggerTests")]
    public class RedisCosmosTests
    {

        [Theory]
        [InlineData(nameof(RedisCosmosTestFunctions.SingleChannelWriteBehind), RedisCosmosTestFunctions.pubsubChannel, "testValue single")]
        [InlineData(nameof(RedisCosmosTestFunctions.SingleChannelWriteBehind), RedisCosmosTestFunctions.pubsubChannel, "testValue multi")]
        [InlineData(nameof(RedisCosmosTestFunctions.MultipleChannelWriteBehind), RedisCosmosTestFunctions.pubsubChannel + "suffix", "testSuffix multi")]
        [InlineData(nameof(RedisCosmosTestFunctions.AllChannelsWriteBehind), RedisCosmosTestFunctions.pubsubChannel + "suffix", "testSuffix all")]
        [InlineData(nameof(RedisCosmosTestFunctions.AllChannelsWriteBehind), "prefix" + RedisCosmosTestFunctions.pubsubChannel, "testPrefix all")]
        [InlineData(nameof(RedisCosmosTestFunctions.AllChannelsWriteBehind), "separate", "testSeparate all")]
        [InlineData(nameof(RedisCosmosTestFunctions.SingleChannelWriteThrough), RedisCosmosTestFunctions.pubsubChannel, "testValue single")]
        [InlineData(nameof(RedisCosmosTestFunctions.SingleChannelWriteThrough), RedisCosmosTestFunctions.pubsubChannel, "testValue multi")]
        [InlineData(nameof(RedisCosmosTestFunctions.MultipleChannelWriteThrough), RedisCosmosTestFunctions.pubsubChannel + "suffix", "testSuffix multi")]
        [InlineData(nameof(RedisCosmosTestFunctions.AllChannelsWriteThrough), RedisCosmosTestFunctions.pubsubChannel + "suffix", "testSuffix all")]
        [InlineData(nameof(RedisCosmosTestFunctions.AllChannelsWriteThrough), "prefix" + RedisCosmosTestFunctions.pubsubChannel, "testPrefix all")]
        [InlineData(nameof(RedisCosmosTestFunctions.AllChannelsWriteThrough), "separate", "testSeparate all")]
        public async void PubSubMessageWrite_SuccessfullyWritesToCosmos(string functionName, string channel, string message)
        {

            using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(RedisUtilities.ResolveConnectionString(IntegrationTestHelpers.localsettings, RedisCosmosTestFunctions.localhostSetting)))
            using (Process functionsProcess = IntegrationTestHelpers.StartFunction(functionName, 7079))
            {
                ISubscriber subscriber = multiplexer.GetSubscriber();

                subscriber.Publish(channel, message);
                await Task.Delay(TimeSpan.FromSeconds(1));

                await multiplexer.CloseAsync();
                functionsProcess.Kill();
            };

            string cosmosMessage = null;
            using (CosmosClient client = new CosmosClient(RedisUtilities.ResolveConnectionString(IntegrationTestHelpers.localsettings, RedisCosmosTestFunctions.cosmosDbConnectionSetting)))
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
        [InlineData(nameof(RedisCosmosTestFunctions.WriteThrough), "testKey-1", "testValue1")]
        [InlineData(nameof(RedisCosmosTestFunctions.WriteBehindAsync), "testKey-2", "testValue2")]
        public async void RedisToCosmos_SuccessfullyWritesToCosmos(string functionName, string key, string value)
        { 
            string keyFromCosmos = null;
            string valueFromCosmos = null;
            
            using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(RedisUtilities.ResolveConnectionString(IntegrationTestHelpers.localsettings, RedisCosmosTestFunctions.localhostSetting)))
            using (Process functionsProcess = IntegrationTestHelpers.StartFunction(functionName, 7079))
            {
                var redisDb = multiplexer.GetDatabase();
                await redisDb.StringSetAsync(key, value);
                await Task.Delay(TimeSpan.FromSeconds(5));

                
                using (CosmosClient client = new CosmosClient(RedisUtilities.ResolveConnectionString(IntegrationTestHelpers.localsettings, RedisCosmosTestFunctions.cosmosDbConnectionSetting)))
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
            string functionName = nameof(RedisCosmosTestFunctions.WriteAroundAsync);
            using (CosmosClient client = new CosmosClient(RedisUtilities.ResolveConnectionString(IntegrationTestHelpers.localsettings, RedisCosmosTestFunctions.cosmosDbConnectionSetting)))
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
                await Task.Delay(TimeSpan.FromSeconds(5));
                client.Dispose();
                functionsProcess.Kill();

            }
            using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(RedisUtilities.ResolveConnectionString(IntegrationTestHelpers.localsettings, RedisCosmosTestFunctions.localhostSetting)))
            {
                var redisValue = await multiplexer.GetDatabase().StringGetAsync("cosmosKey");
                await Task.Delay(TimeSpan.FromSeconds(5));
                Assert.Equal("cosmosValue", redisValue);
                //await multiplexer.GetDatabase().KeyDeleteAsync("cosmosKey");
                // await Task.Delay(TimeSpan.FromSeconds(3));
                await multiplexer.CloseAsync();
            }
            IntegrationTestHelpers.ClearDataFromCosmosDb("DatabaseId", "ContainerId");
        }

        [Fact]
        public async void ReadThrough_SuccessfullyWritesToRedis()
        {
            string functionName = nameof(RedisCosmosTestFunctions.ReadThroughAsync);
            using (CosmosClient client = new CosmosClient(RedisUtilities.ResolveConnectionString(IntegrationTestHelpers.localsettings, RedisCosmosTestFunctions.cosmosDbConnectionSetting)))
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

            using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(RedisUtilities.ResolveConnectionString(IntegrationTestHelpers.localsettings, RedisCosmosTestFunctions.localhostSetting)))
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
    }
}
