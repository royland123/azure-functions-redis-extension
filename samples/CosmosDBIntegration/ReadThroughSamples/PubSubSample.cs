﻿using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Azure.WebJobs.Extensions.Redis.Samples.Models;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Microsoft.Azure.WebJobs.Extensions.Redis.Samples
{
    public static class PubSubSample
    {
        public const string localhostSetting = "redisConnectionString";
        public const string cosmosDBConnectionSetting = "CosmosDBConnectionString";
        public const string cosmosDBDatabaseName = "DatabaseId";
        public const string cosmosDBContainerName = "ContainerId";

        private static readonly Lazy<IDatabaseAsync> s_redisDb = new Lazy<IDatabaseAsync>(() =>
            ConnectionMultiplexer.Connect(Environment.GetEnvironmentVariable(localhostSetting)).GetDatabase());


        //keyspace notifications must be set to KEAm for this to trigger
        //read-through caching: Read from Redis, if not found, read from Cosmos DB, then write to Redis
        [FunctionName(nameof(ReadThroughAsync))]
        public static async Task ReadThroughAsync(
            [RedisPubSubTrigger(localhostSetting, "__keyevent@0__:keymiss")] string missedKey,
            [CosmosDB(
                databaseName: cosmosDBDatabaseName,
                containerName: cosmosDBContainerName,
                Connection = cosmosDBConnectionSetting)]CosmosClient cosmosDB,
            ILogger logger)
        {
            //get the Cosmos DB database and the container to read from
            Container cosmosDBContainer = cosmosDB.GetContainer(cosmosDBDatabaseName, cosmosDBContainerName);
            IOrderedQueryable<RedisData> queryable = cosmosDBContainer.GetItemLinqQueryable<RedisData>();

            //get all entries in the container that contain the missed key
            using FeedIterator<RedisData> feed = queryable
                .Where(p => p.key == missedKey)
                .OrderByDescending(p => p.timestamp)
                .ToFeedIterator();
            FeedResponse<RedisData> response = await feed.ReadNextAsync();

            //if the key is found in Cosmos DB, add  the most recently updated to Redis
            RedisData item = response.FirstOrDefault(defaultValue: null);
            if (item != null)
            {
                await s_redisDb.Value.StringSetAsync(item.key, item.value);
                logger.LogInformation($"Key: \"{item.key}\", Value: \"{item.value}\" added to Redis.");
            }
            else
            {
                //if the key isnt found in Cosmos DB, throw an exception
                throw new Exception($"ERROR: Key: \"{missedKey}\" not found in Redis or Cosmos DB. Try adding the Key-Value pair to Redis or Cosmos DB.");
            }
        }
    }
}