﻿using Microsoft.Azure.WebJobs.Extensions.Redis.Samples.Models;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;

namespace Microsoft.Azure.WebJobs.Extensions.Redis.Samples
{
    public static class WriteThroughSample
    {
        public const string localhostSetting = "redisLocalhost";
        public const string cosmosDbConnectionSetting = "CosmosDBConnection";

        private static readonly IDatabaseAsync s_redisDb =
            ConnectionMultiplexer.ConnectAsync(Environment.GetEnvironmentVariable(localhostSetting)).Result.GetDatabase();


        //write-through caching: Write to Redis then synchronously write to Cosmos DB
        [FunctionName(nameof(WriteThrough))]
        public static void WriteThrough(
           [RedisPubSubTrigger(localhostSetting, "__keyevent@0__:set")] string newKey,
           [CosmosDB(
                databaseName: "DatabaseId",
                containerName: "ContainerId",
                Connection = cosmosDbConnectionSetting)]out dynamic redisData,
           ILogger logger)
        {
          //get the Redis data synchronously
            IDatabase redisDb = s_redisDb.Multiplexer.GetDatabase();

            //assign the data from Redis to a dynamic object that will be written to Cosmos DB
            redisData = new RedisData(
                id: Guid.NewGuid().ToString(),
                key: newKey,
                value: redisDb.StringGet(newKey),
                timestamp: DateTime.UtcNow
            );

            logger.LogInformation($"Key: \"{newKey}\", Value: \"{redisData.value}\" addedd to Cosmos DB container: \"{"ContainerId"}\" at id: \"{redisData.id}\"");
        }
    }
}
