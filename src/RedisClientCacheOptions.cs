//-----------------------------------------------------------------------
// <copyright file="RedisClientCacheOptions.cs" company="GlobalLink Vasont">
// Copyright (c) GlobalLink Vasont. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Vasont.AspnetCore.RedisClient
{
    using Microsoft.Extensions.Options;
    using StackExchange.Redis;

    /// <summary>
    /// Configuration options for the <see cref="RedisClientCache" /> class.
    /// </summary>
    public class RedisClientCacheOptions : IOptions<RedisClientCacheOptions>
    {
        /// <summary>
        /// The configuration used to connect to Redis.
        /// </summary>
        public string Configuration { get; set; }

        /// <summary>
        /// The configuration used to connect to Redis. This is preferred over Configuration.
        /// </summary>
        public ConfigurationOptions ConfigurationOptions { get; set; }

        /// <summary>
        /// The Redis instance name.
        /// </summary>
        public string InstanceName { get; set; }

        /// <summary>
        /// Gets the instance of the Redis cache options
        /// </summary>
        RedisClientCacheOptions IOptions<RedisClientCacheOptions>.Value => this;
    }
}