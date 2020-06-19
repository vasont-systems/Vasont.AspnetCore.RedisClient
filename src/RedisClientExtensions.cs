//-----------------------------------------------------------------------
// <copyright file="RedisClientExtensions.cs" company="GlobalLink Vasont">
// Copyright (c) GlobalLink Vasont. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Vasont.AspnetCore.RedisClient
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using StackExchange.Redis;

    /// <summary>
    /// Contains extensions to support enhanced interactions with a Redis distributed cache.
    /// </summary>
    public static class RedisClientExtensions
    {
        #region Private Constants

        /// <summary>
        /// Contains the command to get the hash member value
        /// </summary>
        private const string HmGetScript = (@"return redis.call('HMGET', KEYS[1], unpack(ARGV))");

        #endregion

        /// <summary>
        /// This method is used to find one or more cache keys that match a specified pattern.
        /// </summary>
        /// <param name="cache">Contains the cache database to search.</param>
        /// <param name="pattern">Contains the key search pattern.</param>
        /// <param name="keyPrefix">Contains an optional key prefix.</param>
        /// <returns>Returns an enumerable list of key names matching the pattern.</returns>
        internal static IEnumerable<string> FindKeys(this IDatabase cache, string pattern, string keyPrefix = "")
        {
            pattern = $"{keyPrefix}{pattern}";
            var keys = new HashSet<string>();

            long nextCursor = 0;

            do
            {
                var redisResult = cache.Execute("SCAN", nextCursor.ToString(), "MATCH", pattern, "COUNT", "1000");
                var innerResult = (RedisResult[])redisResult;

                nextCursor = long.Parse((string)innerResult[0]);

                var resultLines = ((string[])innerResult[1]).ToArray();
                keys.UnionWith(resultLines);
            }
            while (nextCursor != 0);

            return !string.IsNullOrEmpty(keyPrefix) ? keys.Select(k => k.Substring(keyPrefix.Length)) : keys;
        }

        /// <summary>
        /// This method is used to find one or more cache keys that match a specified pattern.
        /// </summary>
        /// <param name="cache">Contains the cache database to search.</param>
        /// <param name="pattern">Contains the key search pattern.</param>
        /// <param name="keyPrefix">Contains an optional key prefix.</param>
        /// <param name="token">Contains an optional cancellation token.</param>
        /// <returns>Returns an enumerable list of key names matching the pattern.</returns>
        internal static async Task<IEnumerable<string>> FindKeysAsync(this IDatabase cache, string pattern, string keyPrefix = "", CancellationToken token = default)
        {
            pattern = $"{keyPrefix}{pattern}";
            var keys = new HashSet<string>();

            long nextCursor = 0;

            do
            {
                token.ThrowIfCancellationRequested();

                var redisResult = await cache.ExecuteAsync("SCAN", nextCursor.ToString(), "MATCH", pattern, "COUNT", "1000").ConfigureAwait(false);
                var innerResult = (RedisResult[])redisResult;

                nextCursor = long.Parse((string)innerResult[0]);

                var resultLines = ((string[])innerResult[1]).ToArray();
                keys.UnionWith(resultLines);
            }
            while (nextCursor != 0);

            return !string.IsNullOrEmpty(keyPrefix) ? keys.Select(k => k.Substring(keyPrefix.Length)) : keys;
        }

        /// <summary>
        /// This method is used to retrieve a hash member set of values from the cache.
        /// </summary>
        /// <param name="cache">Contains the redis Cache database to retrieve the cache value from.</param>
        /// <param name="key">Contains the key to find.</param>
        /// <param name="members">Contains the members to retrieve from the hash.</param>
        /// <returns>Returns the array of Redis values found.</returns>
        internal static RedisValue[] HashMemberGet(this IDatabase cache, string key, params string[] members)
        {
            RedisResult result = cache.ScriptEvaluate(HmGetScript, new RedisKey[] { key }, GetRedisMembers(members));

            // TODO: Error checking?
            return (RedisValue[])result;
        }

        /// <summary>
        /// This method is used to retrieve a hash member set of values from the cache.
        /// </summary>
        /// <param name="cache">Contains the redis Cache database to retrieve the cache value from.</param>
        /// <param name="key">Contains the key to find.</param>
        /// <param name="members">Contains the members to retrieve from the hash.</param>
        /// <returns>Returns the array of Redis values found.</returns>
        internal static async Task<RedisValue[]> HashMemberGetAsync(this IDatabase cache, string key, params string[] members)
        {
            RedisResult result = await cache.ScriptEvaluateAsync(HmGetScript, new RedisKey[] { key }, GetRedisMembers(members));

            // TODO: Error checking?
            return (RedisValue[])result;
        }

        /// <summary>
        /// This method is used to build Redis members into a redis value array.
        /// </summary>
        /// <param name="members">Contains the members.</param>
        /// <returns>Returns a Redis value array.</returns>
        private static RedisValue[] GetRedisMembers(params string[] members)
        {
            RedisValue[] redisMembers = new RedisValue[members.Length];

            for (int i = 0; i < members.Length; i++)
            {
                redisMembers[i] = members[i];
            }

            return redisMembers;
        }
    }
}