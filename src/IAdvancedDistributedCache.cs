//-----------------------------------------------------------------------
// <copyright file="IAdvancedDistributedCache.cs" company="GlobalLink Vasont">
// Copyright (c) GlobalLink Vasont. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Vasont.AspnetCore.RedisClient
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Caching.Distributed;

    /// <summary>
    /// This interface provides additional enhancements to the base distributed cache interface.
    /// </summary>
    public interface IAdvancedDistributedCache : IDistributedCache, IDisposable
    {
        /// <summary>
        /// This method is used to find one or more cache keys that match a specified pattern.
        /// </summary>
        /// <param name="cache">Contains the cache database to search.</param>
        /// <param name="pattern">Contains the key search pattern.</param>
        /// <returns>Returns an enumerable list of key names matching the pattern.</returns>
        IEnumerable<string> FindKeys(string pattern);

        /// <summary>
        /// This method is used to find one or more cache keys that match a specified pattern.
        /// </summary>
        /// <param name="cache">Contains the cache database to search.</param>
        /// <param name="pattern">Contains the key search pattern.</param>
        /// <param name="token">Contains an optional cancellation token.</param>
        /// <returns>Returns an enumerable list of key names matching the pattern.</returns>
        Task<IEnumerable<string>> FindKeysAsync(string pattern, CancellationToken token = default);
    }
}