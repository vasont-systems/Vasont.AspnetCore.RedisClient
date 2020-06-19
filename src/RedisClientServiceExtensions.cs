//-----------------------------------------------------------------------
// <copyright file="RedisClientServiceExtensions.cs" company="GlobalLink Vasont">
// Copyright (c) GlobalLink Vasont. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Vasont.AspnetCore.RedisClient
{
    using System;
    using Microsoft.Extensions.Caching.Distributed;
    using Microsoft.Extensions.DependencyInjection;

    /// <summary>
    /// This class is used for adding Distributed Cache according to configuration
    /// </summary>
    public static class RedisClientServiceExtensions
    {
        /// <summary>
        /// Adds Redis distributed caching services to the specified <see cref="IServiceCollection" />.
        /// </summary>
        /// <param name="services">The <see cref="IServiceCollection" /> to add services to.</param>
        /// <param name="setupAction">An <see cref="Action{RedisCacheOptions}" /> to configure the provided <see cref="RedisCacheOptions" />.</param>
        /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
        public static IServiceCollection AddRedisClientCache(this IServiceCollection services, Action<RedisClientCacheOptions> setupAction)
        {
            if (services == null)
            {
                throw new ArgumentNullException(nameof(services));
            }

            if (setupAction == null)
            {
                throw new ArgumentNullException(nameof(setupAction));
            }

            services.AddOptions();
            services.Configure(setupAction);
            services.Add(ServiceDescriptor.Singleton<IAdvancedDistributedCache, RedisCache>());
            services.Add(ServiceDescriptor.Singleton<IDistributedCache>(r =>
            {
                return r.GetRequiredService<IAdvancedDistributedCache>();
            }));

            return services;
        }
    }
}