using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Options;
using ServiceStack;
using ServiceStack.Redis;

namespace ylast.Caching.ServiceStackRedis {
    public class RedisCacheSentinel : IDistributedCache, IDisposable {
        // KEYS[1] = = key
        // ARGV[1] = absolute-expiration - ticks as long (-1 for none)
        // ARGV[2] = sliding-expiration - ticks as long (-1 for none)
        // ARGV[3] = relative-expiration (long, in seconds, -1 for none) - Min(absolute-expiration - Now, sliding-expiration)
        // ARGV[4] = data - byte[]
        // this order should not change LUA script depends on it
        private const string SetScript = (@"
                redis.call('HMSET', KEYS[1], 'absexp', ARGV[1], 'sldexp', ARGV[2], 'data', ARGV[4])
                if ARGV[3] ~= '-1' then
                  redis.call('EXPIRE', KEYS[1], ARGV[3])
                end
                return 1");
        private const string AbsoluteExpirationKey = "absexp";
        private const string SlidingExpirationKey = "sldexp";
        private const string DataKey = "data";
        private const long NotPresent = -1;

        private readonly RedisSentinelOptions _options;
        private readonly string _instance;

        private RedisSentinel _sentinel;
        private IRedisClientsManager _manager;

        private readonly SemaphoreSlim _connectionLock = new SemaphoreSlim(initialCount: 1, maxCount: 1);

        public RedisCacheSentinel(Microsoft.Extensions.Options.IOptions<RedisSentinelOptions> optionsAccessor) {
            if (optionsAccessor == null) {
                throw new ArgumentNullException(nameof(optionsAccessor));
            }

            _options = optionsAccessor.Value;

            // This allows partitioning a single backend cache for use with multiple apps/services.
            _instance = _options.InstanceName ?? string.Empty;
        }

        public byte[] Get(string key) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return GetAndRefresh(key, getData : true);
        }

        public async Task<byte[]> GetAsync(string key, CancellationToken token = default(CancellationToken)) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            token.ThrowIfCancellationRequested();

            return await GetAndRefreshAsync(key, getData : true, token : token).ConfigureAwait(false);
        }

        public void Set(string key, byte[] value, DistributedCacheEntryOptions options) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            if (value == null) {
                throw new ArgumentNullException(nameof(value));
            }

            if (options == null) {
                throw new ArgumentNullException(nameof(options));
            }

            Connect();

            var creationTime = DateTimeOffset.UtcNow;

            var absoluteExpiration = GetAbsoluteExpiration(creationTime, options);

            using(var redis = _manager.GetClient()) {
                redis.ExecCachedLua(SetScript, sha1 =>
                    ((IRedisNativeClient) redis).EvalShaCommand(sha1, 1, new byte[][] {
                        (_options.InstanceName + key).ToUtf8Bytes(),
                        (absoluteExpiration?.Ticks ?? NotPresent).ToUtf8Bytes(),
                        (options.SlidingExpiration?.Ticks ?? NotPresent).ToUtf8Bytes(),
                        (GetExpirationInSeconds(creationTime, absoluteExpiration, options) ?? NotPresent).ToUtf8Bytes(),
                        value
                    })
                );
            }
        }

        public async Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token = default(CancellationToken)) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            if (value == null) {
                throw new ArgumentNullException(nameof(value));
            }

            if (options == null) {
                throw new ArgumentNullException(nameof(options));
            }

            token.ThrowIfCancellationRequested();

            await ConnectAsync(token);

            var creationTime = DateTimeOffset.UtcNow;

            var absoluteExpiration = GetAbsoluteExpiration(creationTime, options);

            using(var redis = _manager.GetClient()) {
                await Task.Run(() => redis.ExecCachedLua(SetScript, sha1 =>
                    ((IRedisNativeClient) redis).EvalShaCommand(sha1, 1, new byte[][] {
                        (_options.InstanceName + key).ToUtf8Bytes(),
                        (absoluteExpiration?.Ticks ?? NotPresent).ToUtf8Bytes(),
                        (options.SlidingExpiration?.Ticks ?? NotPresent).ToUtf8Bytes(),
                        (GetExpirationInSeconds(creationTime, absoluteExpiration, options) ?? NotPresent).ToUtf8Bytes(),
                        value
                    })
                )).ConfigureAwait(false);
            }
        }

        public void Refresh(string key) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            GetAndRefresh(key, getData : false);
        }

        public async Task RefreshAsync(string key, CancellationToken token = default(CancellationToken)) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            token.ThrowIfCancellationRequested();

            await GetAndRefreshAsync(key, getData : false, token : token).ConfigureAwait(false);
        }

        private void Connect() {
            if (_sentinel != null) {
                return;
            }

            _connectionLock.Wait();
            try {

                if (_sentinel == null) {
                    // sentinel settings
                    _sentinel = new RedisSentinel(_options.Hosts, _options.MasterGroup) {
                        ScanForOtherSentinels = _options.ScanForOtherSentinels,
                            RedisManagerFactory = (master, slaves) => new RedisManagerPool(master),
                            // set the client connection string
                            HostFilter = host => {
                                host = "localhost";
                                var h = String.Empty;
                                h += String.IsNullOrWhiteSpace(_options.AuthPass) ? "" : $"{_options.AuthPass}@";
                                h += $"{host}?db=1&RetryTimeout=5000";
                                return h;
                            },
                    };

                    // start monitoring
                    _manager = _sentinel.Start();
                }

            } finally {
                _connectionLock.Release();
            }

        }

        private async Task ConnectAsync(CancellationToken token = default(CancellationToken)) {
            token.ThrowIfCancellationRequested();

            if (_sentinel != null) {
                return;
            }

            await _connectionLock.WaitAsync(token);
            try {
                if (_sentinel == null) {
                    // sentinel settings
                    _sentinel = new RedisSentinel(_options.Hosts, _options.MasterGroup) {
                        ScanForOtherSentinels = _options.ScanForOtherSentinels,
                            RedisManagerFactory = (master, slaves) => new RedisManagerPool(master),
                            // set the client connection string
                            HostFilter = host => {
                                host = "localhost";
                                var h = String.Empty;
                                h += String.IsNullOrWhiteSpace(_options.AuthPass) ? "" : $"{_options.AuthPass}@";
                                h += $"{host}?db=1&RetryTimeout=5000";
                                return h;
                            },
                    };

                    // start monitoring
                    _manager = await Task.Run(() => _sentinel.Start()).ConfigureAwait(false);
                }
            } finally {
                _connectionLock.Release();
            }
        }

        private byte[] GetAndRefresh(string key, bool getData) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            Connect();

            byte[][] results = null;

            // This also resets the LRU status as desired.
            // TODO: Can this be done in one operation on the server side? Probably, the trick would just be the DateTimeOffset math.
            using(var redis = _manager.GetReadOnlyClient() as IRedisNativeClient) {
                if (getData) {
                    results = redis.HMGet((_options.InstanceName + key), new [] {
                        AbsoluteExpirationKey.ToUtf8Bytes(),
                            SlidingExpirationKey.ToUtf8Bytes(),
                            DataKey.ToUtf8Bytes()
                    });
                } else {
                    results = redis.HMGet((_options.InstanceName + key), new [] {
                        AbsoluteExpirationKey.ToUtf8Bytes(),
                            SlidingExpirationKey.ToUtf8Bytes()
                    });
                }
            }

            // TODO: Error handling
            if (results.Length >= 2) {
                MapMetadata(results, out DateTimeOffset? absExpr, out TimeSpan? sldExpr);
                Refresh(key, absExpr, sldExpr);
            }

            if (results.Length >= 3 && results[2]?.Length > 0) {
                return results[2];
            }

            return null;
        }

        private async Task<byte[]> GetAndRefreshAsync(string key, bool getData, CancellationToken token = default(CancellationToken)) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            token.ThrowIfCancellationRequested();

            await ConnectAsync(token);

            byte[][] results = null;

            // This also resets the LRU status as desired.
            // TODO: Can this be done in one operation on the server side? Probably, the trick would just be the DateTimeOffset math.
            using(var redis = _manager.GetReadOnlyClient() as IRedisNativeClient) {
                if (getData) {
                    results = await Task.Run(() => redis.HMGet((_options.InstanceName + key), new [] {
                        AbsoluteExpirationKey.ToUtf8Bytes(),
                            SlidingExpirationKey.ToUtf8Bytes(),
                            DataKey.ToUtf8Bytes()
                    })).ConfigureAwait(false);
                } else {
                    results = await Task.Run(() => redis.HMGet((_options.InstanceName + key), new [] {
                        AbsoluteExpirationKey.ToUtf8Bytes(),
                            SlidingExpirationKey.ToUtf8Bytes()
                    })).ConfigureAwait(false);
                }
            }

            // TODO: Error handling
            if (results.Length >= 2) {
                MapMetadata(results, out DateTimeOffset? absExpr, out TimeSpan? sldExpr);
                await RefreshAsync(key, absExpr, sldExpr).ConfigureAwait(false);
            }

            if (results.Length >= 3 && results[2]?.Length > 0) {
                return results[2];
            }

            return null;
        }

        public void Remove(string key) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            Connect();

            using(var redis = _manager.GetClient()) {
                redis.Remove((_options.InstanceName + key));
            }

            // TODO: Error handling
        }

        public async Task RemoveAsync(string key, CancellationToken token = default(CancellationToken)) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            await ConnectAsync(token);

            using(var redis = _manager.GetClient()) {
                await Task.Run(() => redis.Remove((_options.InstanceName + key))).ConfigureAwait(false);
            }
            // TODO: Error handling
        }

        private void MapMetadata(byte[][] results, out DateTimeOffset? absoluteExpiration, out TimeSpan? slidingExpiration) {
            absoluteExpiration = null;
            slidingExpiration = null;

            if (results[0] != null) {
                var absoluteExpirationTicks = long.Parse(results[0].FromUtf8Bytes());
                if (absoluteExpirationTicks != NotPresent) {
                    absoluteExpiration = new DateTimeOffset(absoluteExpirationTicks, TimeSpan.Zero);
                }
            }

            if (results[1] != null) {
                var slidingExpirationTicks = long.Parse(results[1].FromUtf8Bytes());
                if (slidingExpirationTicks != NotPresent) {
                    slidingExpiration = new TimeSpan(slidingExpirationTicks);
                }
            }
        }

        private void Refresh(string key, DateTimeOffset? absExpr, TimeSpan? sldExpr) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            // Note Refresh has no effect if there is just an absolute expiration (or neither).
            TimeSpan? expr = null;
            if (sldExpr.HasValue) {
                if (absExpr.HasValue) {
                    var relExpr = absExpr.Value - DateTimeOffset.Now;
                    expr = relExpr <= sldExpr.Value ? relExpr : sldExpr;
                } else {
                    expr = sldExpr;
                }

                using(var redis = _manager.GetClient() as IRedisNativeClient) {
                    redis.Expire((_options.InstanceName + key), (int) expr.Value.TotalSeconds);
                }

                // TODO: Error handling
            }
        }

        private async Task RefreshAsync(string key, DateTimeOffset? absExpr, TimeSpan? sldExpr, CancellationToken token = default(CancellationToken)) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            token.ThrowIfCancellationRequested();

            // Note Refresh has no effect if there is just an absolute expiration (or neither).
            TimeSpan? expr = null;
            if (sldExpr.HasValue) {
                if (absExpr.HasValue) {
                    var relExpr = absExpr.Value - DateTimeOffset.Now;
                    expr = relExpr <= sldExpr.Value ? relExpr : sldExpr;
                } else {
                    expr = sldExpr;
                }

                using(var redis = _manager.GetClient() as IRedisNativeClient) {
                    await Task.Run(() => redis.Expire((_options.InstanceName + key), (int) expr.Value.TotalSeconds)).ConfigureAwait(false);
                }

                // TODO: Error handling
            }
        }

        private static long? GetExpirationInSeconds(DateTimeOffset creationTime, DateTimeOffset? absoluteExpiration, DistributedCacheEntryOptions options) {
            if (absoluteExpiration.HasValue && options.SlidingExpiration.HasValue) {
                return (long) Math.Min(
                    (absoluteExpiration.Value - creationTime).TotalSeconds,
                    options.SlidingExpiration.Value.TotalSeconds);
            } else if (absoluteExpiration.HasValue) {
                return (long) (absoluteExpiration.Value - creationTime).TotalSeconds;
            } else if (options.SlidingExpiration.HasValue) {
                return (long) options.SlidingExpiration.Value.TotalSeconds;
            }
            return null;
        }

        private static DateTimeOffset? GetAbsoluteExpiration(DateTimeOffset creationTime, DistributedCacheEntryOptions options) {
            if (options.AbsoluteExpiration.HasValue && options.AbsoluteExpiration <= creationTime) {
                throw new ArgumentOutOfRangeException(
                    nameof(DistributedCacheEntryOptions.AbsoluteExpiration),
                    options.AbsoluteExpiration.Value,
                    "The absolute expiration value must be in the future.");
            }
            var absoluteExpiration = options.AbsoluteExpiration;
            if (options.AbsoluteExpirationRelativeToNow.HasValue) {
                absoluteExpiration = creationTime + options.AbsoluteExpirationRelativeToNow;
            }

            return absoluteExpiration;
        }

        public void Dispose() {
            _manager?.Dispose();
            _sentinel?.Dispose();
        }

    }
}
