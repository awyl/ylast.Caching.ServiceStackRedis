using Microsoft.Extensions.Options;

namespace ylast.Caching.ServiceStackRedis {
    public class RedisSentinelOptions : IOptions<RedisSentinelOptions> {
        RedisSentinelOptions IOptions<RedisSentinelOptions>.Value => this;

        /// <summary>
        /// Sentinel hosts, e.g, "sentinel:6390, "sentinel"
        /// </summary>
        public string[] Hosts { get; set; }

        /// <summary>
        /// The name of the master.
        /// </summary>
        public string MasterGroup { get; set; }

        /// <summary>
        /// Instance name, aka prefix, for sharing a single backend with multiple apps/services.
        /// </summary>
        /// <value></value>
        public string InstanceName { get; set; }

        /// <summary>
        /// Default database
        /// </summary>
        public int DefaultDatabase { get; set; }

        /// <summary>
        /// Whether to look for other sentinels, default: true.
        /// </summary>
        public bool ScanForOtherSentinels { get; set; } = true;

        /// <summary>
        /// Redis server auth password if any
        /// </summary>
        /// <value></value>
        public string AuthPass { get; set; }
    }
}
