const Redis = require("ioredis");
const { config } = require("../redisConfig");

/**
 * Creates and returns a new ioredis client.
 * Using a factory function allows for easier lifecycle management
 * and multiple connections if needed.
 */
function createRedisClient(overrides = {}) {
  const finalConfig = { ...config, ...overrides };

  const client = new Redis(finalConfig);

  client.on("error", (err) => {
    // Log error but don't crash - ioredis will attempt to reconnect automatically
    console.error("Redis Client Error:", err);
  });

  return client;
}

// Singleton instance for convenience
const redis = createRedisClient();

module.exports = {
  createRedisClient,
  redis,
};
