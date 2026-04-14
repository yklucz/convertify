// Redis configuration
const config = {
  host: process.env.REDIS_HOST,
  port: Number.parseInt(process.env.REDIS_PORT),
  username: process.env.REDIS_USERNAME,
  password: process.env.REDIS_PASSWORD,
  db: Number.parseInt(process.env.REDIS_DB),
  maxRetriesPerRequest: null,
  enableReadyCheck: false,
};

module.exports = { config };
