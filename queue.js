const { Queue } = require("bullmq");
const { config } = require("./redisConfig");

// Create the conversion queue
const conversionQueue = new Queue("conversion", {
  connection: config,
  defaultJobOptions: {
    attempts: 2, // Max 2 attempts (1 initial + 1 retry)
    removeOnComplete: { age: 3600 }, // Keep completed jobs for 1 hour so the frontend can poll for "completed" status
    removeOnFail: { count: 500 }, // Keep failed jobs for inspection
  },
});

module.exports = { conversionQueue };
