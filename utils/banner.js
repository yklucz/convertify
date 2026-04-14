"use strict";

const os = require("node:os");

/**
 * High-fidelity ANSI banner for Convertify
 * Displays system status, HW acceleration, and service health.
 */
function printBanner(serviceName, options = {}) {
  const {
    redis,
    ffmpegHW,
    cacheCount,
    port,
    additionalTips = [],
    minimal = false,
  } = options;

  const reset = "\x1b[0m";
  const bold = "\x1b[1m";
  const cyan = "\x1b[36m";
  const green = "\x1b[32m";
  const yellow = "\x1b[33m";
  const red = "\x1b[31m";
  const gray = "\x1b[90m";

  const logo = `
   ${red} ██████╗  ██████╗ ███╗   ██╗██╗   ██╗███████╗██████╗ ████████╗██╗███████╗██╗   ██╗
   ${red}██╔════╝ ██╔═══██╗████╗  ██║██║   ██║██╔════╝██╔══██╗╚══██╔══╝██║██╔════╝╚██╗ ██╔╝
   ${red}██║      ██║   ██║██╔██╗ ██║██║   ██║█████╗  ██████╔╝   ██║   ██║█████╗   ╚████╔╝ 
   ${red}██║      ██║   ██║██║╚██╗██║╚██╗ ██╔╝██╔══╝  ██╔══██╗   ██║   ██║██╔══╝    ╚██╔╝  
   ${red}╚██████╗ ╚██████╔╝██║ ╚████║ ╚████╔╝ ███████╗██║  ██║   ██║   ██║██║        ██║   
   ${red} ╚═════╝  ╚═════╝ ╚═╝  ╚═══╝  ╚═══╝  ╚══════╝╚═╝  ╚═╝   ╚═╝   ╚═╝╚═╝        ╚═╝   
  `;

  if (minimal) {
    // True minimal: just a single line indicator
    const statusDot = green + "●" + reset;
    console.log(
      `${statusDot} ${bold}${cyan}${serviceName.toUpperCase()}${reset} is starting...`,
    );
    return; // EXIT EARLY for minimal mode
  } else {
    console.log(logo);
    console.log(
      `${bold}${cyan}--- ${serviceName.toUpperCase()} SYSTEM STATUS ---${reset}\n`,
    );
  }

  // OS Info
  const freeMem = (os.freemem() / 1024 / 1024 / 1024).toFixed(2);
  const totalMem = (os.totalmem() / 1024 / 1024 / 1024).toFixed(2);
  const memUsagePct = ((1 - os.freemem() / os.totalmem()) * 100).toFixed(1);

  console.log(`${bold}OS/ENV:${reset}`);
  console.log(
    `  ${gray}Node:${reset} ${process.version}  ${gray}Platform:${reset} ${os.platform()} (${os.arch()})`,
  );
  console.log(
    `  ${gray}CPU:${reset}  ${os.cpus()[0].model} (${os.cpus().length} cores)`,
  );
  console.log(
    `  ${gray}RAM:${reset}  ${freeMem}GB free of ${totalMem}GB (${memUsagePct}% used)`,
  );

  // Service Status
  console.log(`\n${bold}SERVICES:${reset}`);

  if (redis) {
    const isConnected =
      redis.status === "ready" ||
      redis.status === "connect" ||
      redis.connected === true;
    const redisStatus = isConnected
      ? `${green}CONNECTED${reset}`
      : `${red}DISCONNECTED${reset}`;
    console.log(
      `  ${gray}Redis:${reset} ${redisStatus} (${redis.host}:${redis.port})`,
    );
  }

  if (ffmpegHW) {
    const hwCount = Object.values(ffmpegHW).filter(Boolean).length;
    const hwColor = hwCount > 0 ? green : yellow;
    console.log(
      `  ${gray}FFmpeg:${reset} ${hwColor}${hwCount} HW Encoders detected${reset} ${gray}(v-toolbox, nvenc)${reset}`,
    );
  }

  if (cacheCount !== undefined) {
    console.log(
      `  ${gray}Cache:${reset}  ${green}${cacheCount} files loaded from disk${reset}`,
    );
  }

  if (port) {
    console.log(`\n${bold}${green}READY →${reset} http://localhost:${port}`);
  }

  // Critical Warnings / Recommendations
  if (additionalTips.length > 0) {
    console.log("");
    additionalTips.forEach((tip) => {
      console.log(`${red}${bold}[CRITICAL]${reset} ${tip}`);
    });
  }

  console.log(`\n${gray}${"-".repeat(60)}${reset}\n`);
}

module.exports = { printBanner };
