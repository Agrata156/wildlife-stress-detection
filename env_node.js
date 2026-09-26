const {
  getRegionNames,
  assignRegion,
  randomCoordinates,
  getBaseline,
  getActiveScenario
} = require("./region_router");

const config = {
  apiUrl: process.env.API_URL || "",
  apiKey: process.env.API_KEY || "",
  nodeCount: parseInt(process.env.ENV_NODE_COUNT || "4", 10),
  intervalMin: parseInt(process.env.INTERVAL_MIN_MS || "1000", 10),
  intervalMax: parseInt(process.env.INTERVAL_MAX_MS || "5000", 10),
  dryRun: process.env.DRY_RUN === "true",
  quiet: process.env.QUIET === "true"
};

const LIMITS = {
  ambient_temp: [10, 45],
  humidity: [20, 90],
  noise: [0, 120],
  light: [0, 1000]
};

const STEP = { ambient_temp: 0.4, humidity: 1.5, noise: 3, light: 25 };

const stats = { sent: 0, failed: 0 };

function clamp(value, [min, max]) {
  return Math.min(max, Math.max(min, value));
}

function round1(value) {
  return Math.round(value * 10) / 10;
}

function drift(value, base, field) {
  const pull = (base - value) * 0.1;
  const change = (Math.random() * 2 - 1) * STEP[field];
  return clamp(value + pull + change, LIMITS[field]);
}

function createNode(index) {
  const region = assignRegion(index);
  const number = Math.floor(index / getRegionNames().length) + 1;
  return {
    sensor_id: `env-${region.toLowerCase()}-${number}`,
    region,
    location: randomCoordinates(region),
    baseline: getBaseline(region),
    state: getBaseline(region)
  };
}

function applyScenario(reading, region) {
  const scenario = getActiveScenario(region);
  if (scenario === "heatwave") {
    reading.ambient_temp = clamp(reading.ambient_temp + 12, LIMITS.ambient_temp);
    reading.humidity = clamp(reading.humidity - 15, LIMITS.humidity);
    reading.light = clamp(reading.light + 250, LIMITS.light);
  }
  if (scenario === "noise_spike") {
    reading.noise = clamp(95 + Math.random() * 25, LIMITS.noise);
  }
  return scenario;
}

function buildReading(node) {
  for (const field of Object.keys(node.state)) {
    node.state[field] = drift(node.state[field], node.baseline[field], field);
  }
  const reading = { ...node.state };
  const scenario = applyScenario(reading, node.region);
  return {
    type: "env",
    sensor_id: node.sensor_id,
    region: node.region,
    lat: node.location.lat,
    lon: node.location.lon,
    ambient_temp: round1(reading.ambient_temp),
    humidity: round1(reading.humidity),
    noise: round1(reading.noise),
    light: round1(reading.light),
    scenario: scenario || "normal",
    timestamp: new Date().toISOString()
  };
}

async function send(reading) {
  if (config.dryRun) {
    stats.sent++;
    if (!config.quiet) console.log("Dry run:", reading);
    return;
  }
  try {
    const headers = { "Content-Type": "application/json" };
    if (config.apiKey) headers["x-api-key"] = config.apiKey;
    const response = await fetch(config.apiUrl, {
      method: "POST",
      headers,
      body: JSON.stringify(reading)
    });
    const text = await response.text();
    if (response.ok) stats.sent++;
    else stats.failed++;
    if (!config.quiet) console.log(`${reading.sensor_id} -> ${response.status} ${text}`);
  } catch (error) {
    stats.failed++;
    console.error(`${reading.sensor_id} -> error: ${error.message}`);
  }
}

function nextDelay() {
  return config.intervalMin + Math.random() * (config.intervalMax - config.intervalMin);
}

function schedule(node) {
  setTimeout(async () => {
    await send(buildReading(node));
    schedule(node);
  }, nextDelay());
}

function start() {
  if (!config.dryRun && !config.apiUrl) {
    console.error("Set API_URL or run with DRY_RUN=true");
    process.exit(1);
  }
  const nodes = Array.from({ length: config.nodeCount }, (_, i) => createNode(i));
  console.log(`Starting ${nodes.length} environmental nodes (${config.dryRun ? "dry run" : config.apiUrl})`);
  nodes.forEach(schedule);
  setInterval(() => {
    console.log(`Summary: sent=${stats.sent} failed=${stats.failed}`);
  }, 10000);
  process.on("SIGINT", () => {
    console.log(`Final: sent=${stats.sent} failed=${stats.failed}`);
    process.exit(0);
  });
}

if (require.main === module) start();

module.exports = { createNode, buildReading };