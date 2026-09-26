const {
  getRegionNames,
  assignRegion,
  getRegion,
  randomCoordinates,
  getBaseline,
  getActiveScenario
} = require("./region_router");

const config = {
  apiUrl: process.env.API_URL || "",
  apiKey: process.env.API_KEY || "",
  animalCount: parseInt(process.env.ANIMAL_COUNT || "5", 10),
  intervalMin: parseInt(process.env.INTERVAL_MIN_MS || "1000", 10),
  intervalMax: parseInt(process.env.INTERVAL_MAX_MS || "5000", 10),
  dryRun: process.env.DRY_RUN === "true",
  quiet: process.env.QUIET === "true"
};

const LIMITS = {
  heart_rate: [80, 160],
  movement: [0, 100],
  temp: [30, 45],
  noise: [0, 120],
  light: [0, 1000]
};

const STEP = { heart_rate: 3, movement: 5, temp: 0.2, noise: 3, light: 25 };

const GPS_STEP = 0.0005;

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

function createAnimal(index) {
  const region = assignRegion(index);
  const env = getBaseline(region);
  const baseline = {
    heart_rate: 90 + Math.random() * 15,
    movement: 25 + Math.random() * 20,
    temp: 37 + Math.random(),
    noise: env.noise,
    light: env.light
  };
  return {
    animal_id: `animal-${index + 1}`,
    region,
    location: randomCoordinates(region),
    baseline,
    state: { ...baseline }
  };
}

function moveAnimal(animal) {
  const { bounds } = getRegion(animal.region);
  const lat = animal.location.lat + (Math.random() * 2 - 1) * GPS_STEP;
  const lon = animal.location.lon + (Math.random() * 2 - 1) * GPS_STEP;
  animal.location = {
    lat: Number(clamp(lat, [bounds.latMin, bounds.latMax]).toFixed(5)),
    lon: Number(clamp(lon, [bounds.lonMin, bounds.lonMax]).toFixed(5))
  };
}

function applyScenario(reading, region) {
  const scenario = getActiveScenario(region);
  if (scenario === "heatwave") {
    reading.temp = clamp(reading.temp + 4, LIMITS.temp);
    reading.heart_rate = clamp(reading.heart_rate + 35, LIMITS.heart_rate);
    reading.movement = clamp(reading.movement - 15, LIMITS.movement);
    reading.light = clamp(reading.light + 250, LIMITS.light);
  }
  if (scenario === "noise_spike") {
    reading.noise = clamp(95 + Math.random() * 25, LIMITS.noise);
    reading.heart_rate = clamp(reading.heart_rate + 25, LIMITS.heart_rate);
    reading.movement = clamp(reading.movement + 30, LIMITS.movement);
  }
  return scenario;
}

function buildReading(animal) {
  for (const field of Object.keys(animal.state)) {
    animal.state[field] = drift(animal.state[field], animal.baseline[field], field);
  }
  moveAnimal(animal);
  const reading = { ...animal.state };
  const scenario = applyScenario(reading, animal.region);
  return {
    type: "bio",
    animal_id: animal.animal_id,
    region: animal.region,
    lat: animal.location.lat,
    lon: animal.location.lon,
    heart_rate: round1(reading.heart_rate),
    movement: round1(reading.movement),
    temp: round1(reading.temp),
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
    if (!config.quiet) console.log(`${reading.animal_id} -> ${response.status} ${text}`);
  } catch (error) {
    stats.failed++;
    console.error(`${reading.animal_id} -> error: ${error.message}`);
  }
}

function nextDelay() {
  return config.intervalMin + Math.random() * (config.intervalMax - config.intervalMin);
}

function schedule(animal) {
  setTimeout(async () => {
    await send(buildReading(animal));
    schedule(animal);
  }, nextDelay());
}

function start() {
  if (!config.dryRun && !config.apiUrl) {
    console.error("Set API_URL or run with DRY_RUN=true");
    process.exit(1);
  }
  const animals = Array.from({ length: config.animalCount }, (_, i) => createAnimal(i));
  console.log(`Starting ${animals.length} bio-rhythmic nodes across ${getRegionNames().join(", ")} (${config.dryRun ? "dry run" : config.apiUrl})`);
  animals.forEach(schedule);
  setInterval(() => {
    console.log(`Summary: sent=${stats.sent} failed=${stats.failed}`);
  }, 10000);
  process.on("SIGINT", () => {
    console.log(`Final: sent=${stats.sent} failed=${stats.failed}`);
    process.exit(0);
  });
}

if (require.main === module) start();

module.exports = { createAnimal, buildReading };