const REGIONS = {
  Mallee: {
    bounds: { latMin: -35.6, latMax: -34.2, lonMin: 141.0, lonMax: 143.2 },
    baseline: { ambient_temp: 30, humidity: 35, noise: 35, light: 650 }
  },
  Wimmera: {
    bounds: { latMin: -37.2, latMax: -36.0, lonMin: 141.3, lonMax: 143.0 },
    baseline: { ambient_temp: 26, humidity: 45, noise: 40, light: 550 }
  }
};

const regionNames = Object.keys(REGIONS);

function getRegionNames() {
  return [...regionNames];
}

function assignRegion(index) {
  return regionNames[index % regionNames.length];
}

function getRegion(name) {
  const region = REGIONS[name];
  if (!region) throw new Error(`Unknown region: ${name}`);
  return region;
}

function randomCoordinates(name) {
  const { bounds } = getRegion(name);
  const lat = bounds.latMin + Math.random() * (bounds.latMax - bounds.latMin);
  const lon = bounds.lonMin + Math.random() * (bounds.lonMax - bounds.lonMin);
  return { lat: Number(lat.toFixed(5)), lon: Number(lon.toFixed(5)) };
}

function getBaseline(name) {
  return { ...getRegion(name).baseline };
}

function getActiveScenario(name) {
  const scenario = process.env.SCENARIO;
  if (!scenario || scenario === "normal") return null;
  const target = process.env.SCENARIO_REGION;
  if (target && target !== name) return null;
  return scenario;
}

module.exports = {
  getRegionNames,
  assignRegion,
  getRegion,
  randomCoordinates,
  getBaseline,
  getActiveScenario
};