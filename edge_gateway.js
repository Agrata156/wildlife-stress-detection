const http = require("http");

const config = {
    port: Number(process.env.GATEWAY_PORT || 8081),
    region: process.env.GATEWAY_REGION || "",
    gatewayId: process.env.GATEWAY_ID || `gateway-${(process.env.GATEWAY_REGION || "all").toLowerCase()}`,
    apiUrl: process.env.API_URL || "",
    apiKey: process.env.API_KEY || "",
    flushMs: Number(process.env.FLUSH_MS || 2000),
    maxBatch: Number(process.env.MAX_BATCH || 100),
    maxBuffer: Number(process.env.MAX_BUFFER || 50000),
    maxInFlight: Number(process.env.MAX_IN_FLIGHT || 4),
    maxBackoffMs: Number(process.env.MAX_BACKOFF_MS || 5000),
    priority: process.env.PRIORITY !== "off",
    urgentScore: Number(process.env.URGENT_SCORE || 60),
    urgentCooldownMs: Number(process.env.URGENT_COOLDOWN_SECONDS || 300) * 1000,
    envMaxAgeMs: Number(process.env.ENV_MAX_AGE_SECONDS || 120) * 1000
};

const TIERS = {
    temp: [[40.5, 35], [39.5, 20]],
    heart_rate: [[150, 45], [140, 35], [125, 25], [115, 15]],
    ambient_temp: [[40, 25], [35, 15]],
    noise: [[95, 45], [85, 25]],
    movement: [[65, 10]]
};

const stats = {
    received: 0,
    rejected_local: 0,
    urgent_readings: 0,
    uploads: 0,
    upload_failures: 0,
    forwarded: 0,
    rejected_cloud: 0,
    dropped: 0,
    max_buffered: 0
};

const latestEnv = new Map();
const lastUrgent = new Map();
let buffer = [];
let inFlight = 0;
let backoffMs = 0;
let pausedUntil = 0;
let outageUntil = 0;
let stopping = false;

function tier(value, steps) {
    if (typeof value !== "number") return 0;
    for (const [limit, points] of steps) {
        if (value >= limit) return points;
    }
    return 0;
}

function edgeScore(reading, now) {
    const latest = latestEnv.get(reading.region);
    const env = latest && now - latest.at <= config.envMaxAgeMs ? latest : null;
    const heat = tier(reading.temp, TIERS.temp) + (env ? tier(env.ambient_temp, TIERS.ambient_temp) : 0);
    const noisePoints = tier(Math.max(reading.noise ?? 0, env?.noise ?? 0), TIERS.noise);
    const disturbance = noisePoints > 0 ? noisePoints + tier(reading.movement, TIERS.movement) : 0;
    const cause = heat > 0 && heat >= disturbance ? "heat_stress" : disturbance > 0 ? "noise_stress" : "elevated_heart_rate";
    const score = Math.min(100, Math.max(heat, disturbance) + tier(reading.heart_rate, TIERS.heart_rate));
    return { score, cause, rank: score >= 80 ? 3 : 2 };
}

function isUrgent(reading, now) {
    if (!config.priority || reading.type === "env") return false;
    const { score, cause, rank } = edgeScore(reading, now);
    if (score < config.urgentScore) return false;
    const last = lastUrgent.get(reading.animal_id);
    if (last && now - last.at < config.urgentCooldownMs && rank <= last.rank && cause === last.cause) return false;
    lastUrgent.set(reading.animal_id, { at: now, rank, cause });
    return true;
}

function check(reading) {
    if (!reading || typeof reading !== "object" || Array.isArray(reading)) return "Reading must be an object";
    const type = reading.type || "bio";
    if (type !== "bio" && type !== "env") return `Unknown reading type: ${type}`;
    if (typeof reading.region !== "string" || reading.region.trim() === "") return "region is required";
    if (config.region && reading.region !== config.region) return `This gateway only serves ${config.region}`;
    return null;
}

function trimBuffer() {
    if (buffer.length > config.maxBuffer) {
        const extra = buffer.length - config.maxBuffer;
        buffer.splice(0, extra);
        stats.dropped += extra;
    }
    stats.max_buffered = Math.max(stats.max_buffered, buffer.length);
}

function accept(reading) {
    const now = Date.now();
    stats.received++;
    if (reading.type === "env") {
        latestEnv.set(reading.region, { ambient_temp: reading.ambient_temp, noise: reading.noise, at: now });
    }
    buffer.push({ reading, receivedAt: now });
    trimBuffer();
    if (isUrgent(reading, now)) {
        stats.urgent_readings++;
        flush();
    } else if (buffer.length >= config.maxBatch) {
        flush();
    }
}

function flush() {
    while (buffer.length > 0 && inFlight < config.maxInFlight && Date.now() >= pausedUntil) {
        upload(buffer.splice(0, config.maxBatch));
    }
}

async function upload(entries) {
    inFlight++;
    await Promise.resolve();
    const sentAt = Date.now();
    const readings = entries.map(({ reading, receivedAt }) => ({ ...reading, edge_wait_ms: sentAt - receivedAt }));
    try {
        if (sentAt < outageUntil) throw new Error("simulated uplink outage");
        const response = await fetch(config.apiUrl, {
            method: "POST",
            headers: { "Content-Type": "application/json", "x-api-key": config.apiKey },
            body: JSON.stringify({ type: "batch", gateway_id: config.gatewayId, readings })
        });
        const text = await response.text();
        if (response.ok) {
            let body = {};
            try {
                body = JSON.parse(text);
            } catch {
                body = {};
            }
            stats.uploads++;
            stats.forwarded += typeof body.accepted === "number" ? body.accepted : readings.length;
            stats.rejected_cloud += Array.isArray(body.rejected) ? body.rejected.length : 0;
            backoffMs = 0;
        } else if (response.status === 400) {
            stats.rejected_cloud += readings.length;
            console.warn(`[${config.gatewayId}] batch of ${readings.length} rejected by cloud: ${text}`);
        } else {
            throw new Error(`HTTP ${response.status} ${text}`);
        }
    } catch (error) {
        stats.upload_failures++;
        buffer.unshift(...entries);
        trimBuffer();
        backoffMs = backoffMs ? Math.min(config.maxBackoffMs, backoffMs * 2) : 1000;
        pausedUntil = Date.now() + backoffMs;
        console.warn(`[${config.gatewayId}] upload failed (${error.message}), ${buffer.length} readings held, retrying in ${backoffMs} ms`);
    } finally {
        inFlight--;
    }
    if (buffer.length >= config.maxBatch || (stopping && buffer.length > 0)) flush();
}

function snapshot() {
    return {
        gateway_id: config.gatewayId,
        ...stats,
        buffered: buffer.length,
        in_flight: inFlight,
        outage_active: Date.now() < outageUntil
    };
}

function reply(res, status, body) {
    res.writeHead(status, { "Content-Type": "application/json" });
    res.end(JSON.stringify(body));
}

const server = http.createServer((req, res) => {
    const url = new URL(req.url, "http://localhost");
    if (req.method === "GET" && url.pathname === "/stats") return reply(res, 200, snapshot());
    if (req.method === "POST" && url.pathname === "/admin/outage") {
        const seconds = Number(url.searchParams.get("seconds") || 30);
        outageUntil = Date.now() + seconds * 1000;
        console.log(`[${config.gatewayId}] simulated uplink outage started for ${seconds} s at ${new Date().toISOString()}`);
        return reply(res, 200, { message: `Simulated uplink outage for ${seconds} s` });
    }
    if (req.method !== "POST" || url.pathname !== "/readings") return reply(res, 404, { message: "Not found" });
    if (stopping) return reply(res, 503, { message: "Gateway stopping" });
    let raw = "";
    req.on("data", (chunk) => {
        raw += chunk;
        if (raw.length > 65536) req.destroy();
    });
    req.on("end", () => {
        let reading;
        try {
            reading = JSON.parse(raw);
        } catch {
            stats.rejected_local++;
            return reply(res, 400, { message: "Invalid JSON rejected" });
        }
        const error = check(reading);
        if (error) {
            stats.rejected_local++;
            return reply(res, 400, { message: error });
        }
        accept(reading);
        reply(res, 202, { message: "Reading buffered", gateway: config.gatewayId });
    });
});

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

async function stop() {
    if (stopping) return;
    stopping = true;
    console.log(`[${config.gatewayId}] stopping, sending ${buffer.length} buffered readings`);
    server.close();
    const giveUpAt = Date.now() + 15000;
    while ((buffer.length > 0 || inFlight > 0) && Date.now() < giveUpAt) {
        flush();
        await sleep(200);
    }
    console.log(`[${config.gatewayId}] final ${JSON.stringify(snapshot())}`);
    process.exit(0);
}

function start() {
    if (!config.apiUrl || !config.apiKey) {
        console.error("Set API_URL and API_KEY first");
        process.exit(1);
    }
    server.listen(config.port, () => {
        console.log(`[${config.gatewayId}] listening on http://localhost:${config.port}/readings (region ${config.region || "any"}, flush ${config.flushMs} ms, batch ${config.maxBatch}, priority ${config.priority ? "on" : "off"})`);
    });
    setInterval(flush, config.flushMs);
    setInterval(() => {
        console.log(`[${config.gatewayId}] ${new Date().toISOString()} ${JSON.stringify(snapshot())}`);
    }, 10000);
    process.on("SIGINT", stop);
}

start();
