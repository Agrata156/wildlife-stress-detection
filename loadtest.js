const fs = require("fs");
const bio = require("./bio_node");
const env = require("./env_node");
const { getRegionNames } = require("./region_router");

const config = {
    apiUrl: process.env.API_URL || "",
    apiKey: process.env.API_KEY || "",
    stages: (process.env.STAGES || "50,100,250,500").split(",").map(Number),
    stageSeconds: Number(process.env.STAGE_SECONDS || 60),
    restSeconds: Number(process.env.REST_SECONDS || 30),
    intervalMin: Number(process.env.INTERVAL_MIN_MS || 1000),
    intervalMax: Number(process.env.INTERVAL_MAX_MS || 5000),
    envRatio: Number(process.env.ENV_RATIO || 0.1)
};

const gateways = {};
for (const pair of (process.env.GATEWAY_URLS || "").split(",")) {
    const split = pair.indexOf("=");
    if (split > 0) gateways[pair.slice(0, split).trim()] = pair.slice(split + 1).trim();
}
const edgeMode = Object.keys(gateways).length > 0;
const mode = edgeMode ? "edge" : "direct";

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function percentile(sorted, p) {
    if (sorted.length === 0) return 0;
    const index = Math.ceil((p / 100) * sorted.length) - 1;
    return sorted[Math.min(sorted.length - 1, Math.max(0, index))];
}

function splitNodes(total) {
    const envCount = Math.max(2, Math.round(total * config.envRatio));
    return { envCount, bioCount: Math.max(1, total - envCount) };
}

function targetFor(reading) {
    return edgeMode ? gateways[reading.region] : config.apiUrl;
}

async function post(reading, stats) {
    const started = Date.now();
    try {
        const headers = { "Content-Type": "application/json" };
        if (!edgeMode) headers["x-api-key"] = config.apiKey;
        const response = await fetch(targetFor(reading), {
            method: "POST",
            headers,
            body: JSON.stringify(reading)
        });
        await response.text();
        stats.latencies.push(Date.now() - started);
        stats.codes[response.status] = (stats.codes[response.status] || 0) + 1;
        if (response.ok) stats.ok++;
        else stats.failed++;
    } catch (error) {
        stats.failed++;
        stats.codes.network = (stats.codes.network || 0) + 1;
    }
    stats.sent++;
}

async function runNode(node, build, stats, endAt) {
    await sleep(Math.random() * config.intervalMax);
    while (Date.now() < endAt) {
        await post(build(node), stats);
        await sleep(config.intervalMin + Math.random() * (config.intervalMax - config.intervalMin));
    }
}

async function runStage(total, bioPool, envPool) {
    const { envCount, bioCount } = splitNodes(total);
    const stats = { sent: 0, ok: 0, failed: 0, codes: {}, latencies: [] };
    const startedAt = new Date();
    const endAt = startedAt.getTime() + config.stageSeconds * 1000;
    console.log(`\nStage ${total} nodes (${bioCount} bio, ${envCount} env) started ${startedAt.toISOString()}`);
    const progress = setInterval(() => {
        console.log(`    sent=${stats.sent} ok=${stats.ok} failed=${stats.failed}`);
    }, 10000);
    const workers = [
        ...bioPool.slice(0, bioCount).map((node) => runNode(node, bio.buildReading, stats, endAt)),
        ...envPool.slice(0, envCount).map((node) => runNode(node, env.buildReading, stats, endAt))
    ];
    await Promise.all(workers);
    clearInterval(progress);
    const endedAt = new Date();
    const duration = (endedAt - startedAt) / 1000;
    const sorted = stats.latencies.sort((a, b) => a - b);
    return {
        mode,
        nodes: total,
        bio_nodes: bioCount,
        env_nodes: envCount,
        started_at: startedAt.toISOString(),
        ended_at: endedAt.toISOString(),
        sent: stats.sent,
        ok: stats.ok,
        failed: stats.failed,
        success_rate: stats.sent ? Number(((stats.ok / stats.sent) * 100).toFixed(2)) : 0,
        rps: Number((stats.sent / duration).toFixed(1)),
        p50_ms: percentile(sorted, 50),
        p95_ms: percentile(sorted, 95),
        p99_ms: percentile(sorted, 99),
        max_ms: sorted.length ? sorted[sorted.length - 1] : 0,
        status_codes: JSON.stringify(stats.codes)
    };
}

function writeResults(results) {
    const stamp = new Date().toISOString().replace(/[:.]/g, "-");
    const headers = Object.keys(results[0]);
    const rows = results.map((row) => headers.map((key) => `"${String(row[key]).replace(/"/g, "'")}"`).join(","));
    fs.writeFileSync(`loadtest_${mode}_${stamp}.csv`, [headers.join(","), ...rows].join("\n"));
    fs.writeFileSync(`loadtest_${mode}_${stamp}.json`, JSON.stringify(results, null, 2));
    return `loadtest_${mode}_${stamp}`;
}

async function main() {
    if (edgeMode) {
        const missing = getRegionNames().filter((region) => !gateways[region]);
        if (missing.length > 0) {
            console.error(`GATEWAY_URLS has no gateway for: ${missing.join(", ")}`);
            process.exit(1);
        }
    } else if (!config.apiUrl || !config.apiKey) {
        console.error("Set API_URL and API_KEY, or GATEWAY_URLS for edge mode");
        process.exit(1);
    }
    const maxTotal = Math.max(...config.stages);
    const { envCount, bioCount } = splitNodes(maxTotal);
    const bioPool = Array.from({ length: bioCount }, (_, i) => bio.createAnimal(i));
    const envPool = Array.from({ length: envCount }, (_, i) => env.createNode(i));
    const testStart = new Date().toISOString();
    console.log(`Load test (${edgeMode ? "through edge gateways" : "direct to cloud"}): stages ${config.stages.join(" -> ")}, ${config.stageSeconds}s each, ${config.restSeconds}s rest`);
    const results = [];
    for (let i = 0; i < config.stages.length; i++) {
        results.push(await runStage(config.stages[i], bioPool, envPool));
        console.table([results[results.length - 1]]);
        if (i < config.stages.length - 1) {
            console.log(`Resting ${config.restSeconds}s`);
            await sleep(config.restSeconds * 1000);
        }
    }
    const name = writeResults(results);
    console.log("\nSummary");
    console.table(results.map(({ mode, nodes, sent, ok, failed, success_rate, rps, p50_ms, p95_ms, max_ms }) => ({ mode, nodes, sent, ok, failed, success_rate, rps, p50_ms, p95_ms, max_ms })));
    console.log(`Test window (UTC): ${testStart} to ${new Date().toISOString()}`);
    console.log(`Saved ${name}.csv and .json`);
}

main();
