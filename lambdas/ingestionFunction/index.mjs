import { randomUUID } from "crypto";
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";

const sqs = new SQSClient({});
const QUEUE_URL = process.env.QUEUE_URL;
const SENSOR_API_KEY = process.env.SENSOR_API_KEY;
const MAX_BATCH = Number(process.env.MAX_BATCH || 200);

const RULES = {
    bio: {
        id: "animal_id",
        numbers: {
            heart_rate: [0, 250],
            movement: [0, 100],
            temp: [20, 50],
            noise: [0, 150],
            light: [0, 2000]
        }
    },
    env: {
        id: "sensor_id",
        numbers: {
            ambient_temp: [-20, 60],
            humidity: [0, 100],
            noise: [0, 150],
            light: [0, 2000]
        }
    }
};

function response(statusCode, body) {
    return {
        statusCode,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
    };
}

function getHeader(headers, name) {
    if (!headers) return undefined;
    const key = Object.keys(headers).find((header) => header.toLowerCase() === name);
    return key ? headers[key] : undefined;
}

function isNonEmptyString(value) {
    return typeof value === "string" && value.trim().length > 0;
}

function validate(body, rule) {
    const errors = [];
    if (!isNonEmptyString(body[rule.id])) errors.push(`${rule.id} is required`);
    if (!isNonEmptyString(body.region)) errors.push("region is required");
    for (const [field, [min, max]] of Object.entries(rule.numbers)) {
        const value = body[field];
        if (typeof value !== "number" || Number.isNaN(value)) errors.push(`${field} must be a number`);
        else if (value < min || value > max) errors.push(`${field} out of range (${min} to ${max})`);
    }
    if (body.lat !== undefined && (typeof body.lat !== "number" || body.lat < -90 || body.lat > 90)) errors.push("lat is invalid");
    if (body.lon !== undefined && (typeof body.lon !== "number" || body.lon < -180 || body.lon > 180)) errors.push("lon is invalid");
    if (body.edge_wait_ms !== undefined && (typeof body.edge_wait_ms !== "number" || body.edge_wait_ms < 0 || body.edge_wait_ms > 86400000)) errors.push("edge_wait_ms is invalid");
    return errors;
}

function checkReading(body) {
    if (!body || typeof body !== "object" || Array.isArray(body)) return { errors: ["Reading must be an object"] };
    const type = body.type || "bio";
    const rule = RULES[type];
    if (!rule) return { errors: [`Unknown reading type: ${type}`] };
    return { type, rule, errors: validate(body, rule) };
}

function buildReading(body, type, rule, receivedAt, batchId) {
    const reading = {
        type,
        [rule.id]: body[rule.id].trim(),
        region: body.region.trim(),
        timestamp: new Date(receivedAt - (typeof body.edge_wait_ms === "number" ? Math.round(body.edge_wait_ms) : 0)).toISOString(),
        ingested_at: new Date(receivedAt).toISOString(),
        sent_at: typeof body.timestamp === "string" && !Number.isNaN(Date.parse(body.timestamp)) ? body.timestamp : null,
        scenario: isNonEmptyString(body.scenario) ? body.scenario : "normal",
        batch_id: batchId
    };
    for (const field of Object.keys(rule.numbers)) reading[field] = body[field];
    if (typeof body.lat === "number") reading.lat = body.lat;
    if (typeof body.lon === "number") reading.lon = body.lon;
    if (typeof body.edge_wait_ms === "number") reading.edge_wait_ms = Math.round(body.edge_wait_ms);
    return reading;
}

async function queue(message) {
    await sqs.send(new SendMessageCommand({ QueueUrl: QUEUE_URL, MessageBody: JSON.stringify(message) }));
}

async function handleBatch(body) {
    const items = body.readings;
    if (!Array.isArray(items) || items.length === 0) return response(400, { message: "Batch must contain readings" });
    if (items.length > MAX_BATCH) return response(400, { message: `Batch larger than ${MAX_BATCH} readings rejected` });
    const now = Date.now();
    const batchId = randomUUID();
    const gatewayId = isNonEmptyString(body.gateway_id) ? body.gateway_id.trim() : "unknown";
    const seen = new Map();
    const readings = [];
    const rejected = [];
    items.forEach((item, index) => {
        const { type, rule, errors } = checkReading(item);
        if (errors.length > 0) {
            rejected.push({ index, errors });
            return;
        }
        const key = `${type}:${item[rule.id].trim()}`;
        const offset = seen.get(key) || 0;
        seen.set(key, offset + 1);
        readings.push({ ...buildReading(item, type, rule, now + offset, batchId), gateway_id: gatewayId });
    });
    if (rejected.length > 0) console.warn(JSON.stringify({ event: "batch_readings_rejected", gateway_id: gatewayId, count: rejected.length }));
    if (readings.length === 0) return response(400, { message: "No valid readings in batch", rejected });
    try {
        await queue({ type: "batch", gateway_id: gatewayId, batch_id: batchId, readings });
        return response(200, { message: "Batch queued", batch_id: batchId, accepted: readings.length, rejected });
    } catch (error) {
        console.error(JSON.stringify({ event: "queue_error", message: error.message }));
        return response(500, { message: "Error queueing batch" });
    }
}

export const handler = async (event) => {
    if (!SENSOR_API_KEY || getHeader(event.headers, "x-api-key") !== SENSOR_API_KEY) {
        console.warn(JSON.stringify({ event: "unauthorised_request" }));
        return response(401, { message: "Unauthorised" });
    }

    let body;
    try {
        body = typeof event.body === "string" ? JSON.parse(event.body) : event.body;
    } catch {
        return response(400, { message: "Invalid JSON rejected" });
    }
    if (!body || typeof body !== "object") return response(400, { message: "Empty reading rejected" });

    if (body.type === "batch") return handleBatch(body);

    const { type, rule, errors } = checkReading(body);
    if (!rule) return response(400, { message: errors[0] });
    if (errors.length > 0) {
        console.warn(JSON.stringify({ event: "reading_rejected", type, errors }));
        return response(400, { message: "Invalid reading rejected", errors });
    }

    const reading = buildReading(body, type, rule, Date.now(), randomUUID());
    try {
        await queue(reading);
        return response(200, { message: "Reading queued", type, id: reading[rule.id] });
    } catch (error) {
        console.error(JSON.stringify({ event: "queue_error", message: error.message }));
        return response(500, { message: "Error queueing reading" });
    }
};
