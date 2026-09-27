import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";

const sqs = new SQSClient({});
const QUEUE_URL = process.env.QUEUE_URL;
const SENSOR_API_KEY = process.env.SENSOR_API_KEY;

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
  return errors;
}

function buildReading(body, type, rule) {
  const reading = {
    type,
    [rule.id]: body[rule.id].trim(),
    region: body.region.trim(),
    timestamp: new Date().toISOString(),
    sent_at: typeof body.timestamp === "string" && !Number.isNaN(Date.parse(body.timestamp)) ? body.timestamp : null,
    scenario: isNonEmptyString(body.scenario) ? body.scenario : "normal"
  };
  for (const field of Object.keys(rule.numbers)) reading[field] = body[field];
  if (typeof body.lat === "number") reading.lat = body.lat;
  if (typeof body.lon === "number") reading.lon = body.lon;
  return reading;
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

  const type = body.type || "bio";
  const rule = RULES[type];
  if (!rule) return response(400, { message: `Unknown reading type: ${type}` });

  const errors = validate(body, rule);
  if (errors.length > 0) {
    console.warn(JSON.stringify({ event: "reading_rejected", type, errors }));
    return response(400, { message: "Invalid reading rejected", errors });
  }

  const reading = buildReading(body, type, rule);
  try {
    await sqs.send(new SendMessageCommand({ QueueUrl: QUEUE_URL, MessageBody: JSON.stringify(reading) }));
    return response(200, { message: "Reading queued", type, id: reading[rule.id] });
  } catch (error) {
    console.error(JSON.stringify({ event: "queue_error", message: error.message }));
    return response(500, { message: "Error queueing reading" });
  }
};