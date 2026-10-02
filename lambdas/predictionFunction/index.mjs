import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, QueryCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}), {
    marshallOptions: { removeUndefinedValues: true }
});
const sns = new SNSClient({});

const ANIMAL_TABLE = process.env.ANIMAL_TABLE || "AnimalStressData";
const ENV_TABLE = process.env.ENV_TABLE || "EnvironmentData";
const ALERT_TABLE = process.env.ALERT_TABLE || "AlertState";
const SNS_TOPIC_ARN = process.env.SNS_TOPIC_ARN;
const ALERT_THRESHOLD = Number(process.env.ALERT_THRESHOLD || 60);
const ALERT_COOLDOWN_MS = Number(process.env.ALERT_COOLDOWN_SECONDS || 300) * 1000;
const ENV_MAX_AGE_MS = Number(process.env.ENV_MAX_AGE_SECONDS || 120) * 1000;
const ENV_CACHE_MS = Number(process.env.ENV_CACHE_SECONDS || 10) * 1000;

const TIERS = {
    temp: [[40.5, 35], [39.5, 20]],
    heart_rate: [[150, 45], [140, 35], [125, 25], [115, 15]],
    ambient_temp: [[40, 25], [35, 15]],
    noise: [[95, 45], [85, 25]],
    movement: [[65, 10]]
};

const SEVERITY_RANK = { none: 0, low: 1, medium: 2, high: 3 };

const envCache = new Map();

function tier(value, steps) {
    if (typeof value !== "number") return 0;
    for (const [limit, points] of steps) {
        if (value >= limit) return points;
    }
    return 0;
}

function scoreReading(bio, env) {
    const noiseLevel = Math.max(bio.noise ?? 0, env?.noise ?? 0);
    const breakdown = {
        body_temp: tier(bio.temp, TIERS.temp),
        heart_rate: tier(bio.heart_rate, TIERS.heart_rate),
        ambient_temp: env ? tier(env.ambient_temp, TIERS.ambient_temp) : 0,
        noise: tier(noiseLevel, TIERS.noise),
        movement: tier(bio.movement, TIERS.movement)
    };
    const heat = breakdown.body_temp + breakdown.ambient_temp;
    const disturbance = breakdown.noise > 0 ? breakdown.noise + breakdown.movement : 0;
    let cause = "none";
    let score = 0;
    if (heat > 0 && heat >= disturbance) {
        cause = "heat_stress";
        score = heat + breakdown.heart_rate;
    } else if (disturbance > 0) {
        cause = "noise_stress";
        score = disturbance + breakdown.heart_rate;
    } else if (breakdown.heart_rate > 0) {
        cause = "elevated_heart_rate";
        score = breakdown.heart_rate;
    }
    score = Math.min(100, score);
    const severity = score >= 80 ? "high" : score >= ALERT_THRESHOLD ? "medium" : score > 0 ? "low" : "none";
    return { stress_score: score, cause, severity, score_breakdown: breakdown };
}

async function getLatestEnv(region, now) {
    const cached = envCache.get(region);
    if (cached && now - cached.fetchedAt < ENV_CACHE_MS) return cached.item;
    const result = await ddb.send(new QueryCommand({
        TableName: ENV_TABLE,
        KeyConditionExpression: "#r = :r",
        ExpressionAttributeNames: { "#r": "region" },
        ExpressionAttributeValues: { ":r": region },
        ScanIndexForward: false,
        Limit: 1
    }));
    const item = result.Items?.[0] || null;
    envCache.set(region, { item, fetchedAt: now });
    return item;
}

async function storeEnv(reading, now, invocationId) {
    const item = {
        ...reading,
        timestamp_sensor: `${reading.timestamp}#${reading.sensor_id}`,
        processed_at: new Date(now).toISOString(),
        invocation_id: invocationId
    };
    await ddb.send(new PutCommand({ TableName: ENV_TABLE, Item: item }));
    const cached = envCache.get(reading.region);
    if (!cached?.item || cached.item.timestamp <= item.timestamp) {
        envCache.set(reading.region, { item, fetchedAt: now });
    }
    return { alertSent: false, alertSuppressed: false };
}

async function claimAlert(reading, result, now) {
    try {
        await ddb.send(new UpdateCommand({
            TableName: ALERT_TABLE,
            Key: { animal_id: reading.animal_id },
            UpdateExpression: "SET last_alert_at = :now, last_rank = :rank, last_cause = :cause, last_score = :score, #rg = :region",
            ConditionExpression: "attribute_not_exists(animal_id) OR last_alert_at < :cutoff OR last_rank < :rank OR last_cause <> :cause",
            ExpressionAttributeNames: { "#rg": "region" },
            ExpressionAttributeValues: {
                ":now": now,
                ":cutoff": now - ALERT_COOLDOWN_MS,
                ":rank": SEVERITY_RANK[result.severity],
                ":cause": result.cause,
                ":score": result.stress_score,
                ":region": reading.region
            }
        }));
        return true;
    } catch (error) {
        if (error.name === "ConditionalCheckFailedException") return false;
        throw error;
    }
}

async function releaseAlert(animalId, now) {
    await ddb.send(new UpdateCommand({
        TableName: ALERT_TABLE,
        Key: { animal_id: animalId },
        UpdateExpression: "SET last_alert_at = :zero, last_rank = :zero",
        ConditionExpression: "last_alert_at = :now",
        ExpressionAttributeValues: { ":zero": 0, ":now": now }
    })).catch(() => {});
}

async function processBio(reading, now, invocationId) {
    const latest = await getLatestEnv(reading.region, now);
    const env = latest && now - Date.parse(latest.timestamp) <= ENV_MAX_AGE_MS ? latest : null;
    const result = scoreReading(reading, env);
    const processedAt = new Date(now).toISOString();
    let alertSent = false;
    let alertSuppressed = false;
    if (result.stress_score >= ALERT_THRESHOLD) {
        if (await claimAlert(reading, result, now)) {
            try {
                await sns.send(new PublishCommand({
                    TopicArn: SNS_TOPIC_ARN,
                    Subject: "Stress Alert",
                    Message: JSON.stringify({
                        animal_id: reading.animal_id,
                        region: reading.region,
                        lat: reading.lat,
                        lon: reading.lon,
                        stress_score: result.stress_score,
                        cause: result.cause,
                        severity: result.severity,
                        sent_at: reading.sent_at,
                        timestamp: reading.timestamp,
                        processed_at: processedAt,
                        gateway_id: reading.gateway_id
                    })
                }));
                alertSent = true;
            } catch (error) {
                await releaseAlert(reading.animal_id, now);
                throw error;
            }
        } else {
            alertSuppressed = true;
        }
    }
    const item = {
        ...reading,
        ...result,
        env_available: Boolean(env),
        env_sensor_id: env?.sensor_id,
        env_ambient_temp: env?.ambient_temp,
        env_humidity: env?.humidity,
        env_noise: env?.noise,
        alert_sent: alertSent,
        alert_suppressed: alertSuppressed,
        processed_at: processedAt,
        invocation_id: invocationId,
        pipeline_latency_ms: reading.sent_at ? now - Date.parse(reading.sent_at) : undefined
    };
    await ddb.send(new PutCommand({ TableName: ANIMAL_TABLE, Item: item }));
    return { alertSent, alertSuppressed };
}

function processReading(reading, now, invocationId) {
    if (reading.type === "env") return storeEnv(reading, now, invocationId);
    return processBio(reading, now, invocationId);
}

async function processMessage(record, invocationId) {
    const message = JSON.parse(record.body);
    const now = Date.now();
    if (message.type !== "batch") return [await processReading(message, now, invocationId)];
    const envReadings = message.readings.filter((reading) => reading.type === "env");
    const bioReadings = message.readings.filter((reading) => reading.type !== "env");
    const envOutcomes = await Promise.allSettled(envReadings.map((reading) => storeEnv(reading, now, invocationId)));
    const bioOutcomes = await Promise.allSettled(bioReadings.map((reading) => processBio(reading, now, invocationId)));
    const outcomes = [...envOutcomes, ...bioOutcomes];
    const failed = outcomes.find((outcome) => outcome.status === "rejected");
    if (failed) throw failed.reason;
    return outcomes.map((outcome) => outcome.value);
}

export const handler = async (event, context) => {
    const invocationId = context?.awsRequestId || "local";
    const results = await Promise.allSettled(event.Records.map((record) => processMessage(record, invocationId)));
    const batchItemFailures = [];
    let readings = 0;
    let alerts = 0;
    let suppressed = 0;
    results.forEach((result, index) => {
        if (result.status === "rejected") {
            console.error(JSON.stringify({
                event: "record_failed",
                messageId: event.Records[index].messageId,
                error: result.reason?.message
            }));
            batchItemFailures.push({ itemIdentifier: event.Records[index].messageId });
            return;
        }
        for (const value of result.value) {
            readings++;
            if (value.alertSent) alerts++;
            if (value.alertSuppressed) suppressed++;
        }
    });
    console.log(JSON.stringify({
        event: "batch_processed",
        records: event.Records.length,
        readings,
        failed: batchItemFailures.length,
        alerts,
        suppressed
    }));
    return { batchItemFailures };
};
