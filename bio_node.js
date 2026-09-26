const axios = require("axios");

const API_URL = "https://wx1326exaa.execute-api.us-east-1.amazonaws.com/default/ingestionFunction";

function randomBetween(min, max) {
  return Math.round((Math.random() * (max - min) + min) * 10) / 10;
}

function generateReading() {
  return {
    animal_id: "animal-" + Math.floor(Math.random() * 5 + 1),
    heart_rate: randomBetween(80, 160),
    movement: randomBetween(0, 100),
    temp: randomBetween(30, 45),
    noise: randomBetween(0, 120),
    light: randomBetween(0, 1000),
    region: ["Mallee", "Wimmera"][Math.floor(Math.random() * 2)],
  };
}

async function sendReading() {
  const reading = generateReading();
  try {
    const res = await axios.post(API_URL, reading);
    console.log("Sent:", reading, "-> Response:", res.data.message);
  } catch (err) {
    console.error("Error sending reading:", err.message);
  }
}

setInterval(sendReading, 3000);
console.log("Simulator running... sending readings every 3s");