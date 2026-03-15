import pkg from "pg";
const { Client } = pkg;

const useSsl = String(process.env.DB_SSL || "").toLowerCase() === "true";
const connectionString = process.env.DATABASE_URL;

const clientConfig = connectionString
  ? {
      connectionString,
      ...(useSsl ? { ssl: { rejectUnauthorized: false } } : {}),
    }
  : {
      user: process.env.DB_USER || "postgres",
      host: process.env.DB_HOST || "localhost",
      database: process.env.DB_NAME || "postgres",
      password: process.env.DB_PASSWORD || "Ifromurgut2005$",
      port: Number(process.env.DB_PORT || 5432),
      ...(useSsl ? { ssl: { rejectUnauthorized: false } } : {}),
    };

export const client = new Client(clientConfig);

export async function connectDB() {
  try {
    await client.connect();
    console.log("[DB] Connected successfully");
  } catch (err) {
    console.error("Database connection error:", err);
  }
}
