import cron from "node-cron";
import { client } from "../config/dbcon.js";
import fs from "fs";
import path from "path";
import archiver from "archiver";
import { spawn } from "child_process";
import fetch, { FormData } from "node-fetch";
import { fileFromSync } from "fetch-blob/from.js";
import { fileURLToPath, URL } from "url";
import { dirname } from "path";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const TELEGRAM_BOT_TOKEN = "";
const TELEGRAM_CHAT_IDS = ["563429481"];
const TELEGRAM_API_BASE =
  process.env.TELEGRAM_API_BASE || "https://api.telegram.org";

// Create backup folders
const BACKUP_TEMP_DIR = path.join(__dirname, "../backups_temp");
const BACKUP_FAILED_DIR = path.join(__dirname, "../backups_failed");

if (!fs.existsSync(BACKUP_TEMP_DIR)) {
  fs.mkdirSync(BACKUP_TEMP_DIR, { recursive: true });
}

if (!fs.existsSync(BACKUP_FAILED_DIR)) {
  fs.mkdirSync(BACKUP_FAILED_DIR, { recursive: true });
}

/**
 * Build a database URL for pg_dump/psql commands
 */
const buildDatabaseUrl = () => {
  const database = "postgres";
  const user = "postgres";
  const password = "12345678";
  const host = "localhost";
  const port = 5432;
  const sslMode = "disable";

  return `postgresql://${encodeURIComponent(user)}:${encodeURIComponent(password)}@${host}:${port}/${database}?sslmode=${sslMode}`;
};

const getPostgresProcessOptions = () => {
  const databaseUrl = buildDatabaseUrl();
  const parsed = new URL(databaseUrl);
  const password = parsed.password ? decodeURIComponent(parsed.password) : undefined;

  return {
    databaseUrl,
    env: {
      ...process.env,
      ...(password ? { PGPASSWORD: password } : {}),
    },
  };
};

/**
 * Generate a plain SQL dump file with pg_dump
 */
const createSqlDumpFile = async (sqlFilePath) => {
  const { databaseUrl, env } = getPostgresProcessOptions();

  await new Promise((resolve, reject) => {
    const dumpProcess = spawn(
      "pg_dump",
      [
        "--format=plain",
        "--clean",
        "--if-exists",
        "--no-owner",
        "--no-privileges",
        "--dbname",
        databaseUrl,
      ],
      { env }
    );

    const output = fs.createWriteStream(sqlFilePath);
    let stderr = "";

    dumpProcess.stdout.pipe(output);

    dumpProcess.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    dumpProcess.on("error", reject);
    output.on("error", reject);

    dumpProcess.on("close", (code) => {
      if (code === 0) {
        resolve();
        return;
      }

      reject(new Error(stderr.trim() || `pg_dump exited with code ${code}`));
    });
  });

  const stats = fs.statSync(sqlFilePath);
  console.log(`[BACKUP] SQL dump created: ${sqlFilePath} (${stats.size} bytes)`);
};

/**
 * Create a ZIP file containing the SQL dump
 */
const createZipFile = async (sqlFilePath, zipFilePath) => {
  return new Promise((resolve, reject) => {
    const output = fs.createWriteStream(zipFilePath);
    const archive = archiver("zip", {
      zlib: { level: 9 },
    });

    output.on("close", () => {
      console.log(
        `[BACKUP] ZIP file created: ${zipFilePath} (${archive.pointer()} bytes)`
      );
      resolve();
    });

    output.on("error", (err) => {
      console.error("[BACKUP ERROR] ZIP output stream failed:", err.message);
      reject(err);
    });

    archive.on("error", (err) => {
      console.error("[BACKUP ERROR] ZIP creation failed:", err.message);
      reject(err);
    });

    archive.pipe(output);
    archive.file(sqlFilePath, { name: path.basename(sqlFilePath) });
    archive.finalize();
  });
};

/**
 * Sleep helper for retries
 */
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Move failed ZIP to permanent failed backup folder
 */
const preserveFailedZip = (zipFilePath) => {
  try {
    if (!fs.existsSync(zipFilePath)) {
      return null;
    }

    const targetPath = path.join(BACKUP_FAILED_DIR, path.basename(zipFilePath));
    fs.copyFileSync(zipFilePath, targetPath);

    console.log(`[BACKUP] Failed backup preserved at: ${targetPath}`);
    return targetPath;
  } catch (err) {
    console.error("[BACKUP ERROR] Failed to preserve ZIP:", err.message);
    return null;
  }
};

/**
 * Upload ZIP file to Telegram with retries
 */
const uploadToTelegram = async (filePath, fileName) => {
  if (!TELEGRAM_BOT_TOKEN || TELEGRAM_CHAT_IDS.length === 0) {
    throw new Error(
      "TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_IDS is not set. Cannot upload backup to Telegram."
    );
  }

  if (!fs.existsSync(filePath)) {
    throw new Error(`Backup file not found: ${filePath}`);
  }

  const stats = fs.statSync(filePath);
  console.log(`[BACKUP] Ready for upload: ${filePath} (${stats.size} bytes)`);

  let lastError = null;
  const url = `${TELEGRAM_API_BASE}/bot${TELEGRAM_BOT_TOKEN}/sendDocument`;

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      console.log(`[BACKUP] Uploading to Telegram... attempt ${attempt}`);

      const results = [];

      for (const chatId of TELEGRAM_CHAT_IDS) {
        const form = new FormData();
        form.set("chat_id", chatId);
        form.set(
          "caption",
          `ShopPos backup ${new Date().toISOString().replace("T", " ").split(".")[0]}`
        );
        form.set(
          "document",
          fileFromSync(filePath, "application/zip"),
          fileName
        );

        const response = await fetch(url, {
          method: "POST",
          body: form,
        });

        const result = await response.json().catch(() => null);

        if (!response.ok || !result?.ok) {
          const errorMessage = result?.description || "Telegram upload failed";
          throw new Error(errorMessage);
        }

        console.log(
          `[BACKUP SUCCESS] File uploaded to Telegram chat ${chatId}: message_id=${result.result?.message_id}`
        );

        results.push(result.result);
      }

      return results;
    } catch (err) {
      lastError = err;
      console.error(
        `[BACKUP ERROR] Upload attempt ${attempt} failed: ${err.message}`
      );

      if (attempt < 3) {
        const delay = attempt === 1 ? 2000 : 5000;
        console.log(`[BACKUP] Retrying upload in ${delay / 1000} seconds...`);
        await sleep(delay);
      }
    }
  }

  throw lastError;
};

/**
 * Clean up temporary SQL dump and optionally ZIP file
 */
const cleanupTempFiles = async (sqlFilePath, zipFilePath, deleteZip = true) => {
  try {
    if (fs.existsSync(sqlFilePath)) {
      fs.unlinkSync(sqlFilePath);
    }

    if (deleteZip && fs.existsSync(zipFilePath)) {
      fs.unlinkSync(zipFilePath);
    }

    console.log(
      `[BACKUP] Temporary files cleaned up${deleteZip ? " (including ZIP)" : " (ZIP preserved)"}`
    );
  } catch (err) {
    console.error("[BACKUP ERROR] Cleanup failed:", err.message);
  }
};

/**
 * Main backup function - runs every hour
 */
const performHourlyBackup = async () => {
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const sqlFileName = `ShopPos_Backup_${timestamp}.sql`;
  const sqlFilePath = path.join(BACKUP_TEMP_DIR, sqlFileName);
  const zipFileName = `ShopPos_Backup_${timestamp}.zip`;
  const zipFilePath = path.join(BACKUP_TEMP_DIR, zipFileName);

  try {
    console.log(`\n[BACKUP] Starting hourly backup at ${new Date().toISOString()}`);

    console.log("[BACKUP] Creating SQL dump...");
    await createSqlDumpFile(sqlFilePath);

    console.log("[BACKUP] Creating ZIP file...");
    await createZipFile(sqlFilePath, zipFilePath);

    if (!fs.existsSync(zipFilePath)) {
      throw new Error("ZIP file was not created");
    }

    const zipStats = fs.statSync(zipFilePath);
    console.log(
      `[BACKUP] ZIP ready: ${zipFilePath} (${zipStats.size} bytes)`
    );

    if (zipStats.size === 0) {
      throw new Error("ZIP file is empty");
    }

    console.log("[BACKUP] Uploading to Telegram...");
    const telegramResponses = await uploadToTelegram(zipFilePath, zipFileName);

    console.log("[BACKUP] Cleaning up temporary files...");
    await cleanupTempFiles(sqlFilePath, zipFilePath, true);

    console.log(
      `[BACKUP] Hourly backup completed successfully at ${new Date().toISOString()}\n`
    );

    return {
      success: true,
      timestamp,
      telegramMessageIds: telegramResponses?.map((r) => r?.message_id).filter(Boolean),
      telegramFileIds: telegramResponses?.map((r) => r?.document?.file_id).filter(Boolean),
      telegramChatIds: telegramResponses?.map((r) => r?.chat?.id).filter(Boolean),
    };
  } catch (err) {
    console.error(`[BACKUP FAILED] Error during backup: ${err.message}`);

    // Preserve ZIP if it exists
    const preservedPath = preserveFailedZip(zipFilePath);

    // Clean SQL temp file, but do NOT delete original ZIP on failure
    try {
      await cleanupTempFiles(sqlFilePath, zipFilePath, false);
    } catch (cleanupErr) {
      console.error("[BACKUP ERROR] Cleanup after failure failed:", cleanupErr.message);
    }

    return {
      success: false,
      timestamp,
      error: err.message,
      preservedBackupPath: preservedPath,
    };
  }
};

/**
 * Schedule backup job - runs every hour at the start of the hour
 * Cron pattern: "0 * * * *" = every hour at minute 0
 */
export const startBackupCronjob = () => {
  console.log("🔄 Initializing hourly backup cronjob...");
  console.log(
    `[BACKUP] Telegram chats: ${TELEGRAM_CHAT_IDS.length > 0 ? TELEGRAM_CHAT_IDS.join(", ") : "not set"}`
  );

  console.log("📦 Running initial backup on startup...");
  performHourlyBackup().catch((err) => {
    console.error("[BACKUP ERROR] Initial backup failed:", err.message);
  });

  const job = cron.schedule("0 * * * *", async () => {
    await performHourlyBackup();
  });

  console.log("✅ Hourly backup cronjob scheduled (every hour at minute 0)");
  console.log("📁 Temporary backup directory: backups_temp/");
  console.log("📁 Failed backup directory: backups_failed/");

  return job;
};

/**
 * Manual backup trigger (can be called from API endpoints)
 */
export const manualBackup = async () => {
  return await performHourlyBackup();
};

export default { startBackupCronjob, manualBackup };
