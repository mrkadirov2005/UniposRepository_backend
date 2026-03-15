import cron from "node-cron";
import { client } from "../config/dbcon.js";
import fs from "fs";
import path from "path";
import archiver from "archiver";
import { google } from "googleapis";
import { fileURLToPath } from "url";
import { dirname } from "path";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const KEY_DIR = path.join(__dirname, "../key");
const DEFAULT_DRIVE_FOLDER_ID = "1_OB-Y83OFfCxw7AKpoX-wxucXYgc5YlB";
const DRIVE_FOLDER_ID = process.env.GOOGLE_DRIVE_FOLDER_ID || DEFAULT_DRIVE_FOLDER_ID;
const OAUTH_TOKEN_PATH =
  process.env.GOOGLE_DRIVE_OAUTH_TOKEN_PATH ||
  path.join(KEY_DIR, "google_oauth_token.json");
const SERVICE_ACCOUNT_PATH =
  process.env.GOOGLE_DRIVE_SERVICE_ACCOUNT_KEY_PATH ||
  path.join(KEY_DIR, "unipos-490308-f0139a155b46.json");
const AUTH_SCOPE = ["https://www.googleapis.com/auth/drive"];

const findOAuthClientSecretPath = () => {
  if (process.env.GOOGLE_DRIVE_OAUTH_CLIENT_SECRET_PATH) {
    return process.env.GOOGLE_DRIVE_OAUTH_CLIENT_SECRET_PATH;
  }

  if (!fs.existsSync(KEY_DIR)) {
    return null;
  }

  const fileName = fs
    .readdirSync(KEY_DIR)
    .find((name) => name.startsWith("client_secret_") && name.endsWith(".json"));

  return fileName ? path.join(KEY_DIR, fileName) : null;
};

const getOAuthRefreshToken = () => {
  if (process.env.GOOGLE_DRIVE_REFRESH_TOKEN) {
    return process.env.GOOGLE_DRIVE_REFRESH_TOKEN;
  }

  if (!fs.existsSync(OAUTH_TOKEN_PATH)) {
    return null;
  }

  try {
    const tokenPayload = JSON.parse(fs.readFileSync(OAUTH_TOKEN_PATH, "utf8"));
    return tokenPayload.refresh_token || null;
  } catch {
    return null;
  }
};

const createOAuthDriveClient = () => {
  const secretPath = findOAuthClientSecretPath();
  const refreshToken = getOAuthRefreshToken();

  if (!secretPath || !fs.existsSync(secretPath) || !refreshToken) {
    return null;
  }

  const payload = JSON.parse(fs.readFileSync(secretPath, "utf8"));
  const source = payload.installed || payload.web || {};
  const clientId = process.env.GOOGLE_DRIVE_CLIENT_ID || source.client_id;
  const clientSecret =
    process.env.GOOGLE_DRIVE_CLIENT_SECRET || source.client_secret;
  const redirectUri =
    process.env.GOOGLE_DRIVE_REDIRECT_URI ||
    (source.redirect_uris && source.redirect_uris[0]) ||
    "http://localhost";

  if (!clientId || !clientSecret) {
    return null;
  }

  const oAuth2Client = new google.auth.OAuth2(clientId, clientSecret, redirectUri);
  oAuth2Client.setCredentials({ refresh_token: refreshToken });

  return {
    drive: google.drive({ version: "v3", auth: oAuth2Client }),
    authMode: "oauth_user",
    principal: "OAuth user",
  };
};

const createServiceAccountDriveClient = () => {
  if (!fs.existsSync(SERVICE_ACCOUNT_PATH)) {
    return null;
  }

  const credentials = JSON.parse(fs.readFileSync(SERVICE_ACCOUNT_PATH, "utf8"));
  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: AUTH_SCOPE,
  });

  return {
    drive: google.drive({ version: "v3", auth }),
    authMode: "service_account",
    principal: credentials.client_email || "Service account",
  };
};

const createDriveClient = () => {
  const mode = (process.env.GOOGLE_DRIVE_AUTH_MODE || "auto").toLowerCase();
  const oauthClient = createOAuthDriveClient();
  const serviceAccountClient = createServiceAccountDriveClient();

  if (mode === "oauth") {
    if (!oauthClient) {
      throw new Error(
        "GOOGLE_DRIVE_AUTH_MODE=oauth but OAuth config is incomplete. Set client secret file and refresh token."
      );
    }
    return oauthClient;
  }

  if (mode === "service_account") {
    if (!serviceAccountClient) {
      throw new Error(
        "GOOGLE_DRIVE_AUTH_MODE=service_account but service account key file was not found."
      );
    }
    return serviceAccountClient;
  }

  return oauthClient || serviceAccountClient || (() => {
    throw new Error(
      "Google Drive auth is not configured. Provide OAuth refresh token or service account key."
    );
  })();
};

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
 * Export all database tables to individual JSON files
 */
const exportTablesToFiles = async (backupDir) => {
  try {
    const tablesResult = await client.query(`
      SELECT tablename
      FROM pg_tables
      WHERE schemaname = 'public'
      ORDER BY tablename
    `);

    const tables = tablesResult.rows.map((r) => r.tablename);
    console.log(`[BACKUP] Found ${tables.length} tables to backup`);

    const exportedFiles = [];

    for (const table of tables) {
      try {
        // Safely quote table name
        const safeTableName = `"${table.replace(/"/g, '""')}"`;
        const result = await client.query(`SELECT * FROM ${safeTableName}`);
        const filePath = path.join(backupDir, `${table}.json`);

        fs.writeFileSync(
          filePath,
          JSON.stringify(
            {
              table_name: table,
              row_count: result.rows.length,
              exported_at: new Date().toISOString(),
              data: result.rows,
            },
            null,
            2
          )
        );

        exportedFiles.push(filePath);
        console.log(`[BACKUP] Exported table: ${table} (${result.rows.length} rows)`);
      } catch (err) {
        console.error(`[BACKUP ERROR] Failed to export table ${table}:`, err.message);
      }
    }

    return exportedFiles;
  } catch (err) {
    console.error("[BACKUP ERROR] Failed to query tables:", err.message);
    throw err;
  }
};

/**
 * Create a ZIP file from exported table files
 */
const createZipFile = async (backupDir, zipFilePath) => {
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
    archive.directory(`${backupDir}/`, false);
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
 * Upload ZIP file to Google Drive with retries
 */
const uploadToGoogleDrive = async (filePath, fileName) => {
  const { drive, authMode, principal } = createDriveClient();

  if (!fs.existsSync(filePath)) {
    throw new Error(`Backup file not found: ${filePath}`);
  }

  const stats = fs.statSync(filePath);
  console.log(`[BACKUP] Ready for upload: ${filePath} (${stats.size} bytes)`);
  console.log(`[BACKUP] Drive auth mode: ${authMode} (${principal})`);

  let lastError = null;

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      console.log(`[BACKUP] Uploading to Google Drive... attempt ${attempt}`);

      const response = await drive.files.create(
        {
          requestBody: {
            name: fileName,
            mimeType: "application/zip",
            parents: [DRIVE_FOLDER_ID],
          },
          media: {
            mimeType: "application/zip",
            body: fs.createReadStream(filePath),
          },
          supportsAllDrives: true,
          fields: "id, name, webViewLink, createdTime, parents",
        },
        {
          timeout: 120000,
        }
      );

      console.log(
        `[BACKUP SUCCESS] File uploaded to Google Drive: ${response.data.name} (ID: ${response.data.id})`
      );
      console.log(`[BACKUP SUCCESS] Uploaded to folder: ${DRIVE_FOLDER_ID}`);
      console.log(`[BACKUP SUCCESS] View file: ${response.data.webViewLink}`);

      return response.data;
    } catch (err) {
      lastError = err;

      if (
        err.message &&
        err.message.includes("Service Accounts do not have storage quota")
      ) {
        console.error(
          "\n❌ [BACKUP ERROR] Service account quota limitation hit."
        );
        console.error("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        console.error("SOLUTION 1 (recommended): use OAuth user auth with refresh token.");
        console.error("SOLUTION 2: upload to a Shared Drive and grant service account access.");
        console.error(
          "\n📧 Email to share with: unipos@unipos-490308.iam.gserviceaccount.com"
        );
        console.error("\nSteps:");
        console.error(
          "1. Open: https://drive.google.com/drive/folders/1_OB-Y83OFfCxw7AKpoX-wxucXYgc5YlB"
        );
        console.error("2. Click the 'Share' button");
        console.error("3. Paste: unipos@unipos-490308.iam.gserviceaccount.com");
        console.error("4. Select 'Editor' permission");
        console.error("5. Click 'Share'");
        console.error("6. Restart the server");
        console.error("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
        throw err;
      }

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
 * Clean up temporary JSON files and optionally ZIP file
 */
const cleanupTempFiles = async (backupDir, zipFilePath, deleteZip = true) => {
  try {
    if (fs.existsSync(backupDir)) {
      const files = fs.readdirSync(backupDir);

      for (const file of files) {
        const fullPath = path.join(backupDir, file);
        if (fs.existsSync(fullPath)) {
          fs.unlinkSync(fullPath);
        }
      }

      if (fs.existsSync(backupDir) && fs.readdirSync(backupDir).length === 0) {
        fs.rmdirSync(backupDir);
      }
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
  const backupDir = path.join(BACKUP_TEMP_DIR, `backup_${timestamp}`);
  const zipFileName = `ShopPos_Backup_${timestamp}.zip`;
  const zipFilePath = path.join(BACKUP_TEMP_DIR, zipFileName);

  try {
    console.log(`\n[BACKUP] Starting hourly backup at ${new Date().toISOString()}`);

    if (!fs.existsSync(backupDir)) {
      fs.mkdirSync(backupDir, { recursive: true });
    }

    console.log("[BACKUP] Exporting database tables...");
    await exportTablesToFiles(backupDir);

    console.log("[BACKUP] Creating ZIP file...");
    await createZipFile(backupDir, zipFilePath);

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

    console.log("[BACKUP] Uploading to Google Drive...");
    const driveResponse = await uploadToGoogleDrive(zipFilePath, zipFileName);

    console.log("[BACKUP] Cleaning up temporary files...");
    await cleanupTempFiles(backupDir, zipFilePath, true);

    console.log(
      `[BACKUP] Hourly backup completed successfully at ${new Date().toISOString()}\n`
    );

    return {
      success: true,
      timestamp,
      driveFileId: driveResponse.id,
      driveLink: driveResponse.webViewLink,
    };
  } catch (err) {
    console.error(`[BACKUP FAILED] Error during backup: ${err.message}`);

    // Preserve ZIP if it exists
    const preservedPath = preserveFailedZip(zipFilePath);

    // Clean JSON temp files, but do NOT delete original ZIP on failure
    try {
      await cleanupTempFiles(backupDir, zipFilePath, false);
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
  const mode = (process.env.GOOGLE_DRIVE_AUTH_MODE || "auto").toLowerCase();
  console.log("🔄 Initializing hourly backup cronjob...");
  console.log(`[BACKUP] Auth mode setting: ${mode}`);
  console.log(`[BACKUP] Drive folder: ${DRIVE_FOLDER_ID}`);

  if (mode !== "oauth") {
    console.log("\n⚠️  SERVICE ACCOUNT NOTE:");
    console.log("Use a Shared Drive folder when running with service account credentials.");
    console.log("Service account email: unipos@unipos-490308.iam.gserviceaccount.com\n");
  }

  console.log("📦 Running initial backup on startup...");
  performHourlyBackup().catch((err) => {
    console.error("[BACKUP ERROR] Initial backup failed:", err.message);
  });

  const job = cron.schedule("0 * * * *", async () => {
    await performHourlyBackup();
  });

  console.log("✅ Hourly backup cronjob scheduled (every hour at minute 0)");
  console.log(`📍 Service Account Key: ${SERVICE_ACCOUNT_PATH}`);
  console.log(`📍 OAuth token path: ${OAUTH_TOKEN_PATH}`);
  console.log("📁 Temporary backup directory: backups_temp/");
  console.log("📁 Failed backup directory: backups_failed/");
  console.log(`☁️  Google Drive Folder: ${DRIVE_FOLDER_ID}`);

  return job;
};

/**
 * Manual backup trigger (can be called from API endpoints)
 */
export const manualBackup = async () => {
  return await performHourlyBackup();
};

export default { startBackupCronjob, manualBackup };
