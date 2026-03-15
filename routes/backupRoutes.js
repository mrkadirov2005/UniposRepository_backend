import express from "express";
import fs from "fs";
import multer from "multer";
import {
  downloadDatabaseBackup,
  downloadDatabaseSqlDump,
  restoreDatabaseSqlDump,
} from "../controllers/backupController.js";
import { downloadSheetsBackup, restoreDatabaseBackup } from "../controllers/restoreController.js";
import { manualBackup } from "../jobs/update.cronjob.js";
import fetch from "node-fetch"; // if your Node.js version < 18

const router = express.Router();
fs.mkdirSync("tmp/sql-restores", { recursive: true });
const upload = multer({ dest: "tmp/sql-restores/" });

// Apply the increased limit ONLY to the restore route
router.post("/restore", 
  express.json({ limit: "50mb" }),  // Increased limit for restore only
  restoreDatabaseBackup
);

// Backup route keeps the default limit
router.post("/backup", downloadDatabaseBackup);
router.post("/backup-sql", downloadDatabaseSqlDump);
router.post("/restore-sql", upload.single("file"), restoreDatabaseSqlDump);

// Manual trigger for Google Drive backup
router.post("/manual-backup-drive", async (req, res) => {
  try {
    const result = await manualBackup();
    if (result.success) {
      res.status(200).json({
        message: "Backup to Google Drive completed successfully",
        data: result,
      });
    } else {
      res.status(500).json({
        message: "Backup to Google Drive failed",
        error: result.error,
      });
    }
  } catch (error) {
    console.error("Error triggering manual backup:", error);
    res.status(500).json({ message: "Failed to trigger manual backup" });
  }
});

/// routes/backupProxyRoutes.js



const GOOGLE_APPS_SCRIPT_URL =
  process.env.GOOGLE_APPS_SCRIPT_URL ||
  "https://script.google.com/macros/s/AKfycbzgckYdxmsw8WMCsK3bZkeKI1MXLhk2XAGhdxtnlBYdJRWDhN0Imh8arbuTC0qOmkAw/exec";

router.post("/backup-to-sheets", async (req, res) => {
  try {
    if (!req.body || typeof req.body !== "object") {
      return res.status(400).json({ message: "Backup payload is required" });
    }

    // Forward the JSON backup data to Google Apps Script
    const response = await fetch(GOOGLE_APPS_SCRIPT_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(req.body),
    });

    const rawText = await response.text();
    let data;

    try {
      data = rawText ? JSON.parse(rawText) : {};
    } catch {
      data = {
        message: rawText || "Google Apps Script returned a non-JSON response",
      };
    }

    if (!response.ok) {
      console.error("Google Apps Script backup failed:", {
        status: response.status,
        body: rawText.slice(0, 500),
      });
    }

    // Forward the response back to the frontend
    res.status(response.status).json(data);
  } catch (error) {
    console.error("Error proxying backup to sheets:", error);
    res.status(500).json({ message: "Failed to backup to Google Sheets" });
  }
});

router.post("/restore-from-sheets", downloadSheetsBackup)



export default router;
