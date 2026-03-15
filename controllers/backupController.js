import { client } from "../config/dbcon.js";
import { logger } from "../middleware/Logger.js";
import { extractJWT } from "../middleware/extractToken.js";
import { spawn } from "child_process";
import { unlink } from "fs/promises";
import { URL } from "url";

const buildDatabaseUrl = () => {
    if (process.env.DATABASE_URL) {
        return process.env.DATABASE_URL;
    }

    const user = encodeURIComponent(process.env.DB_USER || "postgres");
    const password = encodeURIComponent(process.env.DB_PASSWORD || "Ifromurgut2005$");
    const host = process.env.DB_HOST || "localhost";
    const port = Number(process.env.DB_PORT || 5432);
    const database = process.env.DB_NAME || "postgres";
    const sslMode = String(process.env.DB_SSL || "").toLowerCase() === "true" ? "require" : "disable";

    return `postgresql://${user}:${password}@${host}:${port}/${database}?sslmode=${sslMode}`;
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

const collectProcessOutput = (child) =>
    new Promise((resolve, reject) => {
        let stderr = "";

        child.stderr.on("data", (chunk) => {
            stderr += chunk.toString();
        });

        child.on("error", reject);
        child.on("close", (code) => {
            if (code === 0) {
                resolve(stderr);
                return;
            }

            reject(new Error(stderr.trim() || `Command failed with exit code ${code}`));
        });
    });

export const downloadDatabaseBackup = async (req, res) => {
    const user_id = req.headers["uuid"] || extractJWT(req.headers["authorization"]);
    const shop_id = req.headers["shop_id"] || null;

    try {
        const tablesResult = await client.query(`
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'public'
        `);

        const tables = tablesResult.rows.map(r => r.tablename);
        const data = {};

        for (const table of tables) {
            const result = await client.query(`SELECT * FROM ${table}`);
            data[table] = result.rows;
        }

        const backup = {
            meta: {
                created_at: new Date().toISOString(),
                table_count: tables.length
            },
            data
        };

        await logger(shop_id, user_id, `Database backup downloaded - tables: ${tables.length}`);

        res.setHeader(
            "Content-Disposition",
            `attachment; filename=db-backup-${Date.now()}.json`
        );
        res.setHeader("Content-Type", "application/json");

        return res.status(200).send(JSON.stringify(backup, null, 2));

    } catch (err) {
        console.error("Backup error:", err);
        await logger(shop_id, user_id, `Database backup failed - error: ${err.message}`);
        return res.status(500).json({ message: "Server Error" });
    }
};

export const downloadDatabaseSqlDump = async (req, res) => {
    const user_id = req.headers["uuid"] || extractJWT(req.headers["authorization"]);
    const shop_id = req.headers["shop_id"] || null;

    try {
        const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
        const { databaseUrl, env } = getPostgresProcessOptions();
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

        let stderr = "";
        dumpProcess.stderr.on("data", (chunk) => {
            stderr += chunk.toString();
        });

        res.setHeader(
            "Content-Disposition",
            `attachment; filename=db-backup-${timestamp}.sql`
        );
        res.setHeader("Content-Type", "application/sql");

        dumpProcess.stdout.pipe(res);

        dumpProcess.on("error", async (err) => {
            console.error("SQL dump error:", err);
            await logger(shop_id, user_id, `SQL dump backup failed - error: ${err.message}`);

            if (!res.headersSent) {
                res.status(500).json({ message: "Failed to create SQL dump" });
            } else {
                res.destroy(err);
            }
        });

        dumpProcess.on("close", async (code) => {
            if (code === 0) {
                await logger(shop_id, user_id, "SQL dump backup downloaded successfully");
                return;
            }

            const error = stderr.trim() || `pg_dump exited with code ${code}`;
            console.error("SQL dump error:", error);
            await logger(shop_id, user_id, `SQL dump backup failed - error: ${error}`);

            if (!res.headersSent) {
                res.status(500).json({ message: "Failed to create SQL dump", error });
            } else {
                res.end();
            }
        });
    } catch (err) {
        console.error("SQL dump setup error:", err);
        await logger(shop_id, user_id, `SQL dump backup failed - error: ${err.message}`);
        return res.status(500).json({ message: "Failed to create SQL dump", error: err.message });
    }
};

export const restoreDatabaseSqlDump = async (req, res) => {
    const user_id = req.headers["uuid"] || extractJWT(req.headers["authorization"]);
    const shop_id = req.headers["shop_id"] || null;
    const sqlFile = req.file;

    if (!sqlFile) {
        await logger(shop_id, user_id, "SQL restore failed - missing SQL file");
        return res.status(400).json({ message: "SQL file is required" });
    }

    try {
        const { databaseUrl, env } = getPostgresProcessOptions();
        const restoreProcess = spawn(
            "psql",
            [
                "--dbname",
                databaseUrl,
                "--single-transaction",
                "--set",
                "ON_ERROR_STOP=1",
                "--file",
                sqlFile.path,
            ],
            { env }
        );

        await collectProcessOutput(restoreProcess);

        await logger(shop_id, user_id, `SQL dump restored successfully - file: ${sqlFile.originalname}`);
        return res.status(200).json({ message: "SQL dump restored successfully" });
    } catch (err) {
        console.error("SQL restore error:", err);
        await logger(shop_id, user_id, `SQL restore failed - error: ${err.message}`);
        return res.status(500).json({ message: "SQL restore failed", error: err.message });
    } finally {
        await unlink(sqlFile.path).catch(() => {});
    }
};

