// controllers/debtController.js
import { client } from "../config/dbcon.js";
import { v4 as uuidv4 } from "uuid";
import { logger } from "../middleware/Logger.js";
import { extractJWT } from "../middleware/extractToken.js";

// Get all debts
export const getAllDebts = async (req, res) => {
    console.log("Fetching all debts");
    const { shop_id } = req.body;
    const user_id = req.headers["uuid"] || extractJWT(req.headers["authorization"]);

    if (shop_id == null) {
        await logger(shop_id, user_id, "Get all debts failed - missing shop_id");
        return res.status(400).json({ message: "Shop_id etishmayapti" });
    }

    try {
        const result = await client.query(
            "SELECT * FROM debt_table WHERE shop_id = $1 ORDER BY year DESC, month DESC, day DESC",
            [shop_id]
        );

        await logger(shop_id, user_id, `Fetched all debts - count: ${result.rows.length}`);

        return res.status(200).json({
            message: "Qarzlar muvaffaqiyatli olingan",
            data: result.rows
        });
    } catch (err) {
        console.error("Error fetching debts:", err);
        await logger(shop_id, user_id, `Get all debts failed - error: ${err.message}`);
        return res.status(500).json({ message: "Server xatosi" });
    }
};

// Get debt by ID
export const getDebtById = async (req, res) => {
    const { id } = req.body;
    const user_id = req.headers["uuid"] || extractJWT(req.headers["authorization"]);
    const shop_id = req.body.shop_id || null;

    if (id == null) {
        await logger(shop_id, user_id, "Get debt by ID failed - missing debt ID");
        return res.status(400).json({ message: "Qarz ID talab qilinadi" });
    }

    try {
        const result = await client.query(
            "SELECT * FROM debt_table WHERE id = $1",
            [id]
        );

        if (result.rows.length === 0) {
            await logger(shop_id, user_id, `Get debt by ID failed - debt not found: ${id}`);
            return res.status(404).json({ message: "Qarz topilmadi" });
        }

        await logger(shop_id, user_id, `Fetched debt by ID: ${id}`);

        return res.status(200).json({
            message: "Qarz muvaffaqiyatli olingan",
            data: result.rows[0]
        });
    } catch (err) {
        console.error("Error fetching debt:", err);
        await logger(shop_id, user_id, `Get debt by ID failed - error: ${err.message}`);
        return res.status(500).json({ message: "Server xatosi" });
    }
};

// Get debts by branch
export const getDebtsByBranch = async (req, res) => {
    const { branch_id } = req.headers;
    const user_id = req.headers["uuid"] || extractJWT(req.headers["authorization"]);
    const shop_id = req.body.shop_id || null;

    if (branch_id == null) {
        await logger(shop_id, user_id, "Get debts by branch failed - missing branch_id");
        return res.status(400).json({ message: "Branch_id etishmayapti" });
    }

    try {
        const result = await client.query(
            "SELECT * FROM debt_table WHERE branch_id = $1 AND shop_id = $2 ORDER BY year DESC, month DESC, day DESC",
            [branch_id, shop_id]
        );

        await logger(shop_id, user_id, `Fetched debts by branch: ${branch_id} - count: ${result.rows.length}`);

        return res.status(200).json({
            message: "Qarzlar muvaffaqiyatli olingan",
            data: result.rows
        });
    } catch (err) {
        console.error("Error fetching debts by branch:", err);
        await logger(shop_id, user_id, `Get debts by branch failed - error: ${err.message}`);
        return res.status(500).json({ message: "Server xatosi" });
    }
};

// Get debts by customer name
export const getDebtsByCustomer = async (req, res) => {
    const { name, shop_id } = req.body;
    const user_id = req.headers["uuid"] || extractJWT(req.headers["authorization"]);

    if (name == null || shop_id == null) {
        await logger(shop_id, user_id, "Get debts by customer failed - missing required fields");
        return res.status(400).json({ message: "Kerakli maydonlar etishmayapti" });
    }

    try {
        const result = await client.query(
            "SELECT * FROM debt_table WHERE name ILIKE $1 AND shop_id = $2 ORDER BY year DESC, month DESC, day DESC",
            [`%${name}%`, shop_id]
        );

        await logger(shop_id, user_id, `Fetched debts by customer: ${name} - count: ${result.rows.length}`);

        return res.status(200).json({
            message: "Qarzlar muvaffaqiyatli olingan",
            data: result.rows
        });
    } catch (err) {
        console.error("Error fetching debts by customer:", err);
        await logger(shop_id, user_id, `Get debts by customer failed - error: ${err.message}`);
        return res.status(500).json({ message: "Server xatosi" });
    }
};

// Get unreturned debts
export const getUnreturnedDebts = async (req, res) => {
    const  shop_id  = req.body.shop_id;
    const user_id = req.headers["uuid"] || extractJWT(req.headers["authorization"]);

    if (shop_id == null) {
        await logger(shop_id, user_id, "Get unreturned debts failed - missing shop_id");
        return res.status(400).json({ message: "Shop_id etishmayapti" });
    }

    try {
        const result = await client.query(
            "SELECT * FROM debt_table WHERE shop_id = $1 AND isreturned = false ORDER BY year DESC, month DESC, day DESC",
            [shop_id]
        );

        await logger(shop_id, user_id, `Fetched unreturned debts - count: ${result.rows.length}`);

        return res.status(200).json({
            message: "Qaytarilinmagan qarzlar muvaffaqiyatli olingan",
            data: result.rows
        });
    } catch (err) {
        console.error("Error fetching unreturned debts:", err);
        await logger(shop_id, user_id, `Get unreturned debts failed - error: ${err.message}`);
        return res.status(500).json({ message: "Server xatosi" });
    }
};

// Create new debt
export const createDebt = async (req, res) => {
    const {
        name,
        amount,
        product_names,
        branch_id,
        shop_id,
        admin_id,
        isreturned = false,
        day: reqDay,
        month: reqMonth,
        year: reqYear,
        created_at
    } = req.body;
    const user_id = req.headers["uuid"] || extractJWT(req.headers["authorization"]);
    

    // Validate required fields
    if (name == null || amount == null || product_names == null || branch_id==null || shop_id == null || admin_id == null) {
        await logger(shop_id, user_id, "Create debt failed - missing required fields");
        return res.status(400).json({ message: "Kerakli maydonlar etishmayapti" });
    }

    const id = uuidv4();
    const buildDateParts = () => {
        if (
            Number.isInteger(reqDay) &&
            Number.isInteger(reqMonth) &&
            Number.isInteger(reqYear)
        ) {
            return { day: reqDay, month: reqMonth, year: reqYear };
        }
        if (created_at) {
            const parsed = new Date(created_at);
            if (!isNaN(parsed.getTime())) {
                return {
                    day: parsed.getDate(),
                    month: parsed.getMonth() + 1,
                    year: parsed.getFullYear(),
                };
            }
        }
        const currentDate = new Date();
        return {
            day: currentDate.getDate(),
            month: currentDate.getMonth() + 1,
            year: currentDate.getFullYear(),
        };
    };

    const { day, month, year } = buildDateParts();

    const query = `
        INSERT INTO debt_table (
            id, day, month, year, name, amount, product_names, 
            branch_id, shop_id, admin_id, isreturned
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        RETURNING *;
    `;

    const normalizeProductNames = (value) => {
        if (Array.isArray(value)) {
            return value.filter((v) => typeof v === "string" && v.trim() !== "");
        }
        if (typeof value === "string") {
            if (value.trim() === "") return [];
            return value
                .split("|")
                .map((v) => v.trim())
                .filter((v) => v !== "");
        }
        return [];
    };

    const data = normalizeProductNames(product_names);
    
    try {
        const result = await client.query(
            query,
            [id, day, month, year, name, amount, data, branch_id, shop_id, admin_id, isreturned]
        );

        await logger(shop_id, user_id, `Debt created successfully - customer: ${name}, amount: ${amount}`);

        return res.status(201).json({
            message: "Qarz muvaffaqiyatli yaratildi",
            data: result.rows[0]
        });
    } catch (err) {
        console.error("Error creating debt:", err);
        await logger(shop_id, user_id, `Create debt failed - error: ${err.message}`);
        return res.status(500).json({ message: "Server xatosi" });
    }
};

// Update debt
export const updateDebt = async (req, res) => {
    const {
        id,
        name,
        amount,
        product_names,
        branch_id,
        isreturned
    } = req.body;
    const user_id = req.headers["uuid"] || extractJWT(req.headers["authorization"]);
    const shop_id = req.headers["shop_id"] || null;

    if (id == null) {
        await logger(shop_id, user_id, "Update debt failed - missing debt ID");
        return res.status(400).json({ message: "Qarz ID talab qilinadi" });
    }

    const normalizeProductNames = (value) => {
        if (Array.isArray(value)) {
            return value.filter((v) => typeof v === "string" && v.trim() !== "");
        }
        if (typeof value === "string") {
            if (value.trim() === "") return null;
            return value
                .split("|")
                .map((v) => v.trim())
                .filter((v) => v !== "");
        }
        return null;
    };

    const normalizedProductNames = normalizeProductNames(product_names);

    const query = `
        UPDATE debt_table
        SET
            name = COALESCE($1, name),
            amount = COALESCE($2, amount),
            product_names = COALESCE($3, product_names),
            branch_id = COALESCE($4, branch_id),
            isreturned = COALESCE($5, isreturned)
        WHERE id = $6
        RETURNING *;
    `;

    try {
        const result = await client.query(
            query,
            [name, amount, normalizedProductNames, branch_id, isreturned, id]
        );

        if (result.rows.length === 0) {
            await logger(shop_id, user_id, `Update debt failed - debt not found: ${id}`);
            return res.status(404).json({ message: "Qarz topilmadi" });
        }

        await logger(shop_id, user_id, `Debt updated successfully: ${id}`);

        return res.status(200).json({
            message: "Qarz muvaffaqiyatli yangilandi",
            data: result.rows[0]
        });
    } catch (err) {
        console.error("Error updating debt:", err);
        await logger(shop_id, user_id, `Update debt failed - error: ${err.message}`);
        return res.status(500).json({ message: "Server xatosi" });
    }
};

// Mark debt as returned
export const markDebtAsReturned = async (req, res) => {
    const { id } = req.body;
    const user_id = req.headers["uuid"] || extractJWT(req.headers["authorization"]);
    const shop_id = req.headers["shop_id"] || null;

    if (id == null) {
        await logger(shop_id, user_id, "Mark debt as returned failed - missing debt ID");
        return res.status(400).json({ message: "Qarz ID talab qilinadi" });
    }

    try {
        const result = await client.query(
            "UPDATE debt_table SET isreturned = true WHERE id = $1 RETURNING *",
            [id]
        );

        if (result.rows.length === 0) {
            await logger(shop_id, user_id, `Mark debt as returned failed - debt not found: ${id}`);
            return res.status(404).json({ message: "Qarz topilmadi" });
        }

        await logger(shop_id, user_id, `Debt marked as returned: ${id} - customer: ${result.rows[0].name}`);

        return res.status(200).json({
            message: "Qarz qaytarilgan deb belgilandi",
            data: result.rows[0]
        });
    } catch (err) {
        console.error("Error marking debt as returned:", err);
        await logger(shop_id, user_id, `Mark debt as returned failed - error: ${err.message}`);
        return res.status(500).json({ message: "Server xatosi" });
    }
};

// Delete debt
export const deleteDebt = async (req, res) => {
    const { id } = req.headers;
    const user_id = req.headers["uuid"] || extractJWT(req.headers["authorization"]);
    const shop_id = req.headers["shop_id"] || null;

    if (id == null) {
        await logger(shop_id, user_id, "Delete debt failed - missing debt ID");
        return res.status(400).json({ message: "Qarz ID talab qilinadi" });
    }

    try {
        const result = await client.query(
            "DELETE FROM debt_table WHERE id = $1 RETURNING *",
            [id]
        );

        if (result.rows.length === 0) {
            await logger(shop_id, user_id, `Delete debt failed - debt not found: ${id}`);
            return res.status(404).json({ message: "Qarz topilmadi" });
        }

        await logger(shop_id, user_id, `Debt deleted successfully: ${id} - customer: ${result.rows[0].name}`);

        return res.status(200).json({
            message: "Qarz muvaffaqiyatli o'chirildi",
            data: result.rows[0]
        });
    } catch (err) {
        console.error("Error deleting debt:", err);
        await logger(shop_id, user_id, `Delete debt failed - error: ${err.message}`);
        return res.status(500).json({ message: "Server xatosi" });
    }
};

// Get debt statistics
export const getDebtStatistics = async (req, res) => {
    const { shop_id } = req.body;
    const user_id = req.headers["uuid"] || extractJWT(req.headers["authorization"]);

    if (shop_id == null) {
        await logger(shop_id, user_id, "Get debt statistics failed - missing shop_id");
        return res.status(400).json({ message: "Shop_id etishmayapti" });
    }

    const query = `
        SELECT 
            COUNT(*) as total_debts,
            SUM(CASE WHEN isreturned = false THEN 1 ELSE 0 END) as unreturned_count,
            SUM(CASE WHEN isreturned = true THEN 1 ELSE 0 END) as returned_count,
            SUM(amount) as total_amount,
            SUM(CASE WHEN isreturned = false THEN amount ELSE 0 END) as unreturned_amount,
            SUM(CASE WHEN isreturned = true THEN amount ELSE 0 END) as returned_amount
        FROM debt_table
        WHERE shop_id = $1;
    `;

    try {
        const result = await client.query(query, [shop_id]);

        await logger(shop_id, user_id, "Fetched debt statistics");

        return res.status(200).json({
            message: "Statistika muvaffaqiyatli olingan",
            data: result.rows[0]
        });
    } catch (err) {
        console.error("Error fetching debt statistics:", err);
        await logger(shop_id, user_id, `Get debt statistics failed - error: ${err.message}`);
        return res.status(500).json({ message: "Server Error" });
    }
};
