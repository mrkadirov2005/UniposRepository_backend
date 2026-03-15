import { client } from "../config/dbcon.js";
import { v4 as uuidv4 } from "uuid";
import { errorMessages } from "../config/errorMessages.js";
import { logger } from "../middleware/Logger.js";
import { extractJWT } from "../middleware/extractToken.js";

/**
 * Get all wagons
 */
export const getAllWagons = async (req, res) => {
  const target_id = extractJWT(req.headers["authorization"]);
  const user_id = req.headers["uuid"] || null;

  try {
    const result = await client.query(
      "SELECT * FROM wagons ORDER BY created_at DESC"
    );
    await logger(target_id, user_id, "Fetched all wagons");
    return res.status(200).json({
      success: true,
      count: result.rows.length,
      data: result.rows,
    });
  } catch (err) {
    console.error("Query error:", err);
    await logger(target_id, user_id, "Failed to fetch wagons: " + err.message);
    res.status(500).json({ 
      success: false, 
      error: "Server xatosi",
      message: err.message 
    });
  }
};

/**
 * Get wagon by ID
 */
export const getWagonById = async (req, res) => {
  const { id } = req.body;
  const target_id = extractJWT(req.headers["authorization"]);
  const user_id = req.headers["uuid"] || null;

  if (!id) {
    await logger(target_id, user_id, "Get wagon failed - missing ID");
    return res.status(400).json({
      success: false,
      message: errorMessages.MISSING_FIELDS,
    });
  }

  try {
    const result = await client.query(
      "SELECT * FROM wagons WHERE id = $1",
      [id]
    );

    if (result.rows.length === 0) {
      await logger(target_id, user_id, `Wagon not found with ID: ${id}`);
      return res.status(404).json({
        success: false,
        message: "Vagon topilmadi",
      });
    }

    await logger(target_id, user_id, `Fetched wagon: ${id}`);
    return res.status(200).json({
      success: true,
      data: result.rows[0],
    });
  } catch (err) {
    console.error("Query error:", err);
    await logger(target_id, user_id, "Failed to fetch wagon: " + err.message);
    res.status(500).json({
      success: false,
      error: "Server xatosi",
      message: err.message,
    });
  }
};

/**
 * Get wagon by wagon number
 */
export const getWagonByNumber = async (req, res) => {
  const { wagon_number } = req.body;
  const target_id = extractJWT(req.headers["authorization"]);
  const user_id = req.headers["uuid"] || null;

  if (!wagon_number) {
    await logger(target_id, user_id, "Get wagon failed - missing wagon number");
    return res.status(400).json({
      success: false,
      message: "Vagon raqami talab qilinadi",
    });
  }

  try {
    const result = await client.query(
      "SELECT * FROM wagons WHERE wagon_number = $1",
      [wagon_number]
    );

    if (result.rows.length === 0) {
      await logger(target_id, user_id, `Wagon not found: ${wagon_number}`);
      return res.status(404).json({
        success: false,
        message: "Vagon topilmadi",
      });
    }

    await logger(target_id, user_id, `Fetched wagon: ${wagon_number}`);
    return res.status(200).json({
      success: true,
      data: result.rows[0],
    });
  } catch (err) {
    console.error("Query error:", err);
    await logger(target_id, user_id, "Failed to fetch wagon: " + err.message);
    res.status(500).json({
      success: false,
      error: "Server xatosi",
      message: err.message,
    });
  }
};

/**
 * Create new wagon
 */
export const createWagon = async (req, res) => {
  const {
    wagon_number,
    products = [],
    indicator = "none",
    shop_id = null,
    branch = null,
    created_at = null,
  } = req.body;

  const target_id = extractJWT(req.headers["authorization"]);
  const user_id = req.headers["uuid"] || null;


  
  if (!wagon_number) {
    await logger(target_id, user_id, "Create wagon failed - missing wagon number");
    return res.status(400).json({
      success: false,
      message: "Vagon raqami talab qilinadi",
    });
  }

  if (!["debt_taken", "debt_given", "none"].includes(indicator)) {
    await logger(target_id, user_id, "Create wagon failed - invalid indicator");
    return res.status(400).json({
      success: false,
      message: "Indicator must be 'debt_taken', 'debt_given', or 'none'",
    });
  }

  try {
    // Calculate total from products
    let total = 0;
    const processedProducts = products.map((product) => {
      const {
        product_id,
        product_name,
        amount,
        paid_amount,
        price,
        unit,
      } = product;

      if (!product_id || !product_name || amount == null || price == null || paid_amount == null) {
        throw new Error("Each product must have product_id, product_name, amount, paid_amount, and price");
      }

      const subtotal = parseFloat(amount) * parseFloat(price);
      total += subtotal;

      return {
        product_id,
        product_name,
        amount: parseFloat(amount),
        paid_amount: parseFloat(paid_amount),
        price: parseFloat(price),
        subtotal: parseFloat(subtotal.toFixed(2)),
        unit: unit || "pcs",
      };
    });

    const wagon_id = uuidv4();

    const createdAtValue = created_at || null;

    const result = await client.query(
      `INSERT INTO wagons 
       (id, wagon_number, products, total, indicator, shop_id, branch, created_by, created_at) 
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, COALESCE($9, NOW())) 
       RETURNING *`,
      [
        wagon_id,
        wagon_number,
        JSON.stringify(processedProducts),
        total.toFixed(2),
        indicator,
        shop_id,
        branch,
        user_id,
        createdAtValue,
      ]
    );

    await logger(target_id, user_id, `Created wagon: ${wagon_number}`);
    return res.status(201).json({
      success: true,
      message: "Vagon muvaffaqiyatli yaratildi",
      data: result.rows[0],
    });
  } catch (err) {
    console.error("Query error:", err);
    
    // Check for unique constraint violation
    if (err.code === "23505") {
      await logger(target_id, user_id, `Wagon number already exists: ${wagon_number}`);
      return res.status(409).json({
        success: false,
        message: "Vagon raqami allaqachon mavjud",
      });
    }

    await logger(target_id, user_id, "Failed to create wagon: " + err.message);
    res.status(500).json({
      success: false,
      error: "Server xatosi",
      message: err.message,
    });
  }
};

/**
 * Update wagon
 */
export const updateWagon = async (req, res) => {
  const {
    id,
    wagon_number,
    products,
    indicator,
    shop_id,
    branch,
    created_at,
  } = req.body;

  const target_id = extractJWT(req.headers["authorization"]);
  const user_id = req.headers["uuid"] || null;

  if (!id) {
    await logger(target_id, user_id, "Update wagon failed - missing ID");
    return res.status(400).json({
      success: false,
      message: "Vagon ID talab qilinadi",
    });
  }

  try {
    // Check if wagon exists
    const existingWagon = await client.query(
      "SELECT * FROM wagons WHERE id = $1",
      [id]
    );

    if (existingWagon.rows.length === 0) {
      await logger(target_id, user_id, `Update failed - wagon not found: ${id}`);
      return res.status(404).json({
        success: false,
        message: "Vagon topilmadi",
      });
    }

    // Build dynamic update query
    const updates = [];
    const values = [];
    let paramCount = 1;

    if (wagon_number !== undefined) {
      updates.push(`wagon_number = $${paramCount}`);
      values.push(wagon_number);
      paramCount++;
    }

    if (products !== undefined) {
      // Calculate new total
      let total = 0;
      const processedProducts = products.map((product) => {
        const {
          product_id,
          product_name,
          amount,
          price,
          paid_amount,
          unit,
        } = product;

        if (!product_id || !product_name || amount == null || price == null) {
          throw new Error("Each product must have product_id, product_name, amount, and price");
        }

        const subtotal = parseFloat(amount) * parseFloat(price);
        total += subtotal;

        return {
          product_id,
          product_name,
          paid_amount,
          amount: parseFloat(amount),
          price: parseFloat(price),
          subtotal: parseFloat(subtotal.toFixed(2)),
          unit: unit || "pcs",
        };
      });

      updates.push(`products = $${paramCount}`);
      values.push(JSON.stringify(processedProducts));
      paramCount++;

      updates.push(`total = $${paramCount}`);
      values.push(total.toFixed(2));
      paramCount++;
    }

    if (indicator !== undefined) {
      if (!["debt_taken", "debt_given", "none"].includes(indicator)) {
        await logger(target_id, user_id, "Update wagon failed - invalid indicator");
        return res.status(400).json({
          success: false,
          message: "Indicator must be 'debt_taken', 'debt_given', or 'none'",
        });
      }
      updates.push(`indicator = $${paramCount}`);
      values.push(indicator);
      paramCount++;
    }

    if (shop_id !== undefined) {
      updates.push(`shop_id = $${paramCount}`);
      values.push(shop_id);
      paramCount++;
    }

    if (branch !== undefined) {
      updates.push(`branch = $${paramCount}`);
      values.push(branch);
      paramCount++;
    }

    if (created_at !== undefined) {
      updates.push(`created_at = $${paramCount}`);
      values.push(created_at || null);
      paramCount++;
    }

    if (updates.length === 0) {
      await logger(target_id, user_id, "Update wagon failed - no fields to update");
      return res.status(400).json({
        success: false,
        message: "Yangilash uchun maydonlar yo'q",
      });
    }

    values.push(id);
    const query = `
      UPDATE wagons 
      SET ${updates.join(", ")} 
      WHERE id = $${paramCount} 
      RETURNING *
    `;

    const result = await client.query(query, values);

    await logger(target_id, user_id, `Updated wagon: ${id}`);
    return res.status(200).json({
      success: true,
      message: "Vagon muvaffaqiyatli yangilandi",
      data: result.rows[0],
    });
  } catch (err) {
    console.error("Query error:", err);

    // Check for unique constraint violation
    if (err.code === "23505") {
      await logger(target_id, user_id, `Wagon number already exists`);
      return res.status(409).json({
        success: false,
        message: "Vagon raqami allaqachon mavjud",
      });
    }

    await logger(target_id, user_id, "Failed to update wagon: " + err.message);
    res.status(500).json({
      success: false,
      error: "Internal Server Error",
      message: err.message,
    });
  }
};

/**
 * Delete wagon
 */
export const deleteWagon = async (req, res) => {
  const { id } = req.body;
  const target_id = extractJWT(req.headers["authorization"]);
  const user_id = req.headers["uuid"] || null;

  if (!id) {
    await logger(target_id, user_id, "Delete wagon failed - missing ID");
    return res.status(400).json({
      success: false,
      message: "Vagon ID talab qilinadi",
    });
  }

  try {
    const result = await client.query(
      "DELETE FROM wagons WHERE id = $1 RETURNING *",
      [id]
    );

    if (result.rows.length === 0) {
      await logger(target_id, user_id, `Delete failed - wagon not found: ${id}`);
      return res.status(404).json({
        success: false,
        message: "Vagon topilmadi",
      });
    }

    await logger(target_id, user_id, `Deleted wagon: ${id}`);
    return res.status(200).json({
      success: true,
      message: "Vagon muvaffaqiyatli o'chirildi",
      data: result.rows[0],
    });
  } catch (err) {
    console.error("Query error:", err);
    await logger(target_id, user_id, "Failed to delete wagon: " + err.message);
    res.status(500).json({
      success: false,
      error: "Server xatosi",
      message: err.message,
    });
  }
};

/**
 * Get wagons by indicator (debt_taken, debt_given, none)
 */
export const getWagonsByIndicator = async (req, res) => {
  const { indicator } = req.body;
  const target_id = extractJWT(req.headers["authorization"]);
  const user_id = req.headers["uuid"] || null;

  if (!indicator) {
    await logger(target_id, user_id, "Get wagons by indicator failed - missing indicator");
    return res.status(400).json({
      success: false,
      message: "Ko'rsatkich talab qilinadi",
    });
  }

  if (!["debt_taken", "debt_given", "none"].includes(indicator)) {
    await logger(target_id, user_id, "Get wagons by indicator failed - invalid indicator");
    return res.status(400).json({
      success: false,
      message: "Indicator must be 'debt_taken', 'debt_given', or 'none'",
    });
  }

  try {
    const result = await client.query(
      "SELECT * FROM wagons WHERE indicator = $1 ORDER BY created_at DESC",
      [indicator]
    );

    await logger(target_id, user_id, `Fetched wagons with indicator: ${indicator}`);
    return res.status(200).json({
      success: true,
      count: result.rows.length,
      data: result.rows,
    });
  } catch (err) {
    console.error("Query error:", err);
    await logger(target_id, user_id, "Failed to fetch wagons by indicator: " + err.message);
    res.status(500).json({
      success: false,
      error: "Server xatosi",
      message: err.message,
    });
  }
};

/**
 * Get wagons by shop
 */
export const getWagonsByShop = async (req, res) => {
  const { shop_id } = req.body;
  const target_id = extractJWT(req.headers["authorization"]);
  const user_id = req.headers["uuid"] || null;

  if (!shop_id) {
    await logger(target_id, user_id, "Get wagons by shop failed - missing shop_id");
    return res.status(400).json({
      success: false,
      message: "Shop ID talab qilinadi",
    });
  }

  try {
    const result = await client.query(
      "SELECT * FROM wagons WHERE shop_id = $1 ORDER BY created_at DESC",
      [shop_id]
    );

    await logger(target_id, user_id, `Fetched wagons for shop: ${shop_id}`);
    return res.status(200).json({
      success: true,
      count: result.rows.length,
      data: result.rows,
    });
  } catch (err) {
    console.error("Query error:", err);
    await logger(target_id, user_id, "Failed to fetch wagons by shop: " + err.message);
    res.status(500).json({
      success: false,
      error: "Server xatosi",
      message: err.message,
    });
  }
};
