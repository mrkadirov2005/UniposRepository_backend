import express from "express";
import {
  getAllFinanceRecords,
  createFinanceRecord,
  updateFinanceRecord,
  deleteFinanceRecord,
  addPayment,
} from "../controllers/financeRoutes.js";

const router = express.Router();

/**
 * @swagger
 * tags:
 *   name: Finance
 *   description: Finance records and payment management
 */

/**
 * @swagger
 * /finance:
 *   get:
 *     summary: Get all finance records
 *     tags: [Finance]
 *     security:
 *       - tokenAuth: []
 *     responses:
 *       200:
 *         description: Finance records retrieved successfully
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 data:
 *                   type: array
 *                   items:
 *                     type: object
 *                     properties:
 *                       id:
 *                         type: integer
 *                       amount:
 *                         type: number
 *                       description:
 *                         type: string
 *                       type:
 *                         type: string
 *                       category:
 *                         type: string
 *                       date:
 *                         type: string
 *                         format: date
 *                       created_at:
 *                         type: string
 *                         format: date-time
 *       401:
 *         description: Unauthorized - No token provided
 *       500:
 *         description: Internal server error
 */
router.get("/", getAllFinanceRecords);

/**
 * @swagger
 * /finance:
 *   post:
 *     summary: Create a new finance record
 *     tags: [Finance]
 *     security:
 *       - tokenAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required:
 *               - amount
 *               - type
 *               - date
 *             properties:
 *               amount:
 *                 type: number
 *                 example: 1500.00
 *               description:
 *                 type: string
 *                 example: Monthly sales report
 *               type:
 *                 type: string
 *                 example: income
 *               category:
 *                 type: string
 *                 example: sales
 *               date:
 *                 type: string
 *                 format: date
 *                 example: 2025-01-12
 *     responses:
 *       201:
 *         description: Finance record created successfully
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 data:
 *                   type: object
 *       400:
 *         description: Missing required fields
 *       401:
 *         description: Unauthorized - No token provided
 *       500:
 *         description: Internal server error
 */
router.post("/", createFinanceRecord);

/**
 * @swagger
 * /finance/{id}:
 *   put:
 *     summary: Update a finance record
 *     tags: [Finance]
 *     security:
 *       - tokenAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: integer
 *         description: Finance record ID
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               amount:
 *                 type: number
 *               description:
 *                 type: string
 *               type:
 *                 type: string
 *               category:
 *                 type: string
 *               date:
 *                 type: string
 *                 format: date
 *     responses:
 *       200:
 *         description: Finance record updated successfully
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 data:
 *                   type: object
 *       404:
 *         description: Finance record not found
 *       401:
 *         description: Unauthorized - No token provided
 *       500:
 *         description: Internal server error
 */
router.put("/:id", updateFinanceRecord);

/**
 * @swagger
 * /finance/{id}:
 *   delete:
 *     summary: Delete a finance record
 *     tags: [Finance]
 *     security:
 *       - tokenAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: integer
 *         description: Finance record ID
 *     responses:
 *       200:
 *         description: Finance record deleted successfully
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 data:
 *                   type: object
 *                   properties:
 *                     id:
 *                       type: integer
 *       404:
 *         description: Finance record not found
 *       401:
 *         description: Unauthorized - No token provided
 *       500:
 *         description: Internal server error
 */
router.delete("/:id", deleteFinanceRecord);

/**
 * @swagger
 * /finance/{id}/payment:
 *   post:
 *     summary: Add a payment to a finance record
 *     tags: [Finance]
 *     security:
 *       - tokenAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: integer
 *         description: Finance record ID
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required:
 *               - amount
 *               - payment_date
 *             properties:
 *               amount:
 *                 type: number
 *                 example: 500.00
 *               payment_date:
 *                 type: string
 *                 format: date
 *                 example: 2025-01-12
 *               payment_method:
 *                 type: string
 *                 example: credit_card
 *     responses:
 *       201:
 *         description: Payment added successfully
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 data:
 *                   type: object
 *       400:
 *         description: Missing required fields
 *       401:
 *         description: Unauthorized - No token provided
 *       500:
 *         description: Internal server error
 */
router.post("/:id/payment", addPayment);

export default router;
