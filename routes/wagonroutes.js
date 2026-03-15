import express from "express";
import {
  getAllWagons,
  getWagonById,
  getWagonByNumber,
  createWagon,
  updateWagon,
  deleteWagon,
  getWagonsByIndicator,
  getWagonsByShop,
} from "../controllers/wagoncontroller.js";
import { validateToken } from "../middleware/validateToken.js";

const router = express.Router();

// Protect all wagon routes with auth middleware
router.use(validateToken);

/**
 * @swagger
 * tags:
 *   name: Wagons
 *   description: Wagon management for train wagons with products and debt tracking
 */

/**
 * @swagger
 * components:
 *   schemas:
 *     Product:
 *       type: object
 *       required:
 *         - product_id
 *         - product_name
 *         - amount
 *         - price
 *       properties:
 *         product_id:
 *           type: string
 *           description: Product identifier
 *         product_name:
 *           type: string
 *           description: Name of the product
 *         amount:
 *           type: number
 *           description: Quantity of the product
 *         price:
 *           type: number
 *           description: Price per unit
 *         subtotal:
 *           type: number
 *           description: Calculated subtotal (amount * price)
 *     Wagon:
 *       type: object
 *       required:
 *         - wagon_number
 *       properties:
 *         id:
 *           type: string
 *           format: uuid
 *           description: Auto-generated wagon ID
 *         wagon_number:
 *           type: string
 *           description: Unique wagon number/identifier
 *         products:
 *           type: array
 *           items:
 *             $ref: '#/components/schemas/Product'
 *         total:
 *           type: number
 *           description: Total value of all products
 *         indicator:
 *           type: string
 *           enum: [debt_taken, debt_given, none]
 *           description: Debt indicator - debt_taken (I took debt), debt_given (I gave debt), none
 *         shop_id:
 *           type: string
 *           format: uuid
 *           description: Associated shop ID
 *         branch:
 *           type: string
 *           description: Branch identifier
 *         created_at:
 *           type: string
 *           format: date-time
 *         updated_at:
 *           type: string
 *           format: date-time
 *         created_by:
 *           type: string
 *           format: uuid
 */

/**
 * @swagger
 * /wagons/all:
 *   get:
 *     summary: Get all wagons
 *     tags: [Wagons]
 *     security:
 *       - tokenAuth: []
 *     responses:
 *       200:
 *         description: List of all wagons
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 success:
 *                   type: boolean
 *                 count:
 *                   type: integer
 *                 data:
 *                   type: array
 *                   items:
 *                     $ref: '#/components/schemas/Wagon'
 */
router.get("/all", getAllWagons);

/**
 * @swagger
 * /wagons/get-by-id:
 *   post:
 *     summary: Get wagon by ID
 *     tags: [Wagons]
 *     security:
 *       - tokenAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required:
 *               - id
 *             properties:
 *               id:
 *                 type: string
 *                 format: uuid
 *     responses:
 *       200:
 *         description: Wagon details
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 success:
 *                   type: boolean
 *                 data:
 *                   $ref: '#/components/schemas/Wagon'
 *       404:
 *         description: Wagon not found
 */
router.post("/get-by-id", getWagonById);

/**
 * @swagger
 * /wagons/get-by-number:
 *   post:
 *     summary: Get wagon by wagon number
 *     tags: [Wagons]
 *     security:
 *       - tokenAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required:
 *               - wagon_number
 *             properties:
 *               wagon_number:
 *                 type: string
 *     responses:
 *       200:
 *         description: Wagon details
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 success:
 *                   type: boolean
 *                 data:
 *                   $ref: '#/components/schemas/Wagon'
 *       404:
 *         description: Wagon not found
 */
router.post("/get-by-number", getWagonByNumber);

/**
 * @swagger
 * /wagons/create:
 *   post:
 *     summary: Create a new wagon
 *     tags: [Wagons]
 *     security:
 *       - tokenAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required:
 *               - wagon_number
 *             properties:
 *               wagon_number:
 *                 type: string
 *                 example: "WAGON-001"
 *               products:
 *                 type: array
 *                 items:
 *                   type: object
 *                   required:
 *                     - product_id
 *                     - product_name
 *                     - amount
 *                     - price
 *                   properties:
 *                     product_id:
 *                       type: string
 *                       example: "prod-123"
 *                     product_name:
 *                       type: string
 *                       example: "Steel Pipes"
 *                     amount:
 *                       type: number
 *                       example: 50
 *                     price:
 *                       type: number
 *                       example: 25.50
 *               indicator:
 *                 type: string
 *                 enum: [debt_taken, debt_given, none]
 *                 default: none
 *                 example: "debt_taken"
 *               shop_id:
 *                 type: string
 *                 format: uuid
 *               branch:
 *                 type: string
 *                 example: "Main Branch"
 *     responses:
 *       201:
 *         description: Wagon created successfully
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 success:
 *                   type: boolean
 *                 message:
 *                   type: string
 *                 data:
 *                   $ref: '#/components/schemas/Wagon'
 *       409:
 *         description: Wagon number already exists
 */
router.post("/create", createWagon);

/**
 * @swagger
 * /wagons/update:
 *   put:
 *     summary: Update an existing wagon
 *     tags: [Wagons]
 *     security:
 *       - tokenAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required:
 *               - id
 *             properties:
 *               id:
 *                 type: string
 *                 format: uuid
 *               wagon_number:
 *                 type: string
 *               products:
 *                 type: array
 *                 items:
 *                   type: object
 *                   properties:
 *                     product_id:
 *                       type: string
 *                     product_name:
 *                       type: string
 *                     amount:
 *                       type: number
 *                     price:
 *                       type: number
 *               indicator:
 *                 type: string
 *                 enum: [debt_taken, debt_given, none]
 *               shop_id:
 *                 type: string
 *                 format: uuid
 *               branch:
 *                 type: string
 *     responses:
 *       200:
 *         description: Wagon updated successfully
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 success:
 *                   type: boolean
 *                 message:
 *                   type: string
 *                 data:
 *                   $ref: '#/components/schemas/Wagon'
 *       404:
 *         description: Wagon not found
 */
router.put("/update", updateWagon);

/**
 * @swagger
 * /wagons/delete:
 *   delete:
 *     summary: Delete a wagon
 *     tags: [Wagons]
 *     security:
 *       - tokenAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required:
 *               - id
 *             properties:
 *               id:
 *                 type: string
 *                 format: uuid
 *     responses:
 *       200:
 *         description: Wagon deleted successfully
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 success:
 *                   type: boolean
 *                 message:
 *                   type: string
 *                 data:
 *                   $ref: '#/components/schemas/Wagon'
 *       404:
 *         description: Wagon not found
 */
router.delete("/delete", deleteWagon);

/**
 * @swagger
 * /wagons/by-indicator:
 *   post:
 *     summary: Get wagons by debt indicator
 *     tags: [Wagons]
 *     security:
 *       - tokenAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required:
 *               - indicator
 *             properties:
 *               indicator:
 *                 type: string
 *                 enum: [debt_taken, debt_given, none]
 *                 example: "debt_taken"
 *     responses:
 *       200:
 *         description: List of wagons with specified indicator
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 success:
 *                   type: boolean
 *                 count:
 *                   type: integer
 *                 data:
 *                   type: array
 *                   items:
 *                     $ref: '#/components/schemas/Wagon'
 */
router.post("/by-indicator", getWagonsByIndicator);

/**
 * @swagger
 * /wagons/by-shop:
 *   post:
 *     summary: Get wagons by shop ID
 *     tags: [Wagons]
 *     security:
 *       - tokenAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required:
 *               - shop_id
 *             properties:
 *               shop_id:
 *                 type: string
 *                 format: uuid
 *     responses:
 *       200:
 *         description: List of wagons for specified shop
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 success:
 *                   type: boolean
 *                 count:
 *                   type: integer
 *                 data:
 *                   type: array
 *                   items:
 *                     $ref: '#/components/schemas/Wagon'
 */
router.post("/by-shop", getWagonsByShop);

export default router;
