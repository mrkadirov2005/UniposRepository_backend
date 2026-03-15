import { describe, it, expect, vi, beforeEach } from "vitest";
import { createReq, createRes } from "./helpers.js";

vi.mock("../config/dbcon.js", () => ({
  client: { query: vi.fn() },
}));
vi.mock("../middleware/Logger.js", () => ({
  logger: vi.fn(),
}));
vi.mock("../middleware/extractToken.js", () => ({
  extractJWT: vi.fn(() => "jwt-user"),
}));
vi.mock("uuid", () => ({
  v4: vi.fn(() => "sale-1"),
}));

import { client } from "../config/dbcon.js";
import { logger } from "../middleware/Logger.js";
import { extractJWT } from "../middleware/extractToken.js";
import { v4 as uuidv4 } from "uuid";

import {
  getSales,
  createNewSale,
  updateSale,
  deleteSale,
  getSaleById,
  getAllSales,
  getAdminSales,
} from "../controllers/salescontroller.js";

beforeEach(() => {
  client.query.mockReset();
  logger.mockReset();
  extractJWT.mockReset();
  extractJWT.mockReturnValue("jwt-user");
  uuidv4.mockReset();
  uuidv4.mockReturnValue("sale-1");
});

describe("salescontroller", () => {
  describe("getSales", () => {
    it("returns rows on success", async () => {
      client.query.mockResolvedValueOnce({ rows: [{ id: 1 }] });
      const req = createReq({ headers: { authorization: "t", uuid: "u1" } });
      const res = createRes();

      await getSales(req, res);

      expect(res.json).toHaveBeenCalledWith([{ id: 1 }]);
    });

    it("returns 500 on error", async () => {
      client.query.mockRejectedValueOnce(new Error("db"));
      const req = createReq({ headers: { authorization: "t", uuid: "u1" } });
      const res = createRes();

      await getSales(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
    });
  });

  describe("createNewSale", () => {
    it("returns 400 when body missing", async () => {
      const req = createReq({ body: null, headers: { authorization: "t" } });
      const res = createRes();

      await createNewSale(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 400 when required fields missing", async () => {
      const req = createReq({ body: {}, headers: { authorization: "t" } });
      const res = createRes();

      await createNewSale(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 400 when products array is empty", async () => {
      const req = createReq({
        body: {
          sale: {},
          products: [],
          payment_method: "cash",
        },
        headers: { authorization: "t" },
      });
      const res = createRes();

      await createNewSale(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 400 when stock is insufficient", async () => {
      client.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rowCount: 0 }) // update
        .mockResolvedValueOnce({}); // ROLLBACK
      const req = createReq({
        body: {
          sale: {},
          products: [{ productid: 1, product_name: "p1", amount: 2 }],
          payment_method: "cash",
          shop_id: "s1",
        },
        headers: { authorization: "t" },
      });
      const res = createRes();

      await createNewSale(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 201 when sale created", async () => {
      client.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rowCount: 1, rows: [{ availability: 5 }] }) // update
        .mockResolvedValueOnce({}) // insert sales
        .mockResolvedValueOnce({}) // insert soldproduct
        .mockResolvedValueOnce({}); // COMMIT
      const req = createReq({
        body: {
          sale: { admin_name: "a", total_price: 10 },
          products: [{ productid: 1, product_name: "p1", amount: 2 }],
          payment_method: "cash",
          shop_id: "s1",
        },
        headers: { authorization: "t" },
      });
      const res = createRes();

      await createNewSale(req, res);

      expect(res.status).toHaveBeenCalledWith(201);
      expect(res.send).toHaveBeenCalledWith(
        expect.objectContaining({ sales_id: "sale-1" })
      );
    });

    it("returns 500 on error and rolls back", async () => {
      client.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockRejectedValueOnce(new Error("db")) // update
        .mockResolvedValueOnce({}); // ROLLBACK
      const req = createReq({
        body: {
          sale: {},
          products: [{ productid: 1, product_name: "p1", amount: 2 }],
          payment_method: "cash",
          shop_id: "s1",
        },
        headers: { authorization: "t" },
      });
      const res = createRes();

      await createNewSale(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
    });
  });

  describe("updateSale", () => {
    it("returns 400 when required fields missing", async () => {
      const req = createReq({ body: {}, headers: { authorization: "t" } });
      const res = createRes();

      await updateSale(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 404 when sale not found", async () => {
      client.query.mockResolvedValueOnce({ rowCount: 0 });
      const req = createReq({
        body: { sale_id: "s1", updatedFields: { total_price: 10 } },
        headers: { authorization: "t" },
      });
      const res = createRes();

      await updateSale(req, res);

      expect(res.status).toHaveBeenCalledWith(404);
    });

    it("returns 200 when updated", async () => {
      client.query.mockResolvedValueOnce({ rowCount: 1 });
      const req = createReq({
        body: { sale_id: "s1", updatedFields: { total_price: 10 } },
        headers: { authorization: "t" },
      });
      const res = createRes();

      await updateSale(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
    });

    it("returns 500 on error", async () => {
      client.query.mockRejectedValueOnce(new Error("db"));
      const req = createReq({
        body: { sale_id: "s1", updatedFields: { total_price: 10 } },
        headers: { authorization: "t" },
      });
      const res = createRes();

      await updateSale(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
    });
  });

  describe("deleteSale", () => {
    it("returns 404 when sale not found", async () => {
      client.query.mockResolvedValueOnce({ rowCount: 0 });
      const req = createReq({
        body: { sale_id: "s1" },
        headers: { authorization: "t" },
      });
      const res = createRes();

      await deleteSale(req, res);

      expect(res.status).toHaveBeenCalledWith(404);
    });

    it("returns 200 when deleted", async () => {
      client.query.mockResolvedValueOnce({ rowCount: 1 });
      const req = createReq({
        body: { sale_id: "s1" },
        headers: { authorization: "t" },
      });
      const res = createRes();

      await deleteSale(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
    });

    it("returns 500 on error", async () => {
      client.query.mockRejectedValueOnce(new Error("db"));
      const req = createReq({
        body: { sale_id: "s1" },
        headers: { authorization: "t" },
      });
      const res = createRes();

      await deleteSale(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
    });
  });

  describe("getSaleById", () => {
    it("returns 404 when sale not found", async () => {
      client.query.mockResolvedValueOnce({ rows: [] });
      const req = createReq({
        body: { sale_id: "s1" },
        headers: { authorization: "t" },
      });
      const res = createRes();

      await getSaleById(req, res);

      expect(res.status).toHaveBeenCalledWith(404);
    });

    it("returns 200 with sale and products when found", async () => {
      client.query
        .mockResolvedValueOnce({ rows: [{ sale_id: "s1" }] })
        .mockResolvedValueOnce({ rows: [{ id: 1 }] });
      const req = createReq({
        body: { sale_id: "s1" },
        headers: { authorization: "t" },
      });
      const res = createRes();

      await getSaleById(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
      expect(res.send).toHaveBeenCalledWith(
        expect.objectContaining({ sale: { sale_id: "s1" } })
      );
    });

    it("returns 500 on error", async () => {
      client.query.mockRejectedValueOnce(new Error("db"));
      const req = createReq({
        body: { sale_id: "s1" },
        headers: { authorization: "t" },
      });
      const res = createRes();

      await getSaleById(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
    });
  });

  describe("getAllSales", () => {
    it("returns 400 when shop_id missing", async () => {
      const req = createReq({ body: {}, headers: { authorization: "t" } });
      const res = createRes();

      await getAllSales(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 200 with data on success", async () => {
      client.query.mockResolvedValueOnce({ rows: [{ id: 1 }] });
      const req = createReq({
        body: { shop_id: "s1" },
        headers: { authorization: "t" },
      });
      const res = createRes();

      await getAllSales(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
    });

    it("returns 500 on error", async () => {
      client.query.mockRejectedValueOnce(new Error("db"));
      const req = createReq({
        body: { shop_id: "s1" },
        headers: { authorization: "t" },
      });
      const res = createRes();

      await getAllSales(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
    });
  });

  describe("getAdminSales", () => {
    it("returns 400 when required fields missing", async () => {
      const req = createReq({ body: {}, headers: {} });
      const res = createRes();

      await getAdminSales(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 404 when no data found", async () => {
      client.query.mockResolvedValueOnce({ rowCount: 0, rows: [] });
      const req = createReq({ body: { shop_id: "s1", admin_name: "a" } });
      const res = createRes();

      await getAdminSales(req, res);

      expect(res.status).toHaveBeenCalledWith(404);
    });

    it("returns 200 when data found", async () => {
      client.query.mockResolvedValueOnce({ rowCount: 1, rows: [{ id: 1 }] });
      const req = createReq({ body: { shop_id: "s1", admin_name: "a" } });
      const res = createRes();

      await getAdminSales(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
    });

    it("returns 500 on error", async () => {
      client.query.mockRejectedValueOnce(new Error("db"));
      const req = createReq({ body: { shop_id: "s1", admin_name: "a" } });
      const res = createRes();

      await getAdminSales(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
    });
  });
});
