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
  v4: vi.fn(() => "debt-1"),
}));

import { client } from "../config/dbcon.js";
import { logger } from "../middleware/Logger.js";
import { extractJWT } from "../middleware/extractToken.js";
import { v4 as uuidv4 } from "uuid";

import {
  getAllDebts,
  getDebtById,
  getDebtsByBranch,
  getDebtsByCustomer,
  getUnreturnedDebts,
  createDebt,
  updateDebt,
  markDebtAsReturned,
  deleteDebt,
  getDebtStatistics,
} from "../controllers/debtcontroller.js";

beforeEach(() => {
  client.query.mockReset();
  logger.mockReset();
  extractJWT.mockReset();
  extractJWT.mockReturnValue("jwt-user");
  uuidv4.mockReset();
  uuidv4.mockReturnValue("debt-1");
});

describe("debtcontroller", () => {
  describe("getAllDebts", () => {
    it("returns 400 when shop_id missing", async () => {
      const req = createReq({ body: {}, headers: {} });
      const res = createRes();

      await getAllDebts(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 200 with data on success", async () => {
      client.query.mockResolvedValueOnce({ rows: [{ id: 1 }] });
      const req = createReq({ body: { shop_id: "s1" }, headers: {} });
      const res = createRes();

      await getAllDebts(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
    });

    it("returns 500 on error", async () => {
      client.query.mockRejectedValueOnce(new Error("db"));
      const req = createReq({ body: { shop_id: "s1" }, headers: {} });
      const res = createRes();

      await getAllDebts(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
    });
  });

  describe("getDebtById", () => {
    it("returns 400 when id missing", async () => {
      const req = createReq({ body: {}, headers: {} });
      const res = createRes();

      await getDebtById(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 404 when debt not found", async () => {
      client.query.mockResolvedValueOnce({ rows: [] });
      const req = createReq({ body: { id: "d1", shop_id: "s1" }, headers: {} });
      const res = createRes();

      await getDebtById(req, res);

      expect(res.status).toHaveBeenCalledWith(404);
    });

    it("returns 200 when debt found", async () => {
      client.query.mockResolvedValueOnce({ rows: [{ id: "d1" }] });
      const req = createReq({ body: { id: "d1", shop_id: "s1" }, headers: {} });
      const res = createRes();

      await getDebtById(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
    });

    it("returns 500 on error", async () => {
      client.query.mockRejectedValueOnce(new Error("db"));
      const req = createReq({ body: { id: "d1", shop_id: "s1" }, headers: {} });
      const res = createRes();

      await getDebtById(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
    });
  });

  describe("getDebtsByBranch", () => {
    it("returns 400 when branch_id missing", async () => {
      const req = createReq({ body: { shop_id: "s1" }, headers: {} });
      const res = createRes();

      await getDebtsByBranch(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 200 with data on success", async () => {
      client.query.mockResolvedValueOnce({ rows: [{ id: 1 }] });
      const req = createReq({
        body: { shop_id: "s1" },
        headers: { branch_id: "b1" },
      });
      const res = createRes();

      await getDebtsByBranch(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
    });

    it("returns 500 on error", async () => {
      client.query.mockRejectedValueOnce(new Error("db"));
      const req = createReq({
        body: { shop_id: "s1" },
        headers: { branch_id: "b1" },
      });
      const res = createRes();

      await getDebtsByBranch(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
    });
  });

  describe("getDebtsByCustomer", () => {
    it("returns 400 when required fields missing", async () => {
      const req = createReq({ body: { name: "n" }, headers: {} });
      const res = createRes();

      await getDebtsByCustomer(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 200 with data on success", async () => {
      client.query.mockResolvedValueOnce({ rows: [{ id: 1 }] });
      const req = createReq({ body: { name: "n", shop_id: "s1" }, headers: {} });
      const res = createRes();

      await getDebtsByCustomer(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
    });

    it("returns 500 on error", async () => {
      client.query.mockRejectedValueOnce(new Error("db"));
      const req = createReq({ body: { name: "n", shop_id: "s1" }, headers: {} });
      const res = createRes();

      await getDebtsByCustomer(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
    });
  });

  describe("getUnreturnedDebts", () => {
    it("returns 400 when shop_id missing", async () => {
      const req = createReq({ body: {}, headers: {} });
      const res = createRes();

      await getUnreturnedDebts(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 200 with data on success", async () => {
      client.query.mockResolvedValueOnce({ rows: [{ id: 1 }] });
      const req = createReq({ body: { shop_id: "s1" }, headers: {} });
      const res = createRes();

      await getUnreturnedDebts(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
    });

    it("returns 500 on error", async () => {
      client.query.mockRejectedValueOnce(new Error("db"));
      const req = createReq({ body: { shop_id: "s1" }, headers: {} });
      const res = createRes();

      await getUnreturnedDebts(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
    });
  });

  describe("createDebt", () => {
    it("returns 400 when required fields missing", async () => {
      const req = createReq({ body: { name: "n" }, headers: {} });
      const res = createRes();

      await createDebt(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 201 when created", async () => {
      client.query.mockResolvedValueOnce({ rows: [{ id: "d1" }] });
      const req = createReq({
        body: {
          name: "n",
          amount: 10,
          product_names: ["p1"],
          branch_id: "b1",
          shop_id: "s1",
          admin_id: "a1",
        },
        headers: {},
      });
      const res = createRes();

      await createDebt(req, res);

      expect(uuidv4).toHaveBeenCalled();
      expect(res.status).toHaveBeenCalledWith(201);
    });

    it("returns 500 on error", async () => {
      client.query.mockRejectedValueOnce(new Error("db"));
      const req = createReq({
        body: {
          name: "n",
          amount: 10,
          product_names: ["p1"],
          branch_id: "b1",
          shop_id: "s1",
          admin_id: "a1",
        },
        headers: {},
      });
      const res = createRes();

      await createDebt(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
    });
  });

  describe("updateDebt", () => {
    it("returns 400 when id missing", async () => {
      const req = createReq({ body: {}, headers: {} });
      const res = createRes();

      await updateDebt(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 404 when debt not found", async () => {
      client.query.mockResolvedValueOnce({ rows: [] });
      const req = createReq({ body: { id: "d1" }, headers: {} });
      const res = createRes();

      await updateDebt(req, res);

      expect(res.status).toHaveBeenCalledWith(404);
    });

    it("returns 200 when updated", async () => {
      client.query.mockResolvedValueOnce({ rows: [{ id: "d1" }] });
      const req = createReq({ body: { id: "d1", name: "n" }, headers: {} });
      const res = createRes();

      await updateDebt(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
    });

    it("returns 500 on error", async () => {
      client.query.mockRejectedValueOnce(new Error("db"));
      const req = createReq({ body: { id: "d1", name: "n" }, headers: {} });
      const res = createRes();

      await updateDebt(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
    });
  });

  describe("markDebtAsReturned", () => {
    it("returns 400 when id missing", async () => {
      const req = createReq({ body: {}, headers: {} });
      const res = createRes();

      await markDebtAsReturned(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 404 when debt not found", async () => {
      client.query.mockResolvedValueOnce({ rows: [] });
      const req = createReq({ body: { id: "d1" }, headers: {} });
      const res = createRes();

      await markDebtAsReturned(req, res);

      expect(res.status).toHaveBeenCalledWith(404);
    });

    it("returns 200 when marked", async () => {
      client.query.mockResolvedValueOnce({ rows: [{ id: "d1", name: "n" }] });
      const req = createReq({ body: { id: "d1" }, headers: {} });
      const res = createRes();

      await markDebtAsReturned(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
    });

    it("returns 500 on error", async () => {
      client.query.mockRejectedValueOnce(new Error("db"));
      const req = createReq({ body: { id: "d1" }, headers: {} });
      const res = createRes();

      await markDebtAsReturned(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
    });
  });

  describe("deleteDebt", () => {
    it("returns 400 when id missing", async () => {
      const req = createReq({ body: {}, headers: {} });
      const res = createRes();

      await deleteDebt(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 404 when debt not found", async () => {
      client.query.mockResolvedValueOnce({ rows: [] });
      const req = createReq({ body: {}, headers: { id: "d1" } });
      const res = createRes();

      await deleteDebt(req, res);

      expect(res.status).toHaveBeenCalledWith(404);
    });

    it("returns 200 when deleted", async () => {
      client.query.mockResolvedValueOnce({ rows: [{ id: "d1", name: "n" }] });
      const req = createReq({ body: {}, headers: { id: "d1" } });
      const res = createRes();

      await deleteDebt(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
    });

    it("returns 500 on error", async () => {
      client.query.mockRejectedValueOnce(new Error("db"));
      const req = createReq({ body: {}, headers: { id: "d1" } });
      const res = createRes();

      await deleteDebt(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
    });
  });

  describe("getDebtStatistics", () => {
    it("returns 400 when shop_id missing", async () => {
      const req = createReq({ body: {}, headers: {} });
      const res = createRes();

      await getDebtStatistics(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 200 with data on success", async () => {
      client.query.mockResolvedValueOnce({ rows: [{ total_debts: 1 }] });
      const req = createReq({ body: { shop_id: "s1" }, headers: {} });
      const res = createRes();

      await getDebtStatistics(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
    });

    it("returns 500 on error", async () => {
      client.query.mockRejectedValueOnce(new Error("db"));
      const req = createReq({ body: { shop_id: "s1" }, headers: {} });
      const res = createRes();

      await getDebtStatistics(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
    });
  });
});
