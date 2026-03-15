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

import { client } from "../config/dbcon.js";
import { logger } from "../middleware/Logger.js";
import { extractJWT } from "../middleware/extractToken.js";

import {
  getProducts,
  getShopProducts,
  getSingleProduct,
  createNewProduct,
  updateProduct,
  restockProduct,
  deleteProduct,
} from "../controllers/productscontroller.js";

beforeEach(() => {
  client.query.mockReset();
  logger.mockReset();
  extractJWT.mockReset();
  extractJWT.mockReturnValue("jwt-user");
});

describe("productscontroller", () => {
  describe("getProducts", () => {
    it("returns rows on success", async () => {
      client.query.mockResolvedValueOnce({ rows: [{ id: 1 }] });
      const req = createReq({ headers: { authorization: "t", uuid: "u1" } });
      const res = createRes();

      await getProducts(req, res);

      expect(res.json).toHaveBeenCalledWith([{ id: 1 }]);
    });

    it("returns 500 on error", async () => {
      client.query.mockRejectedValueOnce(new Error("db"));
      const req = createReq({ headers: { authorization: "t", uuid: "u1" } });
      const res = createRes();

      await getProducts(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
    });
  });

  describe("getShopProducts", () => {
    it("returns 400 when shop_id missing", async () => {
      const req = createReq({ body: {}, headers: { authorization: "t" } });
      const res = createRes();

      await getShopProducts(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 404 when no products found", async () => {
      client.query.mockResolvedValueOnce({ rowCount: 0, rows: [] });
      const req = createReq({
        body: { shop_id: "s1" },
        headers: { authorization: "t", branch: "100" },
      });
      const res = createRes();

      await getShopProducts(req, res);

      expect(res.status).toHaveBeenCalledWith(404);
    });

    it("returns 200 with data when products exist", async () => {
      client.query.mockResolvedValueOnce({ rowCount: 1, rows: [{ id: 1 }] });
      const req = createReq({
        body: { shop_id: "s1" },
        headers: { authorization: "t", branch: "100" },
      });
      const res = createRes();

      await getShopProducts(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
      expect(res.json).toHaveBeenCalledWith(
        expect.objectContaining({ data: [{ id: 1 }] })
      );
    });

    it("returns 500 on error", async () => {
      client.query.mockRejectedValueOnce(new Error("db"));
      const req = createReq({
        body: { shop_id: "s1" },
        headers: { authorization: "t", branch: "100" },
      });
      const res = createRes();

      await getShopProducts(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
    });
  });

  describe("getSingleProduct", () => {
    it("returns 400 when body missing", async () => {
      const req = createReq({ body: null, headers: { authorization: "t" } });
      const res = createRes();

      await getSingleProduct(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 400 when id missing", async () => {
      const req = createReq({ body: {}, headers: { authorization: "t" } });
      const res = createRes();

      await getSingleProduct(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 404 when product not found", async () => {
      client.query.mockResolvedValueOnce({ rows: [] });
      const req = createReq({
        body: { id: 1 },
        headers: { authorization: "t" },
      });
      const res = createRes();

      await getSingleProduct(req, res);

      expect(res.status).toHaveBeenCalledWith(404);
    });

    it("returns product on success", async () => {
      client.query.mockResolvedValueOnce({ rows: [{ id: 1 }] });
      const req = createReq({
        body: { id: 1 },
        headers: { authorization: "t" },
      });
      const res = createRes();

      await getSingleProduct(req, res);

      expect(res.json).toHaveBeenCalledWith({ id: 1 });
    });

    it("returns 500 on error", async () => {
      client.query.mockRejectedValueOnce(new Error("db"));
      const req = createReq({
        body: { id: 1 },
        headers: { authorization: "t" },
      });
      const res = createRes();

      await getSingleProduct(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
    });
  });

  describe("createNewProduct", () => {
    it("returns 400 when body missing", async () => {
      const req = createReq({ body: null, headers: { authorization: "t" } });
      const res = createRes();

      await createNewProduct(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 400 when required fields are missing", async () => {
      const req = createReq({ body: { name: "x" }, headers: { authorization: "t" } });
      const res = createRes();

      await createNewProduct(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 201 when product is created", async () => {
      client.query.mockResolvedValueOnce({ rows: [{ id: 1 }] });
      const req = createReq({
        body: {
          name: "n",
          brand_id: "b",
          scale: "s",
          availability: 10,
          total: 10,
          sell_price: 5,
          branch: "1",
        },
        headers: { authorization: "t" },
      });
      const res = createRes();

      await createNewProduct(req, res);

      expect(res.status).toHaveBeenCalledWith(201);
    });

    it("returns 500 on error", async () => {
      client.query.mockRejectedValueOnce(new Error("db"));
      const req = createReq({
        body: {
          name: "n",
          brand_id: "b",
          scale: "s",
          availability: 10,
          total: 10,
          sell_price: 5,
          branch: "1",
        },
        headers: { authorization: "t" },
      });
      const res = createRes();

      await createNewProduct(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
    });
  });

  describe("updateProduct", () => {
    it("returns 400 when id missing", async () => {
      const req = createReq({ body: {}, headers: { uuid: "u1" } });
      const res = createRes();

      await updateProduct(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 400 when body empty", async () => {
      const req = createReq({ body: { id: 1 }, headers: { uuid: "u1" } });
      const res = createRes();

      await updateProduct(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 404 when product not found", async () => {
      client.query.mockResolvedValueOnce({ rowCount: 0 });
      const req = createReq({
        body: { id: 1, name: "n" },
        headers: { uuid: "u1" },
      });
      const res = createRes();

      await updateProduct(req, res);

      expect(res.status).toHaveBeenCalledWith(404);
    });

    it("returns 200 when updated", async () => {
      client.query.mockResolvedValueOnce({ rowCount: 1, rows: [{ id: 1 }] });
      const req = createReq({
        body: { id: 1, name: "n" },
        headers: { uuid: "u1" },
      });
      const res = createRes();

      await updateProduct(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
    });

    it("returns 500 on error", async () => {
      client.query.mockRejectedValueOnce(new Error("db"));
      const req = createReq({
        body: { id: 1, name: "n" },
        headers: { uuid: "u1" },
      });
      const res = createRes();

      await updateProduct(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
    });
  });

  describe("restockProduct", () => {
    it("returns 400 when id missing", async () => {
      const req = createReq({ body: { total: 1, availability: 2 } });
      const res = createRes();

      await restockProduct(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 404 when product not found", async () => {
      client.query.mockResolvedValueOnce({ rowCount: 0 });
      const req = createReq({
        body: { id: 1, total: 1, availability: 2 },
        headers: { uuid: "u1" },
      });
      const res = createRes();

      await restockProduct(req, res);

      expect(res.status).toHaveBeenCalledWith(404);
    });

    it("returns 200 when restocked", async () => {
      client.query.mockResolvedValueOnce({ rowCount: 1, rows: [{ id: 1 }] });
      const req = createReq({
        body: { id: 1, total: 1, availability: 2 },
        headers: { uuid: "u1" },
      });
      const res = createRes();

      await restockProduct(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
    });

    it("returns 500 on error", async () => {
      client.query.mockRejectedValueOnce(new Error("db"));
      const req = createReq({
        body: { id: 1, total: 1, availability: 2 },
        headers: { uuid: "u1" },
      });
      const res = createRes();

      await restockProduct(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
    });
  });

  describe("deleteProduct", () => {
    it("returns 400 when body missing", async () => {
      const req = createReq({ body: null, headers: { authorization: "t" } });
      const res = createRes();

      await deleteProduct(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 400 when id missing", async () => {
      const req = createReq({ body: {}, headers: { authorization: "t" } });
      const res = createRes();

      await deleteProduct(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 404 when product not found", async () => {
      client.query.mockResolvedValueOnce({ rowCount: 0 });
      const req = createReq({
        body: { id: 1 },
        headers: { authorization: "t" },
      });
      const res = createRes();

      await deleteProduct(req, res);

      expect(res.status).toHaveBeenCalledWith(404);
    });

    it("returns 200 when deleted", async () => {
      client.query.mockResolvedValueOnce({ rowCount: 1, rows: [{ id: 1 }] });
      const req = createReq({
        body: { id: 1 },
        headers: { authorization: "t" },
      });
      const res = createRes();

      await deleteProduct(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
    });

    it("returns 500 on error", async () => {
      client.query.mockRejectedValueOnce(new Error("db"));
      const req = createReq({
        body: { id: 1 },
        headers: { authorization: "t" },
      });
      const res = createRes();

      await deleteProduct(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
    });
  });
});
