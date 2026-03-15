import { describe, it, expect, vi, beforeEach } from "vitest";
import { createReq, createRes } from "./helpers.js";

vi.mock("../config/dbcon.js", () => ({
  client: { query: vi.fn() },
}));
vi.mock("../middleware/Logger.js", () => ({
  logger: vi.fn(),
}));
vi.mock("jsonwebtoken", () => ({
  default: { sign: vi.fn(() => "token") },
}));

import { client } from "../config/dbcon.js";
import { logger } from "../middleware/Logger.js";
import jwt from "jsonwebtoken";

import {
  generateSuperAdminToken,
  generateAdminToken,
  loginSuperUser,
  loginAdmin,
  handleLogOut,
} from "../controllers/authcontroller.js";

beforeEach(() => {
  client.query.mockReset();
  logger.mockReset();
  jwt.sign.mockReset();
  jwt.sign.mockReturnValue("token");
});

describe("authcontroller", () => {
  describe("generateSuperAdminToken", () => {
    it("returns 400 when body is missing", async () => {
      const req = createReq({ body: null });
      const res = createRes();

      await generateSuperAdminToken(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 400 when required fields are missing", async () => {
      const req = createReq({ body: { uuid: "1" } });
      const res = createRes();

      await generateSuperAdminToken(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 404 when superuser not found", async () => {
      client.query.mockResolvedValueOnce({ rows: [] });
      const req = createReq({
        body: { uuid: "1", name: "A", phonenumber: "1", password: "x" },
      });
      const res = createRes();

      await generateSuperAdminToken(req, res);

      expect(res.status).toHaveBeenCalledWith(404);
    });

    it("returns 200 with tokens when successful", async () => {
      client.query
        .mockResolvedValueOnce({ rows: [{ uuid: "1" }] })
        .mockResolvedValueOnce({ rowCount: 1 });
      const req = createReq({
        body: { uuid: "1", name: "A", phonenumber: "1", password: "x" },
      });
      const res = createRes();

      await generateSuperAdminToken(req, res);

      expect(jwt.sign).toHaveBeenCalledTimes(2);
      expect(res.status).toHaveBeenCalledWith(200);
      expect(res.json).toHaveBeenCalledWith(
        expect.objectContaining({
          accessToken: "token",
          refreshToken: "token",
        })
      );
    });
  });

  describe("generateAdminToken", () => {
    it("returns 400 when body is missing", async () => {
      const req = createReq({ body: null });
      const res = createRes();

      await generateAdminToken(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 400 when required fields are missing", async () => {
      const req = createReq({ body: { uuid: "1" } });
      const res = createRes();

      await generateAdminToken(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 404 when admin not found", async () => {
      client.query.mockResolvedValueOnce({ rows: [] });
      const req = createReq({
        body: { uuid: "1", name: "A", phonenumber: "1", password: "x" },
      });
      const res = createRes();

      await generateAdminToken(req, res);

      expect(res.status).toHaveBeenCalledWith(404);
    });

    it("returns 200 when successful", async () => {
      client.query.mockResolvedValueOnce({ rows: [{ uuid: "1" }] });
      const req = createReq({
        body: { uuid: "1", name: "A", phonenumber: "1", password: "x" },
      });
      const res = createRes();

      await generateAdminToken(req, res);

      expect(jwt.sign).toHaveBeenCalledTimes(2);
      expect(res.status).toHaveBeenCalledWith(200);
    });
  });

  describe("loginSuperUser", () => {
    it("returns 400 when body is missing", async () => {
      const req = createReq({ body: null });
      const res = createRes();

      await loginSuperUser(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 400 when required fields are missing", async () => {
      const req = createReq({ body: { name: "A" } });
      const res = createRes();

      await loginSuperUser(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 404 when user not found", async () => {
      client.query.mockResolvedValueOnce({ rows: [] });
      const req = createReq({ body: { name: "A", password: "x" } });
      const res = createRes();

      await loginSuperUser(req, res);

      expect(res.status).toHaveBeenCalledWith(404);
    });

    it("returns 200 when successful", async () => {
      client.query
        .mockResolvedValueOnce({
          rows: [{ uuid: "u1", shop_id: "s1", refreshtoken: "r1" }],
        })
        .mockResolvedValueOnce({ rowCount: 1 });
      const req = createReq({ body: { name: "A", password: "x" } });
      const res = createRes();

      await loginSuperUser(req, res);

      expect(jwt.sign).toHaveBeenCalledTimes(1);
      expect(res.status).toHaveBeenCalledWith(200);
    });

    it("returns 500 on query error", async () => {
      client.query.mockRejectedValueOnce(new Error("db"));
      const req = createReq({ body: { name: "A", password: "x" } });
      const res = createRes();

      await loginSuperUser(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
    });
  });

  describe("loginAdmin", () => {
    it("returns 400 when body is missing", async () => {
      const req = createReq({ body: null });
      const res = createRes();

      await loginAdmin(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 400 when required fields are missing", async () => {
      const req = createReq({ body: { name: "A" } });
      const res = createRes();

      await loginAdmin(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 404 when admin not found", async () => {
      client.query.mockResolvedValueOnce({ rows: [] });
      const req = createReq({ body: { name: "A", password: "x" } });
      const res = createRes();

      await loginAdmin(req, res);

      expect(res.status).toHaveBeenCalledWith(404);
    });

    it("returns 200 when successful", async () => {
      client.query
        .mockResolvedValueOnce({ rows: [{ uuid: "u1", refreshtoken: "r1" }] })
        .mockResolvedValueOnce({ rowCount: 1 });
      const req = createReq({ body: { name: "A", password: "x" } });
      const res = createRes();

      await loginAdmin(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
    });
  });

  describe("handleLogOut", () => {
    it("returns 400 when role or uuid missing", async () => {
      const req = createReq({ body: { uuid: "u1" } });
      const res = createRes();

      await handleLogOut(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });

    it("returns 404 when admin not found", async () => {
      client.query.mockResolvedValueOnce({ rowCount: 0 });
      const req = createReq({ body: { uuid: "u1", role: "admin" } });
      const res = createRes();

      await handleLogOut(req, res);

      expect(res.status).toHaveBeenCalledWith(404);
    });

    it("returns 200 when admin logout succeeds", async () => {
      client.query
        .mockResolvedValueOnce({ rowCount: 1 })
        .mockResolvedValueOnce({ rowCount: 1 });
      const req = createReq({ body: { uuid: "u1", role: "admin" } });
      const res = createRes();

      await handleLogOut(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
    });

    it("returns 404 when superuser not found", async () => {
      client.query.mockResolvedValueOnce({ rowCount: 0 });
      const req = createReq({ body: { uuid: "u1", role: "superuser" } });
      const res = createRes();

      await handleLogOut(req, res);

      expect(res.status).toHaveBeenCalledWith(404);
    });

    it("returns 200 when superuser logout succeeds", async () => {
      client.query
        .mockResolvedValueOnce({ rowCount: 1 })
        .mockResolvedValueOnce({ rowCount: 1 });
      const req = createReq({ body: { uuid: "u1", role: "superuser" } });
      const res = createRes();

      await handleLogOut(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
    });

    it("returns 400 for invalid role", async () => {
      const req = createReq({ body: { uuid: "u1", role: "other" } });
      const res = createRes();

      await handleLogOut(req, res);

      expect(res.status).toHaveBeenCalledWith(400);
    });
  });
});
