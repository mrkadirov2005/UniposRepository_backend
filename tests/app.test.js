import request from "supertest";
import { describe, it, expect } from "vitest";
import app from "../app.js";

describe("GET /", () => {
  it("returns a greeting", async () => {
    const response = await request(app).get("/");

    expect(response.status).toBe(200);
    expect(response.text).toBe("Hello from Express!");
  });
});
