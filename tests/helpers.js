import { vi } from "vitest";

export const createRes = () => {
  const res = {};
  res.status = vi.fn().mockReturnValue(res);
  res.json = vi.fn().mockReturnValue(res);
  res.send = vi.fn().mockReturnValue(res);
  return res;
};

export const createReq = ({ body = {}, headers = {} } = {}) => ({
  body,
  headers,
});
