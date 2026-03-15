# Backend Overview

This file summarizes the available backend endpoints based on the current Express routing setup in `abdumannon_sale_backends/app.js` and the route files under `abdumannon_sale_backends/routes/`.

## Auth & Public Endpoints

These are **public** (no token required):

- `GET /` — health check (“Hello from Express!”)
- `GET /tables` — HTML view of DB tables/columns
- `GET /api-docs` — Swagger UI
- `POST /auth/generate/superuser`
- `POST /auth/generate/admin`
- `POST /auth/login/superuser`
- `POST /auth/login/admin`
- `POST /auth/logout`

## Protected Endpoints (require token)

All routes below are behind `validateToken` middleware. In general, the backend expects `Authorization` header (and some endpoints use `uuid`, `shop_id`, etc. in headers/body).

### Statistics — `/statistics`
- `GET /statistics/finance/main`
- `GET /statistics/high-stock`
- `GET /statistics/low-stock`
- `GET /statistics/day-stats`
- `GET /statistics/graph-weekly`

### Finance — `/finance`
- `GET /finance`
- `POST /finance`
- `PUT /finance/:id`
- `DELETE /finance/:id`
- `POST /finance/:id/payment`

### Superadmin — `/superadmin`
- `GET /superadmin/`
- `POST /superadmin/`
- `PUT /superadmin/`
- `DELETE /superadmin/`

### Admin — `/admin`
- `POST /admin/`
- `PUT /admin/`
- `DELETE /admin/`
- `GET /admin/one`
- `POST /admin/admins`

### Product — `/product`
- `GET /product/all`
- `GET /product/one`
- `POST /product/`
- `PUT /product/`
- `PUT /product/restock`
- `DELETE /product/`
- `POST /product/shop-products`

### Brand — `/brand`
- `POST /brand/`
- `GET /brand/one`
- `POST /brand/create`
- `PUT /brand/`
- `DELETE /brand/`

### Category — `/category`
- `POST /category/`
- `GET /category/one`
- `POST /category/create`
- `PUT /category/`
- `DELETE /category/`

### Permission — `/permission`
- `POST /permission/`
- `POST /permission/permissions`

### Shop / Branch — `/shop`
- `GET /shop/`
- `POST /shop/branches`
- `POST /shop/getbranch`
- `POST /shop/branch`
- `POST /shop/update_shop`
- `PUT /shop/branch`
- `DELETE /shop/branch`

### Sales — `/sales`
- `POST /sales/update-sale`
- `DELETE /sales/delete`
- `POST /sales/get-sale`
- `POST /sales/all`
- `GET /sales/by-id`
- `POST /sales/`
- `POST /sales/admin/sales`

### Reports — `/report`
- `POST /report/shop`

### Debts — `/debts`
- `POST /debts/all`
- `GET /debts/branch`
- `POST /debts/unreturned`
- `POST /debts/statistics`
- `POST /debts/by-id`
- `POST /debts/by-customer`
- `POST /debts/create`
- `POST /debts/update`
- `POST /debts/mark-returned`
- `DELETE /debts/delete`

### Backup — `/backup`
- `POST /backup/restore`
- `POST /backup/backup`
- `POST /backup/backup-to-sheets`
- `POST /backup/restore-from-sheets`

### Wagons — `/wagons`
- `GET /wagons/all`
- `POST /wagons/get-by-id`
- `POST /wagons/get-by-number`
- `POST /wagons/create`
- `PUT /wagons/update`
- `DELETE /wagons/delete`
- `POST /wagons/by-indicator`
- `POST /wagons/by-shop`

## Notes

- `routes/routes.js` appears to be a legacy/duplicate debts route file. It is **not** mounted in `app.js` (the app uses `routes/debtRoutes.js`).
- Actual request/response shapes live in controller files under `abdumannon_sale_backends/controllers/`.
