# Water Pipeline Management System

> **Last Updated:** 2025-12-16
> **Time: ** 5:15
 
A full‑stack **Water Pipeline Management System** built with **Node.js, Express, SQLite, Firebase, WebSockets**, and a static frontend. The system is deployed on **Render** and supports **multiple domains** with a production‑ready CORS configuration.

---

## 🚀 Live Deployment

* **Primary URL (Render):**
  [https://jms-1-1-1.onrender.com](https://jms-1-1-1.onrender.com)

* **Frontend & Backend:**
  Served from the same Render service

---

## 🧱 Tech Stack

### Backend

* Node.js (>=18)
* Express
* SQLite3
* Firebase / Firebase Admin
* WebSockets (`ws`)
* CORS (dynamic origin support)

### Frontend

* HTML / CSS / JavaScript
* Fetch API
* Served via Express `public/` directory

---

## 📁 Project Structure

```
project-root/
├── server.js
├── package.json
├── package-lock.json
└── public/
    └── index.html
```

---

## 🔐 CORS Configuration (Current)

The application is configured to allow **multiple domains**, including Render, GitHub Pages, localhost, and future custom domains.

```js
app.use(cors({
  origin: true,
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS']
}));
```

✔ Works with Render domains
✔ Works with GitHub Pages
✔ Works with custom domains
✔ Safe for public APIs

---

## 🌍 Frontend API Configuration

The frontend automatically detects the correct API base URL:

```js
baseURL: `${window.location.origin}/api`
```

This works for:

* `http://localhost:3000`
* `https://jms-1-1-1.onrender.com`
* Any linked custom domain

---

## 📡 Available API Endpoints

### 🛢️ Tanks

* `GET    /api/tanks`
* `POST   /api/tank`
* `GET    /api/tank/:tankId`
* `PUT    /api/tank/:tankId`
* `DELETE /api/tank/:tankId`

### 🚰 Valves

* `GET    /api/valves`
* `POST   /api/valve`
* `GET    /api/valve/:valveId`
* `PUT    /api/valve/:valveId`
* `DELETE /api/valve/:valveId`
* `PATCH /api/valve/:valveId/toggle`

### 🧵 Pipelines

* `GET    /api/pipelines`
* `POST   /api/pipeline`
* `GET    /api/pipeline/:id`
* `PUT    /api/pipeline/:id`
* `DELETE /api/pipeline/:id`

### 📜 History

* `GET    /api/history/:deviceId`
* `GET    /api/history/:deviceId/latest`
* `GET    /api/history/:deviceId/range`
* `DELETE /api/history/:deviceId`

### 🌊 Flow & Supply

* `GET /api/flow/calculate`
* `GET /api/supply/overview`

### 📡 Sensor Data

* `GET /api/sensor/:deviceId`
* `GET /api/sensor/:deviceId/live`

### ⚙️ System

* `GET /api/poll/all`
* `GET /api/export/all`
* `POST /api/import`

---

## 🧪 How to Test

### Browser (GET requests only)

```
https://jms-1-1-1.onrender.com/api/tanks
https://jms-1-1-1.onrender.com/api/valves
```

### curl

```bash
curl https://jms-1-1-1.onrender.com/api/poll/all
```

---

## 🛠️ Render Configuration

* **Root Directory:** *(leave empty)*
* **Build Command:** `npm install`
* **Start Command:** `npm start`
* **Environment:** Node

The server listens on the Render‑assigned port:

```js
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0');
```

---

## 🗓️ Recent Updates (2025‑12‑16)

* ✅ Deployed successfully on Render
* ✅ Frontend moved to `/public` and served via Express
* ✅ API base URL made environment‑agnostic
* ✅ CORS updated to support multiple domains
* ✅ Render port binding verified (port 10000 internal)

---

## 📌 Notes

* `/` serves `index.html`
* `/api/*` serves backend APIs
* No hard‑coded ports in frontend
* Ready for custom domain linking

---

## 📄 License

ISC License

---

**Status:** 🟢 Production‑ready and stable
