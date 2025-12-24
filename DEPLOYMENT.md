# Railway Deployment Guide - IDX Copytrading System

Complete step-by-step guide to deploy the full-stack IDX Copytrading System on Railway.

---

## Prerequisites

- GitHub account
- Railway account (https://railway.app)
- NeoBDM credentials (NEOBDM_USERNAME, NEOBDM_PASSWORD)
- Node.js 18+ (for local frontend development)
- Python 3.9+ (for local backend development)

---

## Project Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Railway Project                         │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐   ┌─────────────────┐                  │
│  │   Web Service   │   │  Cron Service   │                  │
│  │   FastAPI +     │   │  Python Crawler │                  │
│  │   React Static  │   │  (Daily 9 PM)   │                  │
│  │   Port 8000     │   │                 │                  │
│  └────────┬────────┘   └────────┬────────┘                  │
│           │                     │                            │
│           └──────────┬──────────┘                            │
│                      │                                       │
│           ┌──────────▼──────────┐                            │
│           │     PostgreSQL      │                            │
│           │     (Railway DB)    │                            │
│           └─────────────────────┘                            │
└─────────────────────────────────────────────────────────────┘
```

**Estimated Cost:** ~$11-18/month

---

## Step 1: Push Code to GitHub

```bash
cd /Users/admin/github/crawler-neobdm

# Initialize git
git init

# Stage all files
git add .

# Commit
git commit -m "IDX Copytrading: Full-stack implementation"

# Create GitHub repository, then:
git remote add origin https://github.com/YOUR_USERNAME/idx-copytrading.git
git branch -M main
git push -u origin main
```

---

## Step 2: Create Railway Project

1. Go to https://railway.app and sign in
2. Click **"New Project"** → **"Deploy from GitHub repo"**
3. Authorize Railway to access your GitHub
4. Select the `idx-copytrading` repository
5. Railway will create your first service automatically

---

## Step 3: Add PostgreSQL Database

1. In your Railway project dashboard, click **"+ New"**
2. Select **"Database"** → **"PostgreSQL"**
3. Railway provisions a PostgreSQL instance automatically
4. The `DATABASE_URL` variable is created automatically

---

## Step 4: Initialize Database Schema

### Option A: Using Railway CLI (Recommended)

```bash
# Install Railway CLI
npm install -g @railway/cli

# Login
railway login

# Link to your project
railway link

# Run schema script
railway run psql $DATABASE_URL -f schema.sql
```

### Option B: Using Railway Dashboard

1. Click on the PostgreSQL service
2. Go to **"Data"** tab → **"Query"**
3. Paste the contents of `schema.sql` and execute

---

## Step 5: Configure Web Service

Your main service (deployed from GitHub) will be the **Web Service**.

### Set Environment Variables

Go to your service → **"Variables"** tab and add:

| Variable | Value | How to Set |
|----------|-------|------------|
| `DATABASE_URL` | (auto) | Click "Add Reference" → PostgreSQL → DATABASE_URL |
| `NEOBDM_USERNAME` | Your username | Add manually |
| `NEOBDM_PASSWORD` | Your password | Add manually |
| `TZ` | `Asia/Jakarta` | Add manually |

### Verify Build Settings

Railway auto-detects `railway.json`. Verify in **"Settings"**:
- **Start Command:** `cd frontend && npm run build && cd .. && uvicorn api:app --host 0.0.0.0 --port ${PORT:-8000}`
- **Health Check:** `/api/health`

---

## Step 6: Create Cron Service

The cron service runs the daily crawler. You need to create a **second service**:

1. In your project, click **"+ New"** → **"GitHub Repo"**
2. Select the same repository
3. In the new service's **"Settings"**:
   - **Root Directory:** `/` (leave empty)
   - **Start Command:** `python cron_runner.py`
4. Add a **Cron Schedule:** `0 21 * * 1-5`
   - This runs at 9 PM Jakarta time, Monday-Friday

### Set Environment Variables for Cron Service

Same as web service:
- `DATABASE_URL` (reference from PostgreSQL)
- `NEOBDM_USERNAME`
- `NEOBDM_PASSWORD`
- `TZ` = `Asia/Jakarta`

---

## Step 7: Deploy

Railway deploys automatically when you push to GitHub.

To trigger a manual deployment:
1. Go to your service
2. Click **"Deploy"** → **"Deploy Now"**

---

## Step 8: Verify Deployment

### Check Web Service

1. Railway provides a public URL (e.g., `https://your-app.railway.app`)
2. Visit the URL - you should see the React frontend
3. Check `/api/health` - should return database status

### Check Database

```bash
# Connect via Railway CLI
railway run psql $DATABASE_URL

# Verify tables
\dt

# Check broker data
SELECT COUNT(*) FROM brokers;
-- Should return: 90
```

### Trigger Test Crawl

```bash
# Run cron manually via Railway CLI
railway run python cron_runner.py
```

---

## Local Development

### Backend (FastAPI)

```bash
# Install Python dependencies
pip install -r requirements.txt

# Set environment variables
export DATABASE_URL=postgresql://user:pass@localhost:5432/idx_copytrading
export NEOBDM_USERNAME=your_username
export NEOBDM_PASSWORD=your_password

# Run API server
uvicorn api:app --reload --port 8000
```

### Frontend (React)

```bash
cd frontend

# Install dependencies
npm install

# Run dev server (proxies API to localhost:8000)
npm run dev
```

### Build for Production

```bash
cd frontend
npm run build
# Output goes to frontend/dist/
```

---

## Troubleshooting

### Frontend Not Loading

1. Check `frontend/dist/` exists after build
2. Verify FastAPI is serving static files (check `/` route)
3. Check Railway logs for build errors

### API Returns 500 Error

1. Check `DATABASE_URL` is set correctly
2. Verify database schema was created
3. Check Railway logs for Python errors

### Cron Not Running

1. Verify cron schedule in Railway settings: `0 21 * * 1-5`
2. Check `TZ=Asia/Jakarta` is set
3. Review cron service logs for errors

### No Data After Crawl

1. Check `crawl_log` table for status
2. Verify NeoBDM credentials are correct
3. Run `python cron_runner.py` manually to see errors

---

## Monitoring

### View Crawl History

```sql
SELECT 
    crawl_date,
    status,
    successful_brokers,
    failed_brokers,
    total_rows,
    crawl_end - crawl_start as duration
FROM crawl_log
ORDER BY crawl_date DESC
LIMIT 10;
```

### Health Check API

```bash
curl https://your-app.railway.app/api/health
```

---

## Cost Breakdown

| Service | Plan | Monthly Cost |
|---------|------|--------------|
| PostgreSQL | Starter | ~$5 |
| Web Service | Hobby | ~$5-10 |
| Cron Service | Hobby | ~$1-3 |
| **Total** | | **~$11-18** |

Railway offers $5 free credit/month for Hobby plan users.
