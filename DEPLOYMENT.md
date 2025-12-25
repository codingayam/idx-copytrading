# Railway Deployment Guide - IDX Copytrading System

Complete step-by-step guide to deploy the full-stack IDX Copytrading System on Railway using Docker.

---

## Prerequisites

- GitHub account
- Railway account (https://railway.app)
- NeoBDM credentials (NEOBDM_USERNAME, NEOBDM_PASSWORD)

---

## Project Output

The system deploys as two services built from the same Docker image:

1. **Web Service**: FastAPI backend + React frontend
2. **Cron Service**: Python crawler running daily

---

## Step 1: Push Code to GitHub

```bash
cd /Users/admin/github/crawler-neobdm
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/YOUR_USERNAME/idx-copytrading.git
git branch -M main
git push -u origin main
```

---

## Step 2: Create Railway Project

1. Go to https://railway.app
2. Click **"New Project"** → **"Deploy from GitHub repo"**
3. Select your `idx-copytrading` repository
4. Railway will auto-detect the `Dockerfile` and start building

---

## Step 3: Add PostgreSQL Database

1. Click **"+ New"** → **"Database"** → **"PostgreSQL"**
2. Wait for it to be provisoned

---

## Step 4: Configure Web Service

Your main service (deployed from GitHub) is the Web Service.

1. Go to **"Variables"** tab:
   - `DATABASE_URL`: Click "Add Reference" → select PostgreSQL
   - `NEOBDM_USERNAME`: Your username
   - `NEOBDM_PASSWORD`: Your password
   - `TZ`: `Asia/Jakarta`

2. Go to **"Settings"**:
   - Railway uses `railway.json` to override the Docker CMD if needed. 
   - Verify Start Command: `./start.sh`
   - Verify Watch Path: `/api/health`

---

## Step 5: Create Cron Service

**Why a separate service?** 
Railway follows a "one process per service" model. You cannot run both a web server and a background job manager reliably in the same container.
- **Service 1 (Web)**: Always listens on port 8000 for HTTP requests.
- **Service 2 (Cron)**: Starts up, runs the Python script, and shuts down (or sleeps). Since it has a different start command (`python3 cron_runner.py`), it must be a separate service instance.

1. Click **"+ New"** → **"GitHub Repo"**
2. Select the **SAME** repository again
3. Go to **"Variables"** tab:
   - Add all the same variables (`DATABASE_URL`, `NEOBDM_...`, `TZ`)
   - **Tip:** You can use "Shared Variables" in Railway to manage these in one place

4. Go to **"Settings"**:
   - **Start Command**: `python3 cron_runner.py`
   - **Cron Schedule**: `0 13 * * 1-5` (9 PM SGT / 1 PM UTC, Mon-Fri)

---

## Step 6: Initialize Database

1. Install Railway CLI: `npm install -g @railway/cli`
2. Run schema:
   ```bash
   railway login
   # Link to your project
   railway link
   # IMPORTANT: When prompted for "Select a service", select your "Postgres" service (not the web service).

   # Run schema script
   cat schema.sql | railway connect Postgres

   # Alternative (if you have psql installed and DATABASE_URL is public)
   # railway run psql $DATABASE_URL -f schema.sql
   ```

---

## Verification

- **Web**: Open the provided Railway URL. You should see the dashboard.
- **API**: Check `/api/health`.
- **Cron**: Run `railway run python3 cron_runner.py` to test the crawler manually.
