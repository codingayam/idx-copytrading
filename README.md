# IDX Copytrading System

Full-stack application for tracking broker trading activity on the Indonesia Stock Exchange (IDX). Crawls data from NeoBDM, stores in PostgreSQL, and provides a React dashboard for visualization.

![Architecture](https://img.shields.io/badge/FastAPI-009688?style=flat&logo=fastapi&logoColor=white)
![React](https://img.shields.io/badge/React-61DAFB?style=flat&logo=react&logoColor=black)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=flat&logo=postgresql&logoColor=white)
![Railway](https://img.shields.io/badge/Railway-0B0D0E?style=flat&logo=railway&logoColor=white)

## Features

- 🕷️ **Automated Daily Crawls** - Scheduled at 9 PM Jakarta time (weekdays)
- 📊 **Three Analysis Views** - Broker tab, Ticker tab, Insights tab
- 📈 **Volume-Weighted Averages** - Accurate price averaging across periods
- 🗓️ **IDX Holiday Awareness** - Skips crawls on non-trading days
- 🌙 **Dark Mode UI** - Modern glassmorphism design
- 🚀 **Railway Deployment** - Serverless with cron scheduling

## Quick Start

### Local Development

```bash
# Clone repository
git clone https://github.com/YOUR_USERNAME/idx-copytrading.git
cd idx-copytrading

# Set up Python environment
pip install -r requirements.txt

# Set environment variables
cp .env.example .env
# Edit .env with your credentials

# Run API server
uvicorn api:app --reload --port 8000

# In another terminal, run frontend
cd frontend
npm install
npm run dev
```

Open http://localhost:5173 to view the dashboard.

### Database Setup

```bash
# Create PostgreSQL database
createdb idx_copytrading

# Run schema
psql idx_copytrading -f schema.sql
```

## Project Structure

```
idx-copytrading/
├── api.py                 # FastAPI REST API
├── broker_crawler.py      # NeoBDM data crawler
├── cron_runner.py         # Railway cron orchestrator
├── aggregates.py          # Data aggregation logic
├── db.py                  # Database operations
├── holidays.py            # IDX holiday calendar
├── schema.sql             # PostgreSQL schema
├── requirements.txt       # Python dependencies
├── railway.json           # Railway web service config
├── railway.cron.json      # Railway cron service config
├── nixpacks.toml          # Nixpacks build config
├── DEPLOYMENT.md          # Deployment guide
└── frontend/              # React application
    ├── src/
    │   ├── pages/         # Tab components
    │   ├── components/    # Reusable UI components
    │   └── api/           # API client
    └── package.json
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/health` | Health check with DB status |
| `GET /api/brokers` | List all 90 brokers |
| `GET /api/brokers/{code}/aggregates` | Broker aggregates by period |
| `GET /api/brokers/{code}/trades` | Broker trades with pagination |
| `GET /api/tickers` | List all active symbols |
| `GET /api/tickers/{symbol}/aggregates` | Ticker aggregates by period |
| `GET /api/tickers/{symbol}/brokers` | Brokers trading this ticker |
| `GET /api/insights` | Top movers and market stats |

## Database Schema

| Table | Purpose |
|-------|---------|
| `brokers` | 90 broker reference data |
| `symbols` | Ticker reference with first/last seen |
| `broker_trades` | Raw daily trade data |
| `aggregates_by_broker` | Period aggregates by broker |
| `aggregates_by_ticker` | Period aggregates by ticker |
| `aggregates_broker_symbol` | Broker-symbol cross-reference |
| `daily_totals` | Market totals for % calculations |
| `daily_insights` | Top movers for insights |
| `crawl_log` | Crawl history and status |

## Deployment

See [DEPLOYMENT.md](DEPLOYMENT.md) for complete Railway deployment instructions.

**Quick Overview:**
1. Push to GitHub
2. Create Railway project from repo
3. Add PostgreSQL database
4. Run schema.sql
5. Set environment variables
6. Create cron service for daily crawl

**Estimated Cost:** ~$11-18/month on Railway

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `DATABASE_URL` | PostgreSQL connection string | Yes |
| `NEOBDM_USERNAME` | NeoBDM login | Yes |
| `NEOBDM_PASSWORD` | NeoBDM password | Yes |
| `TZ` | Timezone (Asia/Jakarta) | Recommended |

## License

MIT
