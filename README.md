# MTProto + V2Ray Scraper

Fetches unique Telegram MTProto proxies and V2Ray/Xray share links from channels and groups you already joined. Everything is driven from a single GUI control panel.

## Layout

```
fetch-mtproto/
├── app.py                   # single entry point (GUI control panel)
├── setup.cmd                # Windows installer (Python, venv, deps, Xray, config)
├── setup.sh                 # Linux installer (same steps as setup.cmd)
├── config.example.yaml → config.yaml
├── requirements.txt
├── fetch_mtproto/           # application package
│   ├── gui/                 # Tkinter control panel
│   ├── cli/                 # task implementations run by the GUI
│   ├── scraper/             # Telegram scrape / watch
│   ├── mtproto/             # parse + ping
│   ├── v2ray/               # parse, Xray ping, subscription export
│   ├── catalogs.py          # open SQLite + legacy import
│   ├── db.py                # SQLite schema / access
│   ├── config_loader.py
│   └── paths.py
└── data/                    # SQLite catalog + exports (auto-created)
    ├── catalog.db           # single SQLite DB (MTProto + V2Ray)
    └── subscription.txt     # NekoRay export (working V2Ray only)
sessions/                    # Telegram session files (auto-created)
logs/                        # saved GUI logs (auto-created)
xray/                        # Xray-core binary + geo data (when not on PATH)
```

## Setup

**Windows**

```bat
setup.cmd
```

**Linux**

```bash
chmod +x setup.sh
./setup.sh
```

Both installers look on **PATH first** for Python 3.10+ and Xray, skip `pip install` when dependencies are already in `.venv`, and only download Xray into the `xray/` folder when it is not found on PATH or locally. They also create `config.yaml` when missing. On Windows, Python 3.10+ is installed via winget only when not on PATH. On Linux, install Python 3.10+ yourself (e.g. `sudo apt install python3 python3-venv python3-tk` on Debian/Ubuntu).

Then edit `config.yaml`: set `telegram.api_id` / `telegram.api_hash` from https://my.telegram.org/apps and list `telegram.sources`.

## Run

**Windows**

```bat
.venv\Scripts\pythonw.exe app.py
```

**Linux**

```bash
.venv/bin/python app.py
```

The control panel provides:

| Button | What it does |
|--------|--------------|
| Start Scraper | Connects to Telegram, scans sources, watches for new posts (login prompts answered in the input box) |
| Ping MTProto | Tests all MTProto proxies and reorganizes working/failed in the DB |
| Ping V2Ray | Tests all V2Ray servers through Xray and reorganizes the DB |
| Start Subscription server | Rebuilds `data/subscription.txt` and serves it at `http://127.0.0.1:8765/subscription.txt` for NekoRay |
| Open top N proxies | Opens the fastest working MTProto links in Telegram Desktop |

Output of every task streams into the log pane. The status bar shows working/total counts for both catalogs.

## Storage

Everything lives in `DATABASE_FILE` (default `data/catalog.db`):

| Table | Contents |
|-------|----------|
| `mtproto` | Working + failed MTProto proxies (`status`) |
| `v2ray` | Working + failed V2Ray share links (`status`, `scheme`) |
| `meta` | Migration flags |

Working V2Ray rows are exported to `SUBSCRIPTION_FILE` (default `data/subscription.txt`).

On first open, legacy text files under `data/mtproto/` and `data/v2ray/` are imported once.

## Adaptive probe scheduler

Each catalog row tracks lifetime health:

| Field | Meaning |
|-------|---------|
| `success_count` / `failure_count` | Lifetime OK / fail totals |
| `consecutive_successes` / `consecutive_failures` | Current streak |
| `check_count` | Total probes |
| `last_latency_ms` / `avg_latency_ms` | Last and EMA latency |
| `last_error` / `last_checked_at` | Last failure reason + timestamp |
| `skip_until` | Backoff deadline after repeated fails |
| `priority_score` | Explore / exploit / recover score |

Probes are ordered by **highest `priority_score` first**:

1. **Explore** — never-tested servers jump the queue.
2. **Exploit** — high success rate + low latency stay near the front.
3. **Recover** — chronic failures drop down but get sparse retries after exponential backoff (`PROBE_RESPECT_BACKOFF`).

Optional `MTPROTO_MAX_WORKING` / `V2RAY_MAX_WORKING` cap how many top-scoring servers are tested each cycle and kept in the working set (0 = unlimited).

## Behavior

1. Scraper prefers an MTProto proxy; falls back to a direct connection if none work.
2. Scans recent messages for MTProto and V2Ray share URIs.
3. Inserts new unique links into SQLite as working.
4. Stays online for new posts.
5. On `PROXY_CHECK_INTERVAL` (default 30 minutes), re-pings both catalogs.

V2Ray health checks spin up a short-lived local Xray SOCKS inbound per server and HTTP-ping `V2RAY_TEST_URL`. Schemes Xray can outbound (`vmess`, `vless`, `trojan`, `ss`) are tested; others are marked failed as unsupported.

## Fake TLS (`ee…`) proxies

Most modern MTProto links use Fake TLS secrets (starting with `ee`). Stock Telethon does not support those; this project uses `TelethonFakeTLS` plus a small ChangeCipherSpec patch so they work for both pinging and scraping.
