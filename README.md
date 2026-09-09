# MTProto + V2Ray Scraper

Fetches unique Telegram MTProto proxies and V2Ray/Xray share links from channels and groups you already joined. Everything is driven from a single GUI control panel.

## Layout

```
fetch-mtproto/
├── app.py                   # single entry point (GUI control panel)
├── setup.cmd                # Windows installer (Python, deps, Xray, config)
├── setup.sh                 # Linux installer (same steps as setup.cmd)
├── config.example.yaml → config.yaml
├── urls.txt             # HTTP(S) V2Ray list sources (GitHub raw, etc.)
├── requirements.txt
├── fetch_mtproto/           # application package
│   ├── gui/                 # Tkinter control panel
│   ├── cli/                 # task implementations run by the GUI
│   ├── scraper/             # Telegram scrape / watch
│   ├── mtproto/             # parse + ping
│   ├── v2ray/               # parse, Xray ping, subscription export, URL sources
│   ├── catalogs.py          # open SQLite + legacy import
│   ├── db.py                # SQLite schema / access
│   ├── config_loader.py
│   └── paths.py
└── data/                    # SQLite catalog + exports (auto-created)
    ├── catalog.db           # single SQLite DB (MTProto + V2Ray)
    └── subscription.txt     # NekoRay export (working V2Ray only)
sessions/                    # Telegram session files (auto-created)
logs/                        # error.log, debug.log, saved GUI logs (auto-created)
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

Both installers look on **PATH first** for Python 3.10+ and Xray, skip `pip install` when dependencies are already installed globally, and only download Xray into the `xray/` folder when it is not found on PATH or locally. They also create `config.yaml` when missing. On Windows, Python 3.10+ is installed via winget only when not on PATH. On Linux, install Python 3.10+ yourself (e.g. `sudo apt install python3 python3-pip python3-tk` on Debian/Ubuntu).

Then edit `config.yaml`: set `telegram.api_id` / `telegram.api_hash` from https://my.telegram.org/apps and list `telegram.sources`.

## Run

**Windows**

```bat
pythonw app.pyw
```

**Linux**

```bash
python3 app.pyw
```

The control panel provides:

| Button | What it does |
|--------|--------------|
| Start Scraper | Connects to Telegram, scans sources, watches for new posts (login prompts answered in the input box). Also fetches `urls.txt` on start / on interval. |
| Ping MTProto | Tests all MTProto proxies and reorganizes working/failed in the DB |
| Ping V2Ray | Tests all V2Ray servers through Xray and reorganizes the DB |
| URL sources | Fetches V2Ray lists from `urls.txt` into the catalog (loops on `url_sources.fetch_interval`) |
| Start Subscription server | Rebuilds `data/subscription.txt` and serves it on your LAN (default `http://<your-ip>:8765/subscription.txt`). A QR code appears in the panel for easy import on phones. |
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

## Proxy pool architecture

Python is the **control plane only**. Traffic stays: local client → Xray → upstream.

```
Telegram / URL ingest → SQLite inventory (up to 10,000)
        → probe scheduler (probe_due_at)
        → probe Xray (TCP prefilter then 256+ HTTP HEAD)
        → hot-set engine (~750 outbounds)
        → pool Xray (never restarted on rotate)
        → local SOCKS + HTTP slots
```

| Layer | Typical size | Meaning |
|-------|----------------|---------|
| Inventory | 5,000–10,000 | Unique share links in SQLite (`v2ray.catalog_max`) |
| Hot ring | ~750 | 300 active + 300 standby + 100 reserve loaded in the pool Xray |
| Active slots | 300 | Local listeners; one upstream node per slot |
| Subscription export | 100 | `v2ray.subscription_limit` — a query limit, not a store cap |

**Rotation:** `Rotate all` flips already-loaded balancer targets via gRPC `OverrideBalancerTarget`. Existing TCP sessions do **not** migrate. A cold VLESS/REALITY handshake is still tens–hundreds of ms. Assignment is sub-ms to low-ms after parallel RPCs — not a 0.1 ms promise.

Failed/empty slots are repaired from standby automatically. Optional `proxy_pool.diversity_rotate_sec` (default 0 = off) can shuffle healthy slots on a timer.

### Ports (defaults)

| Role | Range |
|------|--------|
| Pool SOCKS | 10801–11100 |
| Pool HTTP | 11201–11500 |
| Pool gRPC API (Handler + Routing + Stats) | 20802 |
| Probe SOCKS | 45001–45256 (grows with `ping_concurrency`) |
| Probe gRPC API | 45520 |

Keep these below Windows ephemeral ports (typically 49152–65535). Setup pins **Xray-core v26.3.27**.

Pinned control-plane gRPC: use the machine’s installed `grpcio` (this repo requires `grpcio>=1.67.1`).

## Adaptive probe scheduler

Each catalog row tracks lifetime health:

| Field | Meaning |
|-------|---------|
| `success_count` / `failure_count` | Lifetime OK / fail totals |
| `consecutive_successes` / `consecutive_failures` | Current streak |
| `check_count` | Total probes |
| `last_latency_ms` / `avg_latency_ms` | Last and EMA latency |
| `last_error` / `last_checked_at` | Last failure reason + timestamp |
| `skip_until` | Backoff deadline after repeated fails (MTProto) |
| `state` | V2Ray: `unknown` / `healthy` / `degraded` / `dead` / `quarantined` |
| `probe_due_at` | V2Ray next probe time |
| `priority_score` | Explore / exploit / recover score |

**V2Ray Ping** only probes rows whose `probe_due_at` is due:

- unknown → now
- healthy → +5–10 min (very healthy +10–15)
- degraded → +2 min
- dead → 10m / 30m / 1h / 3h exponential backoff

TCP connect is recorded separately from proxy-verified HTTP HEAD (`generate_204`). Healthy V2Ray rows are **not** demoted to failed just because they are outside the top 300.

`mtproto.max_working` still caps the MTProto working set (0 = unlimited). `v2ray.max_working` is unused. `v2ray.subscription_limit` (default 100) controls how many fastest working servers are exported to `subscription.txt` (0 = unlimited).

## Behavior

1. Scraper prefers an MTProto proxy; falls back to a direct connection if none work.
2. On start (and every `url_sources.fetch_interval`), fetches V2Ray lists from `urls.txt` into the catalog.
3. Scans recent Telegram messages for MTProto and V2Ray share URIs.
4. Inserts new unique links into SQLite.
5. Stays online for new posts.
6. On `PROXY_CHECK_INTERVAL` (default 5 minutes), re-pings MTProto and **due** V2Ray rows.

Edit `urls.txt` (one HTTP(S) URL per line) to control GitHub / subscription sources. Use the **URL sources** job in the GUI to run that fetch without Telegram, or leave it to the scraper.

V2Ray health checks: TCP prefilter, then HTTP HEAD (GET fallback) through a long-lived probe Xray. Schemes Xray can outbound (`vmess`, `vless`, `trojan`, `ss`) are tested.

## Tests and benches

```bat
python -m unittest discover -s tests -v
python scripts\bench_xray_rpc.py
python scripts\bench_probe.py
python scripts\xray_upgrade_regression.py
```

`xray_upgrade_regression.py --full` is the gate before bumping the pinned Xray version (300 inbounds, ~1000 outbounds, 100 rotations, same process).


## Fake TLS (`ee…`) proxies

Most modern MTProto links use Fake TLS secrets (starting with `ee`). Stock Telethon does not support those; this project uses `TelethonFakeTLS` plus a small ChangeCipherSpec patch so they work for both pinging and scraping.
