# keep-alive

Headless HTTP pinger. Hits every URL in `urls.txt` every 5 minutes with 16 parallel
workers so serverless apps (Vercel, Lambda, Cloud Run) never go cold. 1 static binary,
no dependencies, about 3 seconds per round for 70+ URLs.

Each round appends `TS  STATUS  URL  MS` lines to `keep-alive.log` (trimmed to the newest
25,000 lines), rewrites `status.json` with the full result and `stats.json` with all-time
totals per URL (survives restarts). A 3xx or 4xx from the app is counted as alive: the
function ran. Only no answer, 5xx, or 404 (no deployment) fail.

## Run it anywhere

```
make run            # 1 round, prints a summary and any failures
./keep-alive        # loop forever, 5 minute interval
./keep-alive -h     # flags: -urls -workers -interval -timeout -retries -log -status -stats -keep -listen -once
```

## All-time report

The pinger never stops for any of these; they only read what it wrote.

```
make tui                          # live terminal dashboard, uptime %, avg ms, last 40 rounds per app
make report                       # same table once, to stdout
./keep-alive -listen :8787        # also serve it: curl localhost:8787 (text), browser (html),
                                  # /health (503 if no round in 3 intervals), /stats.json, /status.json
```

## Raspberry Pi 5

```
sudo apt install -y git golang
git clone https://github.com/bunlongheng/keep-alive.git
cd keep-alive
make install        # builds, installs the systemd unit, starts it
```

Check on it:

```
systemctl status keep-alive
journalctl -u keep-alive -f
curl localhost:8787/health
./keep-alive -tui
```

Change the list by editing `urls.txt`; no restart needed, it is re-read every round.
`make uninstall` removes the service. `make pi` cross-compiles an arm64 binary from a Mac
if you would rather not install Go on the Pi.
