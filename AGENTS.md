# keep-alive for agents

keep-alive is a Go binary that pings every URL in `urls.txt` on an interval and serves a
report on `-listen` (production: `http://10.0.0.61:8787` on the Pi 5, `pi5.local`).
Everything an agent needs is plain HTTP, no auth, no JSON body required.

## Endpoints

| Method | Path | Returns | Use |
|---|---|---|---|
| GET | `/` | HTML, or text when `Accept` has no `text/html` | `curl http://10.0.0.61:8787/` for the plain report |
| GET | `/health` | `ok 73/73 at <UTC ts>` or 503 | stale if no round in 3 intervals, except during quiet hours |
| GET | `/status.json` | last round: `{ts,total,ok,failed,took,results:[{url,status,ms,err}]}` | which apps are down right now |
| GET | `/stats.json` | all-time totals per URL | uptime history |
| GET | `/pings?url=<url>` | `{url,pings:[{ts,slot,status,ms,ok,slow}]}` | one app, today |
| GET | `/settings` | `{"interval":"1h","quiet":"23-6","paused":["<url>", ...]}` | read current runtime settings |
| POST | `/settings` | `every 1h, quiet 23-6, 2 paused` (200) or an error line (400) | change settings, see below |

## POST /settings payloads

Form-encoded (`application/x-www-form-urlencoded`). Two shapes, 1 per request.

Pause or resume 1 app. `off=1` pauses, `off=0` resumes. The URL must match a line in
`urls.txt` exactly. Paused apps stay in the grid, dimmed, and are skipped every round.

```bash
curl -s -X POST http://10.0.0.61:8787/settings -d 'url=https://3pi-bheng.vercel.app&off=1'
curl -s -X POST http://10.0.0.61:8787/settings -d 'url=https://3pi-bheng.vercel.app&off=0'
```

Set the schedule. Both fields are sent together. `interval` is a Go duration from `1m`
to `24h` (`30m`, `1h`, `90m`, `2h30m`). `quiet` is `START-END` in 24h Eastern hours, or
`off`. A change takes effect at once: the sleeping pinger re-computes its next round.

```bash
curl -s -X POST http://10.0.0.61:8787/settings -d 'interval=1h&quiet=23-6'
curl -s -X POST http://10.0.0.61:8787/settings -d 'interval=30m&quiet=off'
```

Errors come back as 400 with 1 line: `interval: 1m to 24h, e.g. 1h` or
`quiet: use START-END in 24h hours, e.g. 23-6, or off`.

## Where settings live

`settings.json` next to the binary (flag `-settings`), rewritten on every change and
read at start, so a restart keeps them. The file overrides the `-interval` flag. Editing
the file by hand needs a restart; the endpoint does not.

```json
{ "interval": "1h", "quiet": "23-6", "paused": ["https://3pi-bheng.vercel.app"] }
```

Quiet hours mean no round at all between START and END Eastern time; `/health` stays ok
during that window. Defaults when the file is missing: the `-interval` flag, quiet `23-6`,
nothing paused.

## Adding or removing apps

Edit `urls.txt` (1 URL per line, `#` comments), re-read every round, no restart. Pausing
is for apps you keep listed but do not want pinged; removing the line drops the app from
the grid too.

## Deploy to the Pi

No Go on the Pi. Cross-compile, copy, restart the admin user's systemd unit:

```bash
make pi
scp keep-alive-linux-arm64 pi5:~/keep-alive/keep-alive.new
ssh pi5 'cd ~/keep-alive && mv keep-alive keep-alive.prev && mv keep-alive.new keep-alive && systemctl --user restart keep-alive && sleep 3 && curl -s localhost:8787/health'
```

A restart runs 1 round immediately, then follows the interval. `keep-alive.prev` is the
rollback: swap it back and restart.
