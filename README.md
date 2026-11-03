# Quake Alert

Post-event earthquake notifications for Greece. Real earthquake data from public
sources (EMSC, USGS) is ingested, correlated into a single canonical event,
matched against user locations, and delivered as notifications.

> This is **not** an early-warning system. Public sources publish events
> minutes after they happen.

Practice project for microservices, Kafka, Redis, Docker and Kubernetes.

## Services

| Service | Responsibility |
|---|---|
| Ingestor | Connects to EMSC (WebSocket) and USGS (polling), writes raw events to Kafka |
| Correlator | Merges reports of the same earthquake from different sources; handles revisions |
| Subscription | Users, locations, magnitude thresholds |
| Impact matcher | Finds affected users per earthquake (Redis GEO) |
| Notifier | Delivers notifications (email, Telegram) with retries and DLQ |

## Running locally

Infrastructure runs in Docker; our services run from the IDE.

```
docker compose -f infra/docker-compose.yml up -d     # start
docker compose -f infra/docker-compose.yml ps        # status
docker compose -f infra/docker-compose.yml down      # stop (data kept)
docker compose -f infra/docker-compose.yml down -v   # stop and wipe data
```

| Component | Address | Notes |
|---|---|---|
| Kafka | `localhost:9094` | Topics created by `kafka-init`, 1 partition each |
| Kafka UI | http://localhost:8091 | Browse topics and messages |
| Postgres | `localhost:5433` | user/pass `quake`/`quake`; DBs `quake_correlator`, `quake_subscription`, `quake_notifier` |
| Redis | `localhost:6380` | |
| Mailpit | http://localhost:8025 | Fake inbox; SMTP on `localhost:1025` |

Ports are shifted from the defaults so this can run alongside the ATM platform.

## Data sources

| Source | Type | Sample |
|---|---|---|
| EMSC live | WebSocket `wss://www.seismicportal.eu/standing_order/websocket` | `samples/emsc-ws-hawaii.json` |
| EMSC history | REST `https://www.seismicportal.eu/fdsnws/event/1/query` | `samples/emsc-fdsn-chalkidiki.json` |
| USGS | REST `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson` | `samples/usgs-hawaii.json`, `samples/usgs-chalkidiki.json` |

### Observations from real data (2026-10-31)

- **Same quake, Hawaii:** EMSC `20261031_0000142` = USGS `hv75066751`.
  Same agency (HV) → identical time and position. IDs have nothing in common.
- **Same quake, Chalkidiki M4.2:** EMSC `20261031_0000013` = USGS `us7000tugg`.
  Independent calculations → ~0.6 s and **~12 km** apart.
  A nearby M2.3 foreshock (`20261031_0000016`) is *closer* in distance (~7 km)
  but 5 minutes earlier → distance alone is not enough.
- EMSC `coordinates` = `[lon, lat, -depth]`; `properties.depth` is positive.
- USGS `time` is epoch milliseconds; EMSC `time` is an ISO-8601 string (UTC).
- USGS has no `action` field — create vs update must be detected from `id` + `updated`.
- USGS includes non-earthquakes (`"type": "quarry blast"`); magnitudes can be negative.
- Outside the US, USGS mostly reports only ~M4+; small Greek quakes come only from EMSC.

## Design decisions

### The `quakes.raw` event

The Ingestor translates every source format into this one shape.
Nothing after the Ingestor knows what EMSC or USGS JSON looks like.

| Field | Example | Why we need it | From EMSC | From USGS |
|---|---|---|---|---|
| `source` | `"EMSC"` | Correlator must know two reports come from different sources | constant | constant |
| `sourceEventId` | `"20261031_0000013"` | Recognise a revision of a report we already have; link back to the source | `unid` | `id` |
| `sourceUpdatedAt` | `"2026-10-31T09:55:18.113Z"` | Newer revision wins; ignore an older one arriving late | `lastupdate` | `updated` (epoch ms) |
| `eventTime` | `"2026-10-31T01:01:59.900Z"` | Correlation (same quake?) and the notification text | `time` | `time` (epoch ms) |
| `latitude` | `39.66` | Correlation and distance to users | `lat` | `coordinates[1]` |
| `longitude` | `24.19` | Same | `lon` | `coordinates[0]` |
| `depthKm` | `12.0` | Shallow quakes are felt more — impact estimate | `depth` | `coordinates[2]` |
| `magnitude` | `4.2` | Thresholds, correlation, notification text | `mag` | `mag` |
| `magnitudeType` | `"mw"` | Types differ between sources (`mw` vs `mwr`); useful when comparing | `magtype` | `magType` |
| `region` | `"AEGEAN SEA"` | Human-readable text for the notification | `flynn_region` | `place` |
| `receivedAt` | `"2026-10-31T01:05:12.000Z"` | When we got it — lets us measure delay (Grafana later) | our clock | our clock |

Rules:
- **All times are UTC, ISO-8601 strings** with milliseconds. Readable in Kafka UI,
  parsed directly by Java `Instant`. USGS epoch ms is converted by the Ingestor.
- **Latitude and longitude are separate named fields** — never an array, so the
  `[lon, lat]` order trap cannot leak past the Ingestor.
- **Depth is always positive km.**
- Dropped on purpose: `nst`, `dmin`, `rms`, `gap`, `sig`, `url`, `felt`, `cdi`, `mmi`,
  `tsunami`, `status`, `ids`, `sources`, `auth`… No service uses them. Easy to add later.

### New report vs revision
- The Ingestor does **not** decide "new" or "update". It sends every report with
  `sourceEventId` + `sourceUpdatedAt`. The Correlator decides by checking whether
  it has already seen that `source` + `sourceEventId`.
- USGS polling returns ~the same events every minute. The Ingestor remembers the last
  `updated` per USGS `id` and only sends events that are new or changed.

### Kafka key of `quakes.raw`
- Key = `source:sourceEventId` (e.g. `EMSC:20261031_0000013`) → all revisions of the
  same source report stay in order.
- Start with **1 partition**. Volume is tiny (a few events per minute), and the
  Correlator needs to see reports from *different* sources together.
  Revisit when load-testing with the Samos replay.

### Filtering
- Done in the **Ingestor**, so the rest of the system only sees relevant data.
- Rule: bounding box around Greece **with a margin** — latitude 33–43, longitude 18–36
  (includes Cyprus and western Turkey, for users near borders). The Matcher does the
  precise per-user distance check later.
- Non-earthquakes (USGS `type` ≠ `earthquake`, e.g. quarry blast) are dropped
  in the Ingestor.

### Correlator — first idea
- Two reports from different sources are the same quake if **all** hold:
  - time difference ≤ 30 s
  - distance ≤ 50 km
  - magnitude difference ≤ 0.5
- Chalkidiki check: the real match passes (0.6 s, 12 km, 0.0); the M2.3 foreshock
  fails on time (5 min) and magnitude (1.9) even though it is closer in distance.
- **Do not wait** for a second source. Publish a canonical event from the first
  report immediately; if a matching report arrives later, merge it and publish an
  update. (USGS is often minutes late, and many Greek quakes never appear there.)
- Thresholds are a first guess — to be tuned with the Samos 2020 replay.
