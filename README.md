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

> TODO (Panos): answer these before writing code.

### The `quakes.raw` event
- Fields (common format for EMSC and USGS):
- Time representation:
- Depth sign:
- Keep the original source id? Why:
- New vs revision — how detected:
- Kafka message key:

### Filtering
- Where are non-Greek quakes filtered out, and by what rule:
- Non-earthquake events (quarry blasts):

### Correlator — first idea
- Same-quake thresholds (time / distance / magnitude):
- How long to wait for a second source:
