# Mock Tool for Realtime Transcribe Service

This tool simulates Fano Assist client behavior, sends transcript messages to Realtime Transcribe Service over WebSocket, verifies the client-triggerable scenarios in the protocol matrix, and supports concurrent load tests, Kafka message replay, and a CSV-driven Mock Live-Chat playback mode.

## Prerequisites

1. **Start the supporting infrastructure** for Realtime Transcribe Service.

Use the `transcribe_service` project's `docker-compose.yml`, or any equivalent local Redis and Kafka setup.

2. **Start Realtime Transcribe Service** from its own project:

```bash
cp .env.example .env
python -m realtime_transcribe_service.main
# Default WebSocket endpoint: ws://127.0.0.1:8080/ws/v1/realtime-transcriptions
```

The service keeps its own runtime config in its own `.env`. That file does not configure the mock tool.

3. **Install the mock project** from this repository root:

```bash
pip install -e ".[dev]"
# or
poetry install --with dev
```

4. **Optional: configure mock-tool defaults** with this repository's local env file:

```bash
cp .env.example .env
```

Supported mock-tool variables:

- `MOCK_CLIENT_HOST`
- `MOCK_CLIENT_PORT`
- `MOCK_CLIENT_LOG_LEVEL`
- `MOCK_CLIENT_LOG_FORMAT`
- `MOCK_CLIENT_DEFAULT_WS_URL`
- `AUTH_ENABLED`
- `MOCK_CLIENT_AUTH_TOKEN`
- `MOCK_CLIENT_AUTH_SIGNING_MATERIAL`
- `MOCK_CLIENT_AUTH_SUBJECT`
- `MOCK_CLIENT_AUTH_TTL_DAYS`
- `MOCK_CLIENT_DEFAULT_KAFKA_BOOTSTRAP`
- `MOCK_CLIENT_DEFAULT_KAFKA_TOPIC`

When `AUTH_ENABLED=true`, the mock tool resolves credentials in this order:

1. `MOCK_CLIENT_AUTH_TOKEN`
2. `MOCK_CLIENT_AUTH_SIGNING_MATERIAL` -> auto-generate a long-lived HS256 JWT locally

The mock tool then automatically sends:

```text
Authorization: Bearer <token>
```

The mock tool only uses its own environment variables and `.env`. It does not read the main service `.env`.

When `AUTH_ENABLED=false`, the mock tool does not generate a token and does not send `Authorization`, even if `MOCK_CLIENT_AUTH_TOKEN` or `MOCK_CLIENT_AUTH_SIGNING_MATERIAL` is configured.

If you still want the mock tool to generate one explicit token for copy/paste or external callers, run:

```bash
python -m mock_tool.generate_jwt
```

This command uses the mock tool's own auth settings:

- `AUTH_ENABLED`
- `MOCK_CLIENT_AUTH_SIGNING_MATERIAL`
- `MOCK_CLIENT_AUTH_SUBJECT`
- `MOCK_CLIENT_AUTH_TTL_DAYS`

Use it when you already configured those values in `.env` and only want the raw token printed to stdout for quick copy/paste. The generator requires `AUTH_ENABLED=true`.

Examples:

```bash
python -m mock_tool.generate_jwt --sub fano-backend --days 30
```

Generates a raw token while overriding the default `sub` claim and lifetime. Use this when Fano Labs wants a token that clearly identifies the caller as `fano-backend` or when you want a shorter or longer TTL than `MOCK_CLIENT_AUTH_TTL_DAYS`.

```bash
python -m mock_tool.generate_jwt --json
```

Prints a JSON object containing the token, decoded claims, and a ready-to-copy `authorization_header`. Use this when you want to inspect `sub`, `iat`, `exp`, and `jti` instead of only getting the opaque JWT string.

```bash
python -m mock_tool.generate_jwt --signing-material "replace-with-signing-material" --sub fano-backend
```

Ignores `.env` for the signing material and signs the token with the explicit `--signing-material` value. Use this when you do not want to persist the signing material in `.env`, or when you need to generate a token for another environment temporarily.

## Run Mock-Tool Tests

Install the mock-tool test dependencies and run its local suite:

```bash
pip install -r requirements-dev.txt
pytest
```

## Start the Mock Tool

```bash
python -m mock_tool.server
```

Then open **http://127.0.0.1:8088** in the browser.

## UI Overview

The page is split into:

- a `Scenario Tests` tab for contract-path validation
- a `Concurrent Load Test` tab for throughput and latency observation
- a `Mock Live-Chat` tab for CSV-driven conversation playback and Kafka-confirmed chat rendering

Real-time metrics such as sent count, ACK count, errors, active connections, TPS, and latency percentiles live inside the **Concurrent Load Test** card. Those counters are updated only by the load-test path, not by scenario tests. Each time you start a new load test, the dashboard resets and is repopulated from SSE `stats` events.

### 1. Control Panel

| Control | Description |
|------|------|
| WebSocket URL | The Realtime Transcribe Service endpoint, defaulting to `ws://127.0.0.1:8080/ws/v1/realtime-transcriptions` |
| Scenario test groups | **Group 1: Uses the scenario control value**: `N-01`, `N-02`, `N-03`, `E-09`. **Group 2: Fixed error scenarios**: `E-01`, `E-04`, `E-05`, `E-06`, `E-07`, `E-08`, `E-14`, `E-15` |
| Benchmark preset | Fills the `300 / 400 / 500` benchmark presets with the values used during service-side concurrency tuning |
| Scenario control value | The meaning changes by scenario; see the notes below |
| Run all | Executes `N-01 -> N-02 -> N-03 -> E-01 -> E-04 -> E-05 -> E-06 -> E-07 -> E-08 -> E-09 -> E-14 -> E-15` in order |
| Concurrent load test | Runs a normal success-path loop with multiple conversations. Each connection sends several `SESSION_ONGOING` events followed by one `SESSION_COMPLETE` |
| Concurrency / messages per connection / interval | `concurrency` is the number of simultaneously active conversations. `messages per connection` includes the final `SESSION_COMPLETE`. `interval` is the delay between messages within the same connection |
| Start / stop | Starts or stops the current load-test run |

Scenario control value meanings:

- `N-01`: number of `SESSION_ONGOING` messages to send
- `N-02`: number of sequence numbers to test, each sent twice
- `N-03`: total number of messages in one conversation, including the final `SESSION_COMPLETE`
- `E-09`: target sequence number of the out-of-order second message; the implementation uses `max(2, N)`
- Other error scenarios: the control value is ignored

### 2. Scenario Results

The **Clear** action in the panel header removes all scenario cards and restores the empty-state hint.

Each scenario run appears as a card containing:

- a single-line title with the scenario name, conversation ID, and PASS/FAIL badge
- detailed step logs showing what was sent, what came back, and whether the close code matched expectations

### 3. Kafka Messages

The Kafka panel header includes:

- visible message count
- clear list

| Control | Description |
|------|------|
| Bootstrap / Topic | Used when starting the consumer |
| `conversationId` filter | Filters the message list by conversation ID; choosing "All conversations" disables the filter |
| Start consumer / stop | Starts or stops the Kafka consumer |

Message-list behavior:

- Messages are shown in Kafka consumption order from top to bottom
- The list keeps up to 200 visible entries and discards the oldest first
- Clicking a message expands the full JSON body

### 4. Mock Live-Chat

`Mock Live-Chat` uploads one CSV, sends its rows to the WebSocket endpoint in order, and only renders transcript bubbles after the corresponding Kafka records for the same `conversationId` are consumed.

A ready-to-use sample file is included at [`src/mock_tool/samples/live_chat_sample.csv`](src/mock_tool/samples/live_chat_sample.csv).

Supported CSV semantics:

- speaker column aliases: `speaker`, `role`, `sender`, `Speaker Roles`
- transcript column aliases: `transcript`, `Transcription`, `text`, `message`, `content`
- optional delay column aliases: `delay_ms`, `pause_ms`, `wait_ms`, `typing_delay_ms`
- speaker values can be plain `Agent` / `Customer`, or spreadsheet-style values such as `SPEAKER_1;AGENT` and `SPEAKER_2;CUSTOMER`

Runtime behavior:

- only one Live-Chat session can run at a time
- after the CSV rows finish, the mock tool automatically appends one `SESSION_COMPLETE`
- Kafka consumption for Live-Chat is isolated from the normal Kafka viewer and uses `auto_offset_reset=latest`
- the typing cadence is controlled by `chars per second` plus optional jitter, unless a row-level `delay_ms` is present

## Scenario Reference

| Matrix ID | Internal name | Action | Expected result |
|------|------|------|----------|
| `N-01` | `N-01` | Send N consecutive `SESSION_ONGOING` messages | Each one receives `TRANSCRIPT_ACK`; the server keeps the connection open |
| `N-02` | `N-02` | For each `seq in [0, N)`, send one `SESSION_ONGOING` and then replay the same frame | Both the first attempt and the replay receive `TRANSCRIPT_ACK` |
| `N-03` | `N-03` | Send N total business messages including the final `SESSION_COMPLETE` EOL frame | The last frame receives `EOL_ACK`, followed by close code `1000` |
| `E-01` | `E-01` | Omit the `conversationId` query parameter during handshake | HTTP `400` + `E1003` |
| `E-04` | `E-04` | Send invalid JSON as the first frame | `ERROR(E1001)` + close `1007` |
| `E-05` | `E-05` | Send a message with `eventType=INVALID` after the connection opens | `ERROR(E1002)` + close `1008` |
| `E-06` | `E-06` | Send JSON missing required fields | `ERROR(E1003)` + close `1008` |
| `E-07` | `E-07` | Change `metaData.conversationId` to a non-string type | `ERROR(E1004)` + close `1008` |
| `E-08` | `E-08` | Change `speakTimeStamp` or `transcriptGenerateTimeStamp` to a non-UTC or invalid timestamp | `ERROR(E1005)` + close `1008` |
| `E-09` | `E-09` | Send `seq 0`, then jump to `seq=max(2, N)` | `ERROR(E1006)` + close `1008` |
| `E-14` | `E-14` | Use different `conversationId` values in the query string and body | `ERROR(E1009)` + close `1008` |
| `E-15` | `E-15` | Build a message that violates a business rule, such as `isFinal=false` | `ERROR(E1009)` + close `1008` |

> Scenarios such as `E-02`, `E-03`, `E-10`, `E-11`, `E-12`, `E-13`, and `N-04` depend on service state, connection saturation, or failure injection and therefore cannot be triggered reliably by a normal client-only flow.

## HTTP API

The mock UI also exposes HTTP endpoints:

```bash
# Run a single scenario
curl -X POST "http://127.0.0.1:8088/api/scenario/run?name=N-01&n_messages=5"

# Run the whole scenario playlist
curl -X POST "http://127.0.0.1:8088/api/scenario/run-all"

# Start load testing: 10 concurrent conversations, 10 messages each, 20ms interval
curl -X POST "http://127.0.0.1:8088/api/load/start?concurrency=10&messages_per_conv=10&interval_ms=20"

# Stop load testing
curl -X POST "http://127.0.0.1:8088/api/load/stop"

# Read status, including recent error summaries
curl "http://127.0.0.1:8088/api/status"

# Start Kafka consumption
curl -X POST "http://127.0.0.1:8088/api/kafka/start"

# Stop Kafka consumption
curl -X POST "http://127.0.0.1:8088/api/kafka/stop"

# Preview a Live-Chat CSV
curl -X POST "http://127.0.0.1:8088/api/live/preview" \
  -H "Content-Type: application/json" \
  -d "{\"csv_text\":\"speaker,transcript\nAgent,hello\",\"csv_filename\":\"chat.csv\"}"

# Start Mock Live-Chat
curl -X POST "http://127.0.0.1:8088/api/live/start" \
  -H "Content-Type: application/json" \
  -d "{\"csv_text\":\"speaker,transcript\nAgent,hello\",\"csv_filename\":\"chat.csv\",\"ws_url\":\"ws://127.0.0.1:8080/ws/v1/realtime-transcriptions\",\"kafka_bootstrap\":\"127.0.0.1:9092\",\"kafka_topic\":\"AI_STAGING_TRANSCRIPTION\",\"conversation_id\":\"livechat-demo-1\",\"chars_per_second\":18,\"pace_jitter_pct\":0.15}"

# Stop / clear / inspect Live-Chat state
curl -X POST "http://127.0.0.1:8088/api/live/stop"
curl -X POST "http://127.0.0.1:8088/api/live/clear"
curl "http://127.0.0.1:8088/api/live/status"
```

## File Layout

```text
rts-mock-tool/
├── pyproject.toml
├── src/
│   └── mock_tool/     # Runtime package
├── tests/             # Mock-tool-local tests (unit + integration)
└── README.md          # This document
```

## FAQ

**Q: Why does the normal `N-01` scenario fail to connect?**  
Make sure Realtime Transcribe Service is running and listening at `ws://127.0.0.1:8080`.

**Q: Why do I not see Kafka messages?**  
Make sure you clicked **Start Consumer** and that the Kafka container is healthy. The consumer uses `auto_offset_reset=earliest` with no group ID, so each fresh start replays the topic from the beginning.

**Q: Why is TPS low during load tests?**  
`interval_ms` controls how long the client waits between messages on the same connection. Setting it to `0` sends as fast as possible. Increase concurrency or reduce interval if you need more throughput.

**Q: Why do I get `zstd` compression errors?**  
`requirements.txt` already includes `cramjam`. If you use a custom environment, make sure it is installed.
