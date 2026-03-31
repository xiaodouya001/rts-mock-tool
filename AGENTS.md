# Realtime Transcribe Service Agent Rules

## Contract First

- If the UI, tests, and implementation conflict with the API contract, `docs/design/api-contract.md` is the source of truth.
- All other documents, mock tools, scenario matrices, and tests must follow the contract and must not reinterpret contract semantics.
- If the contract must change, explicitly declare it as a contract change first, then update the related docs, tests, and implementation together.

## Change Constraints

- Before changing code, identify which design invariants are being touched.
- Any change that affects error codes, close codes, idempotency or retry semantics, TTL timing, shutdown order, or handshake validation must add matching tests.
- Do not optimize for coverage alone; protect scenario-level guardrails and the contract-level matrix first.

## Documentation Constraints

- `docs/design/design-guardrails.md` is the long-term maintenance baseline.
- `docs/design/protocol-scenario-matrix.md` is the single entry point for protocol-scenario documentation.
- If implementation and documentation diverge, update the documentation first or explicitly declare that the contract has changed.

