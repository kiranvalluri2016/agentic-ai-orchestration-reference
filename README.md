# Agentic AI Orchestration Reference

This is a local-only architecture reference POC, not a production deployment. It demonstrates governed LangGraph incident investigation: specialist analysis, synthesis, critique, deterministic policy, human approval, SQLite checkpoint/resume, and idempotent synthetic tool effects.

## Verified local baseline

Python 3.12.11, LangGraph 1.2.20, LangGraph SQLite checkpoint 3.1.3, Pydantic 2.14.3, and pytest 8.4.2. The verified implementation uses no paid LLM, cloud account, external diagnostic system, or credentials.

## Quick start

```bash
uv sync --python /path/to/python3.12 --group dev
uv run pytest -q
uv run agentic-orchestration validate-config
uv run agentic-orchestration run sample_data/incidents/inc-1001.json
uv run agentic-orchestration inspect wf-inc-1001
```

`INC-1001` pauses for a bound approval before the synthetic rollback. The fake model is the only locally verified provider. OpenAI, Azure OpenAI, Anthropic, and OpenAI-compatible endpoints are provider-contract extension points only; no live integration is claimed.

## Governance boundaries

- Untrusted incident text is data, never policy or tool authority.
- Policy and tool enforcement, not agents, decide authorization.
- High-impact actions require trusted synthetic approver context plus a tenant/workflow/action/parameter-bound approval.
- Synthetic effects and durable idempotency records share one SQLite transaction.
- No RAG, vector store, cloud provisioning, real rollback, or GitHub Actions workflow is included.

See [architecture](docs/architecture.md), [governance](docs/governance.md), and [recovery design](docs/recovery.md).

## Local verification

The current local acceptance suite completed with `12 passed in 0.37s` using `uv run pytest -q`. This evidence covers checkpoint restart, approval binding and rejection, atomic-effect recovery, duplicate resume, bounded Critic abstention, provider fail-closed behavior, tenant-scoped audit reads, and safe logging. It does not certify live-provider, cloud, or production operation.
