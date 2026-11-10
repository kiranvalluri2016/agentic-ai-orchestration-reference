# Tool Contracts

Only three tools are represented: read-only synthetic service health, read-only synthetic deployment metadata, and `simulate_rollback_deployment`. The remediation tool has no external side effect. It accepts only a validated deployment identifier and is invoked only after deterministic policy plus a trusted, bound approval.

Its idempotency key is derived from tenant, workflow, action, canonical parameter hash, and target deployment. SQLite records the synthetic effect and the idempotency evidence in one transaction.
