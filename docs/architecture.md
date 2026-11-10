# Architecture

```mermaid
flowchart TD
  Input[Validated Incident] --> Triage
  Triage -->|Low| NoAction[NO_ACTION]
  Triage -->|High| Diagnostics --> Impact --> Synthesizer --> Critic
  Critic -->|Accepted| Policy
  Critic -->|One revision| Synthesizer
  Critic -->|Retry exhausted| Abstain[ABSTAIN]
  Policy -->|Deny| Denied[DENIED]
  Policy -->|Approval required| Approval[LangGraph interrupt]
  Approval -->|Approved| Enforcer[Independent tool enforcement]
  Approval -->|Rejected| Rejected[APPROVAL_REJECTED]
  Enforcer --> Effect[(Atomic SQLite effect and idempotency ledger)]
  Effect --> Final[Safe final response and audit]
  NoAction --> Final
  Abstain --> Final
  Denied --> Final
  Rejected --> Final
```

LangGraph persists graph checkpoints in SQLite by workflow thread. Audit records and effect records are tenant-scoped in the same local SQLite database. Tool effect creation and its idempotency evidence are transactionally committed together. A process failure after this commit is reconciled by querying the effect ledger; replay cannot create a second effect for the same deterministic key.
