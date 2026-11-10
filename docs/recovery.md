# Recovery

The graph interrupts before a high-impact action. A restart uses the same persisted workflow thread. Duplicate resumes revalidate approval and observe the durable effect ledger. The effect record and its idempotency record are atomically persisted. If execution fails after that commit and before graph progress, a resumed graph observes the existing effect and completes without a duplicate rollback.
