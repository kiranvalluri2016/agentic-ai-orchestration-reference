# Governance

Agents investigate and recommend. Deterministic policy decides `ALLOW`, `DENY`, or `REQUIRE_HUMAN_APPROVAL`; Tool Enforcement independently validates allowlisting, trusted tenant context, bound approval, parameters, and idempotency before an effect.

Input and diagnostic text are untrusted. Logging records identifiers, routing, policy, approval and safe event summaries, but excludes secrets, raw prompts, approval payloads, and raw untrusted diagnostic data. These are OWASP-aligned controls, not an OWASP certification.
