from __future__ import annotations
from .contracts import IncidentInput

class DiagnosticTools:
    """Fixed, local read-only diagnostic tools. No arbitrary tool dispatch exists in V1."""
    def service_health(self, incident: IncidentInput) -> dict[str, object]:
        return {"evidence_id":f"telemetry:{incident.incident_id}","error_rate_percent":incident.telemetry.error_rate_percent,"latency_ms":incident.telemetry.latency_ms}
    def deployment_metadata(self, incident: IncidentInput) -> dict[str, object]:
        return {"evidence_id":f"deployment:{incident.deployment_id}","rollback_eligible":not incident.deployment_id.endswith("-blocked")}
