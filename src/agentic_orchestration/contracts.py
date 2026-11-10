from __future__ import annotations

from datetime import UTC, datetime, timedelta
from enum import StrEnum
from hashlib import sha256
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

class Outcome(StrEnum):
    NO_ACTION="NO_ACTION"; DIAGNOSTICS_COMPLETE="DIAGNOSTICS_COMPLETE"; DENIED="DENIED"; APPROVAL_REJECTED="APPROVAL_REJECTED"; ACTION_EXECUTED="ACTION_EXECUTED"; ABSTAIN="ABSTAIN"; FAILED_SAFE="FAILED_SAFE"
class PolicyDecision(StrEnum):
    ALLOW="ALLOW"; DENY="DENY"; REQUIRE_HUMAN_APPROVAL="REQUIRE_HUMAN_APPROVAL"
class Telemetry(BaseModel):
    error_rate_percent: float = Field(ge=0, le=100)
    latency_ms: int = Field(ge=0, le=120000)
class IncidentInput(BaseModel):
    model_config=ConfigDict(extra="forbid")
    tenant_id: str=Field(pattern=r"^tenant-[a-z0-9-]+$")
    workflow_id: str=Field(pattern=r"^wf-[a-z0-9-]+$")
    incident_id: str=Field(pattern=r"^INC-[0-9]{4}$")
    incident_type: Literal["api_error_rate"]
    occurred_at: datetime
    telemetry: Telemetry
    untrusted_description: str=Field(max_length=4000)
    deployment_id: str=Field(pattern=r"^deploy-[a-z0-9-]+$")
    @field_validator("occurred_at")
    @classmethod
    def aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None: raise ValueError("occurred_at must be timezone aware")
        return value.astimezone(UTC)
class TrustedCallerContext(BaseModel):
    caller_id: str; tenant_id: str; permitted_approvers: frozenset[str]
class ProposedAction(BaseModel):
    action_id: str="act-rollback-v1"; action_type: Literal["ROLLBACK_DEPLOYMENT"]; target_deployment_id: str; parameters: dict[str, Any]; risk_class: Literal["HIGH"]="HIGH"
    @property
    def parameter_hash(self) -> str: return sha256(repr(sorted(self.parameters.items())).encode()).hexdigest()
class ApprovalDecision(BaseModel):
    tenant_id: str; workflow_id: str; incident_id: str; action_id: str; parameter_hash: str; approver_id: str; decision: Literal["APPROVE","REJECT"]; decided_at: datetime; expires_at: datetime
    def active(self, now: datetime) -> bool: return self.decided_at <= now <= self.expires_at
class FinalResponse(BaseModel):
    outcome: Outcome; correlation_id: str; policy_decision: PolicyDecision|None=None; approval_status: str|None=None; evidence_ids: list[str]=[]; safe_error_category: str|None=None
def valid_approval_for(approval: ApprovalDecision, incident: IncidentInput, action: ProposedAction, trusted: TrustedCallerContext, now: datetime|None=None) -> bool:
    return all((approval.tenant_id==trusted.tenant_id==incident.tenant_id, approval.workflow_id==incident.workflow_id, approval.incident_id==incident.incident_id, approval.action_id==action.action_id, approval.parameter_hash==action.parameter_hash, approval.approver_id in trusted.permitted_approvers, approval.decision=="APPROVE", approval.active(now or datetime.now(UTC))))
def make_approval(incident: IncidentInput, action: ProposedAction, approver_id: str) -> ApprovalDecision:
    now=datetime.now(UTC)
    return ApprovalDecision(tenant_id=incident.tenant_id,workflow_id=incident.workflow_id,incident_id=incident.incident_id,action_id=action.action_id,parameter_hash=action.parameter_hash,approver_id=approver_id,decision="APPROVE",decided_at=now,expires_at=now+timedelta(minutes=15))
