from __future__ import annotations

import json
import logging
import sqlite3
from hashlib import sha256
from pathlib import Path
from typing import Annotated, Literal, TypedDict
from uuid import uuid4

from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt

from .contracts import ApprovalDecision, IncidentInput, Outcome, PolicyDecision, ProposedAction, TrustedCallerContext, valid_approval_for
from .persistence import AuditStore
from .providers import CriticOutput, FakeModel, ModelRequest, SynthesizerOutput
from .tools import DiagnosticTools

LOG=logging.getLogger(__name__)

class WorkflowState(TypedDict, total=False):
    incident: dict
    correlation_id: str
    evidence_ids: list[str]
    severity: str
    recommendation: str
    action: dict
    critic_attempts: int
    critic_accepted: bool
    policy: str
    outcome: str
    approval_status: str
    approval: dict
    failure_point: str

def _incident(state: WorkflowState) -> IncidentInput: return IncidentInput.model_validate(state["incident"])

class IncidentWorkflow:
    def __init__(self, db_path: str | Path, trusted: TrustedCallerContext, model: FakeModel | None=None):
        self.db_path=str(db_path); self.trusted=trusted; self.model=model or FakeModel(); self.tools=DiagnosticTools(); self.audit=AuditStore(db_path)
        connection=sqlite3.connect(self.db_path, check_same_thread=False)
        self.checkpointer=SqliteSaver(connection)
        self.graph=self._build()
    def _event(self, state: WorkflowState, name: str, detail: dict[str, object]) -> None:
        incident=_incident(state); correlation_id=state["correlation_id"]
        self.audit.audit(incident.tenant_id,incident.workflow_id,correlation_id,name,detail)
        LOG.info("workflow_event=%s",json.dumps({"event":name,"tenant_id":incident.tenant_id,"workflow_id":incident.workflow_id,"correlation_id":correlation_id,"detail":detail},sort_keys=True))
    def intake(self, state: WorkflowState) -> WorkflowState:
        incident=_incident(state)
        if incident.tenant_id != self.trusted.tenant_id:
            return {"correlation_id":state.get("correlation_id",str(uuid4())),"outcome":Outcome.DENIED.value}
        return {"correlation_id":state.get("correlation_id",str(uuid4())),"critic_attempts":0}
    def triage(self, state: WorkflowState) -> WorkflowState:
        incident=_incident(state); high=incident.telemetry.error_rate_percent>=10 or incident.telemetry.latency_ms>=2000
        self._event(state,"TRIAGE",{"severity":"HIGH" if high else "LOW"})
        return {"severity":"HIGH" if high else "LOW"}
    def diagnostics(self, state: WorkflowState) -> WorkflowState:
        incident=_incident(state); results=[self.tools.service_health(incident),self.tools.deployment_metadata(incident)]; evidence=[str(result["evidence_id"]) for result in results]
        self._event(state,"DIAGNOSTICS",{"evidence_count":len(evidence)})
        return {"evidence_ids":evidence}
    def impact(self, state: WorkflowState) -> WorkflowState:
        self._event(state,"IMPACT_ANALYZED",{"customer_impact":"MATERIAL"}); return {}
    def synthesize(self, state: WorkflowState) -> WorkflowState:
        incident=_incident(state)
        try: output=SynthesizerOutput.model_validate(self.model.invoke(ModelRequest("synthesize",{"evidence_ids":state["evidence_ids"],"error_rate_percent":incident.telemetry.error_rate_percent})).content)
        except Exception:
            self._event(state,"SYNTHESIS_ABSTAIN",{"reason":"unsupported_or_malformed_recommendation"})
            return {"outcome":Outcome.ABSTAIN.value}
        if set(output.evidence_ids) != set(state["evidence_ids"]):
            self._event(state,"SYNTHESIS_ABSTAIN",{"reason":"evidence_mismatch"}); return {"outcome":Outcome.ABSTAIN.value}
        if output.recommendation=="DIAGNOSTICS_ONLY": return {"recommendation":output.recommendation,"outcome":Outcome.DIAGNOSTICS_COMPLETE.value}
        action=ProposedAction(action_type="ROLLBACK_DEPLOYMENT",target_deployment_id=incident.deployment_id,parameters={"deployment_id":incident.deployment_id})
        return {"recommendation":output.recommendation,"action":action.model_dump()}
    def critic(self, state: WorkflowState) -> WorkflowState:
        try: output=CriticOutput.model_validate(self.model.invoke(ModelRequest("critic",{"recommendation":state["recommendation"],"evidence_ids":state["evidence_ids"]})).content)
        except Exception: return {"outcome":Outcome.ABSTAIN.value,"critic_attempts":state.get("critic_attempts",0)+1}
        return {"critic_accepted":output.accepted,"critic_attempts":state.get("critic_attempts",0)+1}
    def policy(self, state: WorkflowState) -> WorkflowState:
        action=ProposedAction.model_validate(state["action"])
        if action.target_deployment_id.endswith("-blocked"):
            self._event(state,"POLICY_DECISION",{"decision":PolicyDecision.DENY.value}); return {"policy":PolicyDecision.DENY.value,"outcome":Outcome.DENIED.value}
        self._event(state,"POLICY_DECISION",{"decision":PolicyDecision.REQUIRE_HUMAN_APPROVAL.value}); return {"policy":PolicyDecision.REQUIRE_HUMAN_APPROVAL.value}
    def approval(self, state: WorkflowState) -> WorkflowState:
        incident=_incident(state); action=ProposedAction.model_validate(state["action"])
        payload=interrupt({"kind":"APPROVAL_REQUIRED","tenant_id":incident.tenant_id,"workflow_id":incident.workflow_id,"incident_id":incident.incident_id,"action_id":action.action_id,"parameter_hash":action.parameter_hash})
        approval=ApprovalDecision.model_validate(payload)
        if not valid_approval_for(approval,incident,action,self.trusted):
            self._event(state,"APPROVAL_REJECTED",{"reason":"invalid_or_mismatched"}); return {"outcome":Outcome.APPROVAL_REJECTED.value,"approval_status":"REJECTED"}
        self._event(state,"APPROVAL_GRANTED",{"approver_id":approval.approver_id}); return {"approval_status":"APPROVED","approval":approval.model_dump(mode="json")}
    def execute(self, state: WorkflowState) -> WorkflowState:
        incident=_incident(state); action=ProposedAction.model_validate(state["action"])
        if state.get("policy") != PolicyDecision.REQUIRE_HUMAN_APPROVAL.value or state.get("approval_status") != "APPROVED": return {"outcome":Outcome.DENIED.value}
        try: approval=ApprovalDecision.model_validate(state["approval"])
        except Exception: return {"outcome":Outcome.DENIED.value}
        if not valid_approval_for(approval,incident,action,self.trusted):
            self._event(state,"TOOL_ENFORCEMENT_DENIED",{"reason":"approval_revalidation_failed"}); return {"outcome":Outcome.DENIED.value}
        key=sha256(f"{incident.tenant_id}|{incident.workflow_id}|{action.action_id}|{action.parameter_hash}|{action.target_deployment_id}".encode()).hexdigest()
        if state.get("failure_point")=="before_effect_transaction":
            raise RuntimeError("injected failure before atomic effect transaction")
        result,created=self.audit.execute_effect(key=key,tenant_id=incident.tenant_id,workflow_id=incident.workflow_id,action_id=action.action_id,correlation_id=state["correlation_id"])
        if state.get("failure_point")=="after_effect_commit" and created:
            raise RuntimeError("injected failure after atomic effect commit")
        self._event(state,"TOOL_EXECUTION",{"created":created,"effect_id":result["effect_id"]})
        return {"outcome":Outcome.ACTION_EXECUTED.value}
    def no_action(self, state: WorkflowState) -> WorkflowState: return {"outcome":Outcome.NO_ACTION.value}
    def abstain(self, state: WorkflowState) -> WorkflowState: return {"outcome":Outcome.ABSTAIN.value}
    def finalize(self, state: WorkflowState) -> WorkflowState:
        self._event(state,"FINALIZED",{"outcome":state.get("outcome",Outcome.FAILED_SAFE.value)}); return {}
    @staticmethod
    def after_intake(state: WorkflowState) -> Literal["triage","finalize"]:
        return "finalize" if state.get("outcome")==Outcome.DENIED.value else "triage"
    @staticmethod
    def after_triage(state: WorkflowState) -> Literal["diagnostics","no_action"]: return "diagnostics" if state.get("severity")=="HIGH" else "no_action"
    @staticmethod
    def after_synthesize(state: WorkflowState) -> Literal["critic","abstain","finalize"]:
        return "abstain" if state.get("outcome")==Outcome.ABSTAIN.value else "finalize" if state.get("outcome")==Outcome.DIAGNOSTICS_COMPLETE.value else "critic"
    @staticmethod
    def after_critic(state: WorkflowState) -> Literal["policy","synthesize","abstain"]:
        if state.get("critic_accepted"): return "policy"
        return "synthesize" if state.get("critic_attempts",0)<2 else "abstain"
    @staticmethod
    def after_approval(state: WorkflowState) -> Literal["execute","finalize"]: return "execute" if state.get("approval_status")=="APPROVED" else "finalize"
    @staticmethod
    def after_policy(state: WorkflowState) -> Literal["approval","finalize"]: return "approval" if state.get("policy")==PolicyDecision.REQUIRE_HUMAN_APPROVAL.value else "finalize"
    def _build(self):
        g=StateGraph(WorkflowState)
        for name,node in (("intake",self.intake),("triage",self.triage),("diagnostics",self.diagnostics),("impact",self.impact),("synthesize",self.synthesize),("critic",self.critic),("policy",self.policy),("approval",self.approval),("execute",self.execute),("no_action",self.no_action),("abstain",self.abstain),("finalize",self.finalize)): g.add_node(name,node)
        g.add_edge(START,"intake");g.add_conditional_edges("intake",self.after_intake);g.add_conditional_edges("triage",self.after_triage);g.add_edge("diagnostics","impact");g.add_edge("impact","synthesize");g.add_conditional_edges("synthesize",self.after_synthesize);g.add_conditional_edges("critic",self.after_critic);g.add_conditional_edges("policy",self.after_policy);g.add_conditional_edges("approval",self.after_approval);g.add_edge("execute","finalize");g.add_edge("no_action","finalize");g.add_edge("abstain","finalize");g.add_edge("finalize",END)
        return g.compile(checkpointer=self.checkpointer)
    def run(self, incident: IncidentInput, *, failure_point: str|None=None):
        return self.graph.invoke({"incident":incident.model_dump(mode="json"),"failure_point":failure_point}, self._config(incident.workflow_id))
    def resume(self, workflow_id: str, approval: ApprovalDecision):
        return self.graph.invoke(Command(resume=approval.model_dump(mode="json")), self._config(workflow_id))
    def _config(self, workflow_id: str) -> dict:
        return {"configurable":{"thread_id":f"{self.trusted.tenant_id}:{workflow_id}"}}
