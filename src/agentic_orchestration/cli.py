from __future__ import annotations
import argparse, json
from pathlib import Path
from .contracts import ApprovalDecision, IncidentInput, TrustedCallerContext
from .graph import IncidentWorkflow
def trusted() -> TrustedCallerContext:
    return TrustedCallerContext(caller_id="svc-local",tenant_id="tenant-acme",permitted_approvers=frozenset({"approver-ops"}))
def main() -> None:
    p=argparse.ArgumentParser(); p.add_argument("--db",default=".local/agentic.sqlite"); s=p.add_subparsers(dest="command",required=True)
    s.add_parser("validate-config"); run=s.add_parser("run"); run.add_argument("incident_json"); resume=s.add_parser("resume"); resume.add_argument("approval_json"); inspect=s.add_parser("inspect"); inspect.add_argument("workflow_id")
    args=p.parse_args(); db=Path(args.db); db.parent.mkdir(parents=True,exist_ok=True); flow=IncidentWorkflow(db,trusted())
    if args.command=="validate-config": print(json.dumps({"status":"VALID","provider":"fake","external_credentials_required":False})); return
    if args.command=="run":
        state=flow.run(IncidentInput.model_validate_json(Path(args.incident_json).read_text()))
        # Do not emit raw incident text or checkpoint internals in a CLI response.
        print(json.dumps({"outcome":state.get("outcome","AWAITING_APPROVAL"),"correlation_id":state.get("correlation_id"),"policy":state.get("policy"),"evidence_ids":state.get("evidence_ids",[]),"interrupted":"__interrupt__" in state},default=str)); return
    if args.command=="resume":
        approval=ApprovalDecision.model_validate_json(Path(args.approval_json).read_text()); state=flow.resume(approval.workflow_id,approval)
        print(json.dumps({"outcome":state.get("outcome"),"correlation_id":state.get("correlation_id"),"policy":state.get("policy"),"approval_status":state.get("approval_status"),"evidence_ids":state.get("evidence_ids",[])},default=str)); return
    if args.command=="inspect": print(json.dumps({"audit":flow.audit.events("tenant-acme",args.workflow_id),"effects":flow.audit.effects("tenant-acme",args.workflow_id)},default=str)); return
if __name__=="__main__": main()
