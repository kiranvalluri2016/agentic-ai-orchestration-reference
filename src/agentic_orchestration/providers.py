from __future__ import annotations
from dataclasses import dataclass
from enum import StrEnum
class ProviderKind(StrEnum):
    FAKE="fake"; OPENAI="openai"; AZURE_OPENAI="azure_openai"; ANTHROPIC="anthropic"; OPENAI_COMPATIBLE="openai_compatible"
class ProviderUnavailable(RuntimeError): pass
@dataclass(frozen=True)
class ModelRequest: capability: str; facts: dict[str, object]
@dataclass(frozen=True)
class ModelResponse: content: dict[str, object]
class FakeModel:
    def invoke(self, request: ModelRequest) -> ModelResponse:
        if request.capability=="synthesize": return ModelResponse({"recommendation":"ROLLBACK_DEPLOYMENT","evidence_ids":["telemetry:INC-1001","deployment:deploy-2026-001"]})
        if request.capability=="critic": return ModelResponse({"accepted":True,"reason":"evidence coverage sufficient"})
        raise ProviderUnavailable(f"unsupported fake capability: {request.capability}")
def select_provider(kind: ProviderKind, approved: set[ProviderKind]) -> FakeModel:
    if kind not in approved or kind is not ProviderKind.FAKE: raise ProviderUnavailable("selected provider is unavailable or not approved")
    return FakeModel()
