from __future__ import annotations
from dataclasses import dataclass
from enum import StrEnum
from pydantic import BaseModel, Field, StrictBool
class ProviderKind(StrEnum):
    FAKE="fake"; OPENAI="openai"; AZURE_OPENAI="azure_openai"; ANTHROPIC="anthropic"; OPENAI_COMPATIBLE="openai_compatible"
class ProviderUnavailable(RuntimeError): pass
@dataclass(frozen=True)
class ModelRequest: capability: str; facts: dict[str, object]
@dataclass(frozen=True)
class ModelResponse: content: dict[str, object]
class SynthesizerOutput(BaseModel):
    recommendation: str = Field(pattern=r"^(ROLLBACK_DEPLOYMENT|DIAGNOSTICS_ONLY)$")
    evidence_ids: list[str] = Field(min_length=1)
class CriticOutput(BaseModel):
    accepted: StrictBool
    reason: str = Field(min_length=1, max_length=500)
class FakeModel:
    def invoke(self, request: ModelRequest) -> ModelResponse:
        if request.capability=="synthesize":
            evidence_ids=list(request.facts["evidence_ids"])
            recommendation="ROLLBACK_DEPLOYMENT" if float(request.facts["error_rate_percent"])>=15 else "DIAGNOSTICS_ONLY"
            return ModelResponse({"recommendation":recommendation,"evidence_ids":evidence_ids})
        if request.capability=="critic": return ModelResponse({"accepted":True,"reason":"evidence coverage sufficient"})
        raise ProviderUnavailable(f"unsupported fake capability: {request.capability}")
def select_provider(kind: ProviderKind, approved: set[ProviderKind]) -> FakeModel:
    if kind not in approved or kind is not ProviderKind.FAKE: raise ProviderUnavailable("selected provider is unavailable or not approved")
    return FakeModel()
