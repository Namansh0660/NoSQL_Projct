from typing import List, Optional
from pydantic import BaseModel

# -----------------------------
# Node model
# -----------------------------
class NodeModel(BaseModel):
    id: str
    type: str
    properties: dict

# -----------------------------
# Edge model
# -----------------------------
class EdgeModel(BaseModel):
    source: str
    target: str
    relation: str
    properties: Optional[dict] = {}
