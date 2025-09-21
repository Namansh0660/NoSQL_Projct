from fastapi import APIRouter, Query, HTTPException
from typing import List, Optional
from db import KG_EDGES
from models import EdgeModel

router = APIRouter(prefix="/edges", tags=["edges"])

# -----------------------------
# Get edges connected to a node
# -----------------------------
@router.get("/", response_model=List[EdgeModel])
def get_edges(
    node_id: Optional[str] = Query(None, description="Filter edges by connected node ID"),
    relation: Optional[str] = Query(None, description="Filter edges by relation type"),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500)
):
    query = {}
    if node_id:
        query["$or"] = [{"source": node_id}, {"target": node_id}]
    if relation:
        query["relation"] = relation
    edges = list(KG_EDGES.find(query).skip(skip).limit(limit))
    return edges

