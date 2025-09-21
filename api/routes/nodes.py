from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from db import KG_NODES
from models import NodeModel

router = APIRouter(prefix="/nodes", tags=["nodes"])

@router.get("/", response_model=List[NodeModel])
def list_nodes(
    node_type: Optional[str] = Query(None, description="Filter by node type"),
    skip: int = Query(0, ge=0, description="Number of nodes to skip"),
    limit: int = Query(50, ge=1, le=500, description="Maximum nodes to return")
):
    query = {}
    if node_type:
        query["type"] = node_type
    nodes = list(KG_NODES.find(query).skip(skip).limit(limit))
    return nodes
