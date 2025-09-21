# api/routes/search.py
from fastapi import APIRouter, Query
from typing import List, Optional
from db import KG_NODES

router = APIRouter()

@router.get("/search/papers/")
async def search_papers(
    title: Optional[str] = None,
    author: Optional[str] = None,
    doi: Optional[str] = None,
    skip: int = 0,
    limit: int = 50
):
    query = {"type": "Paper"}
    if title:
        query["properties.title"] = {"$regex": title, "$options": "i"}
    if author:
        query["properties.authors"] = {"$elemMatch": {"$regex": author, "$options": "i"}}
    if doi:
        query["properties.doi"] = doi

    papers = list(KG_NODES.find(query).skip(skip).limit(limit))
    return papers
# api/routes/search.py

@router.get("/search/nodes/")
async def search_nodes(
    name: Optional[str] = None,
    node_type: Optional[str] = None,
    skip: int = 0,
    limit: int = 50
):
    query = {}
    if node_type:
        query["type"] = node_type
    if name:
        query["properties.name"] = {"$regex": name, "$options": "i"}
    nodes = list(KG_NODES.find(query).skip(skip).limit(limit))
    return nodes
