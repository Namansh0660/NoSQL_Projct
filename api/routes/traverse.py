from fastapi import APIRouter, HTTPException, Query
from typing import List
from db import KG_NODES, KG_EDGES

router = APIRouter(prefix="/traverse", tags=["traverse"])

@router.get("/author_papers")
def get_author_papers(author_id: str):
    """
    Get all papers authored by an author, including co-authors and institutions.
    """
    # Validate author exists
    author_node = KG_NODES.find_one({"id": author_id, "type": "Author"})
    if not author_node:
        raise HTTPException(status_code=404, detail="Author not found")

    # Find all papers authored by this author
    paper_edges = list(KG_EDGES.find({"relation": "authored_by", "target": author_id}))
    papers = [KG_NODES.find_one({"id": e["source"]}) for e in paper_edges]

    # Co-authors for each paper
    results = []
    for paper in papers:
        co_edges = list(KG_EDGES.find({"relation": "authored_by", "source": paper["id"], "target": {"$ne": author_id}}))
        co_authors = [KG_NODES.find_one({"id": e["target"]}) for e in co_edges]
        results.append({
            "paper": paper,
            "co_authors": co_authors
        })

    return results
