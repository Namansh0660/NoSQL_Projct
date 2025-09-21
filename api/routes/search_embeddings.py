from fastapi import APIRouter, Query
from typing import List
from db import KG_NODES
import numpy as np
from sentence_transformers import SentenceTransformer

router = APIRouter(prefix="/semantic_search", tags=["semantic_search"])

# Load model once
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

def cosine_similarity(a: np.ndarray, b: np.ndarray):
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))

@router.get("/papers")
def semantic_search_papers(query: str, top_k: int = 5):
    """
    Search papers semantically by embedding similarity
    """
    papers = list(KG_NODES.find({"type": "Paper"}))
    query_emb = model.encode(query)

    scored = []
    for paper in papers:
        text = paper["properties"].get("title", "") + " " + paper["properties"].get("abstract", "")
        if not text.strip():
            continue
        paper_emb = model.encode(text)
        score = cosine_similarity(query_emb, paper_emb)
        scored.append((score, paper))

    scored.sort(reverse=True, key=lambda x: x[0])
    return [p[1] for p in scored[:top_k]]
