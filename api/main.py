from fastapi import FastAPI
from routes import nodes, edges, search

app = FastAPI(
    title="NOSQL KG API",
    description="API for querying Knowledge Graph of scientific literature",
    version="1.0.0"
)

# Include routers
app.include_router(nodes.router)
app.include_router(edges.router)
app.include_router(search.router)

# Root endpoint
@app.get("/")
def root():
    return {"message": "Welcome to NOSQL Knowledge Graph API"}
