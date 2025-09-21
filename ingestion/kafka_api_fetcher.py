"""
NOSQL/ingestion/kafka_api_fetcher.py

Fetch papers from multiple APIs (ArXiv, PubMed, CrossRef) in batches.
Outputs JSON-ready dicts for Kafka producer.

Uniform output format per paper:
{
    "title": str,
    "authors": [str],
    "abstract": str,
    "doi": str,
    "fetch_url": str,
    "fetched_at": float,
    "source": str,
}
"""

import time
import logging
import requests
import xml.etree.ElementTree as ET

logger = logging.getLogger("nosql.api_fetcher")
logging.basicConfig(level=logging.INFO)

# -------------------------
# ArXiv Fetcher
# -------------------------
def fetch_arxiv(batch_size=10, start_index=0, search_query="all"):
    """
    Fetch papers from ArXiv API.
    Returns list of dicts in uniform format.
    """
    url = f"http://export.arxiv.org/api/query?search_query={search_query}&start={start_index}&max_results={batch_size}"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        root = ET.fromstring(r.text)
        ns = {"atom": "http://www.w3.org/2005/Atom"}

        papers = []
        for entry in root.findall("atom:entry", ns):
            title = entry.find("atom:title", ns).text.strip() if entry.find("atom:title", ns) is not None else ""
            abstract = entry.find("atom:summary", ns).text.strip() if entry.find("atom:summary", ns) is not None else ""
            authors = [a.find("atom:name", ns).text.strip() for a in entry.findall("atom:author", ns)]
            doi = ""
            fetch_url = ""
            for link in entry.findall("atom:link", ns):
                if link.attrib.get("title") == "doi":
                    doi = link.attrib.get("href", "")
            id_tag = entry.find("atom:id", ns)
            if id_tag is not None:
                fetch_url = id_tag.text.strip()

            papers.append({
                "title": title,
                "abstract": abstract,
                "authors": authors,
                "doi": doi,
                "fetch_url": fetch_url,
                "fetched_at": time.time(),
                "source": "arxiv"
            })
        return papers
    except Exception as e:
        logger.error("ArXiv fetch error: %s", e)
        return []

# -------------------------
# PubMed Fetcher
# -------------------------
def fetch_pubmed(batch_size=10, start_index=0, term="cancer"):
    """
    Fetch papers from PubMed API.
    Uses E-utilities (esearch + efetch) to get details.
    """
    try:
        # Step 1: search
        search_url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
        params = {
            "db": "pubmed",
            "term": term,
            "retstart": start_index,
            "retmax": batch_size,
            "retmode": "json"
        }
        r = requests.get(search_url, params=params, timeout=10)
        r.raise_for_status()
        id_list = r.json().get("esearchresult", {}).get("idlist", [])
        if not id_list:
            return []

        # Step 2: fetch details
        fetch_url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
        params = {
            "db": "pubmed",
            "id": ",".join(id_list),
            "retmode": "xml"
        }
        r = requests.get(fetch_url, params=params, timeout=10)
        r.raise_for_status()
        root = ET.fromstring(r.text)

        papers = []
        for article in root.findall(".//PubmedArticle"):
            title = article.find(".//ArticleTitle")
            abstract_texts = article.findall(".//AbstractText")
            authors_nodes = article.findall(".//Author")
            doi_node = article.find(".//ELocationID[@EIdType='doi']")
            url_node = article.find(".//ArticleId[@IdType='pubmed']")
            papers.append({
                "title": title.text.strip() if title is not None else "",
                "abstract": " ".join([t.text.strip() for t in abstract_texts if t.text]) if abstract_texts else "",
                "authors": [f"{a.find('LastName').text} {a.find('ForeName').text}" 
                            for a in authors_nodes if a.find("LastName") is not None and a.find("ForeName") is not None],
                "doi": doi_node.text.strip() if doi_node is not None else "",
                "fetch_url": f"https://pubmed.ncbi.nlm.nih.gov/{url_node.text}/" if url_node is not None else "",
                "fetched_at": time.time(),
                "source": "pubmed"
            })
        return papers
    except Exception as e:
        logger.error("PubMed fetch error: %s", e)
        return []

# -------------------------
# CrossRef Fetcher
# -------------------------
def fetch_crossref(batch_size=10, offset=0, query="science"):
    """
    Fetch papers from CrossRef API
    """
    url = "https://api.crossref.org/works"
    try:
        params = {
            "rows": batch_size,
            "offset": offset,
            "query": query
        }
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        items = r.json().get("message", {}).get("items", [])
        papers = []
        for item in items:
            title = item.get("title", [""])[0] if item.get("title") else ""
            abstract = item.get("abstract", "") or ""
            authors = []
            for author in item.get("author", []):
                name_parts = [author.get("given", ""), author.get("family", "")]
                authors.append(" ".join([p for p in name_parts if p]))
            doi = item.get("DOI", "")
            fetch_url = item.get("URL", "")
            papers.append({
                "title": title,
                "abstract": abstract,
                "authors": authors,
                "doi": doi,
                "fetch_url": fetch_url,
                "fetched_at": time.time(),
                "source": "crossref"
            })
        return papers
    except Exception as e:
        logger.error("CrossRef fetch error: %s", e)
        return []

# -------------------------
# Example usage: fetch 5 from each
# -------------------------
if __name__ == "__main__":
    print("Fetching ArXiv...")
    for p in fetch_arxiv(batch_size=5):
        print(p)

    print("Fetching PubMed...")
    for p in fetch_pubmed(batch_size=5):
        print(p)

    print("Fetching CrossRef...")
    for p in fetch_crossref(batch_size=5):
        print(p)
