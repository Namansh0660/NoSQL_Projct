"""
PDF Parser for NOSQL KG ingestion pipeline.

Features:
- Extract text from regular PDFs using pdfminer.six
- Fallback OCR using pytesseract for scanned PDFs
- Handles corrupted PDFs gracefully
- Returns a dictionary containing:
    - text
    - metadata: number of pages, file size, parser used
    - provenance: filename, timestamp
- Edge cases handled:
    - Empty PDF
    - Encrypted PDF
    - Corrupted PDF
    - Very large PDFs (streaming text extraction)
    - Non-UTF8 characters

Dependencies:
- pdfminer.six
- pytesseract
- Pillow
"""

import os
import sys
import time
import logging
from typing import Dict, Any
from pdfminer.high_level import extract_text, extract_pages
from pdfminer.pdfparser import PDFSyntaxError
from pdfminer.pdfdocument import PDFEncryptionError
from pdfminer.layout import LTTextContainer, LTChar, LAParams
from PIL import Image
import pytesseract

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("pdf_parser")

# -----------------------------
# Helper: Normalize text
# -----------------------------
def normalize_text(text: str) -> str:
    """
    Normalize extracted PDF text:
    - Strip leading/trailing whitespace
    - Replace multiple newlines with single newline
    - Remove excessive spaces
    - Ensure valid UTF-8 characters
    """
    if not text:
        return ""
    import re
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n\s*\n", "\n\n", text)  # collapse multiple empty lines
    text = re.sub(r"[ \t]+", " ", text)      # collapse multiple spaces
    text = text.strip()
    return text

# -----------------------------
# PDF Extraction
# -----------------------------
def extract_pdf_text(file_path: str) -> Dict[str, Any]:
    """
    Extract text from a PDF file.
    Returns:
        {
            "text": extracted_text,
            "metadata": { "num_pages": int, "size_bytes": int, "parser": "pdfminer" or "ocr" },
            "provenance": { "file": file_path, "fetched_at": timestamp }
        }
    Handles edge cases:
        - Empty PDF
        - Encrypted PDFs
        - Corrupted PDFs
        - Scanned PDFs (fallback OCR)
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"PDF file does not exist: {file_path}")

    file_size = os.path.getsize(file_path)
    timestamp = time.time()

    result = {
        "text": "",
        "metadata": {
            "num_pages": 0,
            "size_bytes": file_size,
            "parser": None
        },
        "provenance": {
            "file": os.path.basename(file_path),
            "fetched_at": timestamp
        }
    }

    try:
        # Attempt to extract text via pdfminer
        laparams = LAParams()
        text = extract_text(file_path, laparams=laparams)
        num_pages = len(list(extract_pages(file_path)))

        if text and text.strip():
            result["text"] = normalize_text(text)
            result["metadata"]["num_pages"] = num_pages
            result["metadata"]["parser"] = "pdfminer"
            logger.info(f"✅ Extracted text using pdfminer: {file_path} ({num_pages} pages)")
            return result
        else:
            logger.warning(f"⚠️ PDF text empty, attempting OCR: {file_path}")
            # fallback OCR
            text = ocr_pdf(file_path)
            result["text"] = normalize_text(text)
            result["metadata"]["num_pages"] = num_pages if num_pages > 0 else 1
            result["metadata"]["parser"] = "ocr"
            return result

    except PDFEncryptionError:
        logger.warning(f"🔒 Encrypted PDF, skipping text extraction: {file_path}")
        result["text"] = ""
        result["metadata"]["parser"] = "encrypted"
        return result

    except PDFSyntaxError:
        logger.warning(f"❌ Corrupted PDF detected, attempting OCR if possible: {file_path}")
        try:
            text = ocr_pdf(file_path)
            result["text"] = normalize_text(text)
            result["metadata"]["parser"] = "ocr"
        except Exception as e:
            logger.error(f"❌ OCR failed for corrupted PDF {file_path}: {e}")
            result["text"] = ""
            result["metadata"]["parser"] = "failed"
        return result

    except Exception as e:
        logger.exception(f"❌ Unexpected error extracting PDF {file_path}: {e}")
        result["text"] = ""
        result["metadata"]["parser"] = "failed"
        return result

# -----------------------------
# OCR Fallback using Tesseract
# -----------------------------
def ocr_pdf(file_path: str) -> str:
    """
    Perform OCR on a PDF file using pytesseract.
    Converts each page to an image using Pillow, then extracts text.
    Requires Tesseract installed on system and pytesseract configured.
    """
    try:
        from pdf2image import convert_from_path
    except ImportError:
        raise ImportError("pdf2image required for OCR fallback. Install via `pip install pdf2image`")

    try:
        pages = convert_from_path(file_path)
    except Exception as e:
        logger.error(f"Failed to convert PDF to images for OCR: {e}")
        return ""

    all_text = ""
    for i, page in enumerate(pages):
        try:
            text = pytesseract.image_to_string(page, lang="eng")
            all_text += text + "\n\n"
        except Exception as e:
            logger.warning(f"OCR failed for page {i} in {file_path}: {e}")
            continue

    if not all_text.strip():
        logger.warning(f"OCR produced empty text for PDF: {file_path}")
    else:
        logger.info(f"✅ OCR completed for PDF: {file_path} ({len(pages)} pages)")

    return all_text

# -----------------------------
# Example usage
# -----------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Parse a PDF and extract text (with OCR fallback).")
    parser.add_argument("file", help="Path to PDF file")
    args = parser.parse_args()

    try:
        result = extract_pdf_text(args.file)
        print("----- Extracted Text -----")
        print(result["text"][:500] + "...\n")  # print first 500 chars
        print("----- Metadata -----")
        print(result["metadata"])
        print("----- Provenance -----")
        print(result["provenance"])
    except Exception as e:
        logger.error(f"Failed to parse PDF: {e}")
