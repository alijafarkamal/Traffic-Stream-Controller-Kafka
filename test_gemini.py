#!/usr/bin/env python3
"""
Quick sanity test for Gemini API using google-genai SDK.

Reads the API key from environment variable GEMINI_API_KEY (via python-dotenv if .env present).
Prints the model text response or a concise error.
"""
import os
import sys
from dotenv import load_dotenv

try:
    from google import genai
except Exception as e:
    print(f"[ERROR] google-genai not installed: {e}. Install with: pip install --break-system-packages google-genai")
    sys.exit(1)


def main():
    load_dotenv()
    if not os.getenv("GEMINI_API_KEY"):
        print("[ERROR] GEMINI_API_KEY not set in environment/.env")
        sys.exit(2)

    try:
        client = genai.Client()
        resp = client.models.generate_content(
            model="gemini-2.5-flash",
            contents="Explain how AI works in a few words",
        )
        print("[OK] Gemini response:\n" + (resp.text or "<empty>"))
    except Exception as e:
        print(f"[ERROR] Gemini call failed: {e}")
        sys.exit(3)


if __name__ == "__main__":
    main()
