"""
Module 02 — Contact Validation
Validate email addresses to reduce bounce risk.
Supports: ZeroBounce, Abstract API, or Apollo's own email verification.
"""

import json
import time
import requests
from pathlib import Path
from config import PROSPECTS_DIR, ZEROBOUNCE_API_KEY, APOLLO_API_KEY

PROSPECTS_DIR = Path(PROSPECTS_DIR)


def validate_email_apollo(email):
    """Use Apollo's email verification (included with contact enrichment)."""
    url = "https://api.apollo.io/v1/email_verifier"
    headers = {"Authorization": f"Bearer {APOLLO_API_KEY}"}
    resp = requests.get(url, headers=headers, params={"email": email}, timeout=15)
    if resp.status_code == 429:
        time.sleep(60)
        return validate_email_apollo(email)
    if resp.status_code != 200:
        return {"status": "unknown", "email": email}
    data = resp.json()
    return {
        "email": email,
        "status": data.get("result", "unknown"),  # valid | invalid | accept_all | unknown
        "score": data.get("score", 0),
    }


def validate_email_zerobounce(email):
    """Validate via ZeroBounce API."""
    url = f"https://api.zerobounce.net/v2/validate?api_key={ZEROBOUNCE_API_KEY}&email={email}"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    return {
        "email": email,
        "status": data.get("status", "unknown"),
        "score": data.get("catch_all", 0),
    }


def run_validation(prospects_file=None, max_emails=50):
    """
    Validate emails for all discovered prospects.
    Mark invalid emails; keep valid + accept_all for review.
    """
    if prospects_file is None:
        prospects_file = PROSPECTS_DIR / "discovered_prospects.json"
    else:
        prospects_file = Path(prospects_file)

    with open(prospects_file) as f:
        prospects = json.load(f)

    validated = []
    total = min(len(prospects), max_emails)

    for i, p in enumerate(prospects[:total]):
        email = p.get("email", "").strip()
        if not email or "@" not in email:
            p["email_status"] = "missing"
            p["email_score"] = 0
            validated.append(p)
            continue

        print(f"  [{i+1}/{total}] Checking: {email}")
        try:
            result = validate_email_apollo(email)
            p["email_status"] = result.get("status", "unknown")
            p["email_score"] = result.get("score", 0)
            print(f"     → {result.get('status', '?').upper()} (score: {result.get('score', 0)})")
        except Exception as e:
            p["email_status"] = "error"
            p["email_score"] = 0
            print(f"     ⚠️ Error: {e}")

        validated.append(p)
        time.sleep(1.2)  # Stay well within Apollo rate limits

    # Save validated set
    out_path = PROSPECTS_DIR / "validated_prospects.json"
    with open(out_path, "w") as f:
        json.dump(validated, f, indent=2)

    valid_count = sum(1 for p in validated if p.get("email_status") == "valid")
    print(f"\n💾 Saved {len(validated)} validated prospects → {out_path}")
    print(f"✅ Valid emails: {valid_count} / {len(validated)}")
    return validated


if __name__ == "__main__":
    run_validation()
