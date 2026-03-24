"""
Module 01 — Prospect Discovery
Search Apollo.io for companies + contacts matching ICP.
"""

import json
import time
import requests
from pathlib import Path
from config import ICP, APOLLO_API_KEY, APOLLO_SEARCH_PAGE_SIZE, PROSPECTS_DIR

PROSPECTS_DIR = Path(PROSPECTS_DIR)
PROSPECTS_DIR.mkdir(exist_ok=True)


def search_companies(page=1):
    """Search for companies matching ICP keywords."""
    url = "https://api.apollo.io/v1/mixed_companies/search"
    headers = {"Authorization": f"Bearer {APOLLO_API_KEY}"}
    payload = {
        "q_organization_keywords": ",".join(ICP["keywords_include"]),
        "not_q_organization_keywords": ",".join(ICP["keywords_exclude"]),
        "organization_locations": [ICP["geo"]],
        "page": page,
        "per_page": APOLLO_SEARCH_PAGE_SIZE,
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def search_contacts(company_id, page=1):
    """Pull contacts from a specific company matching target titles."""
    url = "https://api.apollo.io/v1/contacts/search"
    headers = {"Authorization": f"Bearer {APOLLO_API_KEY}"}
    excluded_titles = [f"NOT {t}" for t in ICP["titles_excluded"]]
    payload = {
        "organization_id": company_id,
        "q_titles": ICP["titles_primary"] + excluded_titles,
        "page": page,
        "per_page": 10,
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    if resp.status_code == 429:
        print("  ⏳ Apollo rate limited, sleeping 60s...")
        time.sleep(60)
        return search_contacts(company_id, page)
    resp.raise_for_status()
    return resp.json()


def enrich_contact(contact_id):
    """Get full contact details + email + social links."""
    url = f"https://api.apollo.io/v1/contacts/{contact_id}"
    headers = {"Authorization": f"Bearer {APOLLO_API_KEY}"}
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp.json()


def run_discovery(num_company_pages=3):
    """
    Main discovery loop:
    1. Search companies by ICP keywords
    2. For each company, find contacts matching target titles
    3. Enrich and save profiles
    """
    all_prospects = []
    seen_contact_ids = set()

    for page in range(1, num_company_pages + 1):
        print(f"\n🔍 Searching companies page {page}...")
        company_data = search_companies(page)
        companies = company_data.get("organizations", []) or company_data.get("data", [])
        
        if not companies:
            print("  No more companies found.")
            break

        for org in companies:
            org_id = org.get("id") or org.get("organization_id")
            org_name = org.get("name", "Unknown")
            print(f"\n  🏢 {org_name} (Apollo ID: {org_id})")

            if not org_id:
                continue

            time.sleep(1)  # Rate limit breathing room

            try:
                contact_data = search_contacts(org_id)
                contacts = contact_data.get("contacts", []) or contact_data.get("data", [])
            except Exception as e:
                print(f"    ⚠️ Contact search failed: {e}")
                continue

            for contact in contacts:
                cid = contact.get("id")
                if not cid or cid in seen_contact_ids:
                    continue
                seen_contact_ids.add(cid)

                first_name = contact.get("first_name", "")
                last_name = contact.get("last_name", "")
                title = contact.get("title", "")
                email = contact.get("email", "")
                linkedin_url = contact.get("linkedin_url", "")

                prospect = {
                    "contact_id": cid,
                    "first_name": first_name,
                    "last_name": last_name,
                    "full_name": f"{first_name} {last_name}".strip(),
                    "title": title,
                    "email": email,
                    "linkedin_url": linkedin_url,
                    "company_name": org_name,
                    "company_id": org_id,
                    "source": "apollo",
                    "status": "discovered",
                }
                all_prospects.append(prospect)
                print(f"    ✅ {first_name} {last_name} | {title} | {email}")

            time.sleep(1)

        print(f"\n📊 Total prospects discovered so far: {len(all_prospects)}")
        time.sleep(2)

    # Save to file
    out_path = PROSPECTS_DIR / "discovered_prospects.json"
    with open(out_path, "w") as f:
        json.dump(all_prospects, f, indent=2)
    print(f"\n💾 Saved {len(all_prospects)} prospects to {out_path}")
    return all_prospects


if __name__ == "__main__":
    prospects = run_discovery()
    print(f"\n✅ Discovery complete. {len(prospects)} prospects saved.")
