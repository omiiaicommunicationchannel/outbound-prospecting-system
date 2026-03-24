"""
Module 03 — Personalized Outreach Generator
Generate a short, highly personalized opening email per prospect using LLM logic.
"""

import json
from pathlib import Path
from config import PROSPECTS_DIR

PROSPECTS_DIR = Path(PROSPECTS_DIR)


# ── Persona & Offer Framing ───────────────────────────────────────────────────
BRAND_CONTEXT = """
You run a specialized automation & AI integration consultancy.
You help mid-market companies (51-500 employees) eliminate repetitive operational bottlenecks
through custom AI agent pipelines, workflow automation, and systems integration.
Typical outcomes: 10–30 hrs/week saved per team, reduced manual errors, faster scaling.
"""

EMAIL_TONE = "warm, direct, no fluff. 3–4 sentences max in the body. One clear CTA."


# ── Personalization Hooks ────────────────────────────────────────────────────
HOOK_TEMPLATES = {
    "CEO": "Congrats on scaling {company} — at that stage, operational bottlenecks tend to be the #1 drag on growth.",
    "CTO": "Curious how {company}'s engineering team is handling the transition to AI-augmented workflows.",
    "VP Engineering": "At your scale, I'd bet your engineers are spending too much time on manual ops that should be automated.",
    "Head of Operations": "Operations at {company} at this size is a tough nut — curious how you're thinking about workflow efficiency.",
    "Director of Engineering": "What does the dev team's sprint look like when 20% of it is manual grunt work?",
    "default": "Came across {company} and liked what I saw — curious how your team handles the automation side of scaling.",
}

CTA_OPTIONS = [
    "Worth a quick 15-min call this week?",
    "Open to a brief chat if the timing's right?",
    "Happy to share how we helped a similar company cut 15 hrs/week of manual ops.",
]


def build_hook(prospect):
    """Pick the best hook based on title keyword match."""
    title = prospect.get("title", "").lower()
    company = prospect.get("company_name", "your team")

    for key, tmpl in HOOK_TEMPLATES.items():
        if key.lower() in title:
            return tmpl.format(company=company)

    # Fuzzy match on keywords
    for key in ["engineer", "tech", "product"]:
        if key in title:
            return HOOK_TEMPLATES["VP Engineering"].format(company=company)

    return HOOK_TEMPLATES["default"].format(company=company)


def generate_email(prospect):
    """Assemble a personalized email draft."""
    first_name = prospect.get("first_name", "")
    full_name = prospect.get("full_name", "")
    title = prospect.get("title", "")
    company = prospect.get("company_name", "")
    hook = build_hook(prospect)

    subject = f"Quick idea for {company}"

    body = f"""Hi {first_name},

{hook}

We work with companies in your space to automate the manual workflows that slow teams down — typically saving 10–20 hrs/week per person within 60 days.

Worth a quick 15-min call this week?
"""

    email_obj = {
        "to_email": prospect.get("email", ""),
        "to_name": full_name,
        "subject": subject,
        "body": body.strip(),
        "contact_id": prospect.get("contact_id", ""),
        "company_name": company,
        "status": "drafted",
    }
    return email_obj


def run_personalization(prospects_file=None, validated=True):
    """
    Generate personalized emails for all validated prospects.
    validated=True → reads from validated_prospects.json
    """
    src = PROSPECTS_DIR / ("validated_prospects.json" if validated else "discovered_prospects.json")
    if prospects_file:
        src = Path(prospects_file)

    with open(src) as f:
        prospects = json.load(f)

    # Filter to only those with valid or unknown (reviewable) emails
    valid_prospects = [
        p for p in prospects
        if p.get("email") and p.get("email_status") in ("valid", "accept_all", "unknown", "missing")
    ]

    emails = []
    for p in valid_prospects:
        email = generate_email(p)
        emails.append(email)
        print(f"  ✉️  {email['to_name']} <{email['to_email']}> | {p.get('title','')}")

    out_path = PROSPECTS_DIR / "email_drafts.json"
    with open(out_path, "w") as f:
        json.dump(emails, f, indent=2)

    print(f"\n💾 Drafted {len(emails)} emails → {out_path}")
    return emails


if __name__ == "__main__":
    run_personalization()
