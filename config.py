"""
Outbound Prospecting System — Configuration
Defines your ICP, targets, and integrations.
"""

# ── ICP Definition ──────────────────────────────────────────────────────────
ICP = {
    "industries": ["SaaS", "E-commerce", "Professional Services", "Manufacturing"],
    "company_sizes": ["11-50", "51-200", "201-1000"],
    "titles_excluded": ["Intern", "Junior", "HR", "Recruiter", "Talent"],
    "titles_primary": ["CEO", "CTO", "COO", "VP Engineering", "Head of Operations", "Director of Engineering"],
    "geo": "United States",
    "keywords_include": ["automation", "AI", "workflow", "efficiency", "scaling"],
    "keywords_exclude": ["non-profit", "government", "education"],
}

# ── Apollo.io Config ────────────────────────────────────────────────────────
APOLLO_API_KEY = "YOUR_APOLLO_API_KEY"  # Get from https://apollo.io
APOLLO_SEARCH_PAGE_SIZE = 10            # Results per search page

# ── Email Validation ─────────────────────────────────────────────────────────
ZEROBOUNCE_API_KEY = "YOUR_ZEROBOUNCE_API_KEY"  # Optional: https://zerobounce.net

# ── Email Sending ────────────────────────────────────────────────────────────
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "your-email@gmail.com"
SMTP_PASSWORD = "your-app-password"      # Gmail App Password
FROM_NAME = "Omii AI"
FROM_EMAIL = "your-email@gmail.com"

# ── Output / State ───────────────────────────────────────────────────────────
PROSPECTS_DIR = "./prospects"
OUTREACH_LOG = "./outreach_log.jsonl"
