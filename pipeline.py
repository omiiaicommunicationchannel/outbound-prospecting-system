"""
Outbound Prospecting Pipeline v2 — Reusable ICP-to-Outreach System

USAGE:
  python pipeline.py --industry "plumbers" --location "Brooklyn, NY" 
                    --platforms "facebook,instagram" --exclude-website true
                    --offer "website-audit"

PIPELINE STEPS:
  1. ICP Generation    → Generate targeting criteria from inputs
  2. Discovery        → Find businesses matching industry + location
  3. Platform Scan    → Check for FB/IG pages, detect no website
  4. Contact Enrich   → Get contact info (email, phone, owner)
  5. Personalization  → Generate tailored email per prospect
  6. Output           → Ready-to-send outreach list
"""

import argparse
import json
import os
import time
import requests
from pathlib import Path
from datetime import datetime

# Local modules
from google_sheets_output import GoogleSheetsOutput

# ── Configuration ────────────────────────────────────────────────────────────
CONFIG = {
    "output_dir": "./output",
    "prospects_file": "prospects.json",
    "outreach_file": "outreach_ready.jsonl",
    "apollo_api_key": os.getenv("APOLLO_API_KEY", ""),
    "tavily_api_key": os.getenv("TAVILY_API_KEY", ""),
    "search_results_perQuery": 20,
}

CONFIG["output_dir"] = Path(CONFIG["output_dir"])
CONFIG["output_dir"].mkdir(exist_ok=True)


# ── ICP Generator ────────────────────────────────────────────────────────────
class ICPGenerator:
    """Generate ICP from simple inputs."""
    
    TEMPLATES = {
        "plumbers": {
            "keywords": ["plumbing", "plumber", "drain", "pipe", "water heater"],
            "titles": ["owner", "president", "manager", "master plumber"],
            "exclude_keywords": ["repair", "appliance"],
        },
        "roofers": {
            "keywords": ["roofing", "roofer", "roof repair", "shingle"],
            "titles": ["owner", "president", "manager", "ceo"],
            "exclude_keywords": ["solar"],
        },
        "electricians": {
            "keywords": ["electrician", "electrical", "electric repair"],
            "titles": ["owner", "president", "master electrician", "manager"],
            "exclude_keywords": ["solar", "automation"],
        },
        "restaurants": {
            "keywords": ["restaurant", "eatery", "dining"],
            "titles": ["owner", "chef", "manager", "general manager"],
            "exclude_keywords": ["chain", "franchise"],
        },
        "default": {
            "keywords": [],
            "titles": ["owner", "president", "ceo", "founder", "manager"],
            "exclude_keywords": [],
        },
    }
    
    @classmethod
    def generate(cls, industry: str, location: str, platforms: list = None, 
                 exclude_website: bool = False) -> dict:
        """Generate ICP from inputs."""
        industry_lower = industry.lower()
        template = cls.TEMPLATES.get(industry_lower, cls.TEMPLATES["default"])
        
        icp = {
            "industry": industry,
            "location": location,
            "target_business_keywords": template["keywords"],
            "target_contact_titles": template["titles"],
            "exclude_keywords": template["exclude_keywords"],
            "platforms_required": platforms or ["facebook", "instagram"],
            "exclude_website": exclude_website,
            "created_at": datetime.utcnow().isoformat(),
        }
        
        print(f"\n🎯 ICP Generated:")
        print(f"   Industry: {industry}")
        print(f"   Location: {location}")
        print(f"   Platforms: {icp['platforms_required']}")
        print(f"   Exclude No Website: {exclude_website}")
        
        return icp


# ── Discovery Module ─────────────────────────────────────────────────────────
class Discovery:
    """Find businesses via search + Apollo."""
    
    def __init__(self, icp: dict):
        self.icp = icp
        self.results = []
    
    def search_businesses(self) -> list:
        """Search for businesses matching ICP."""
        # Use Tavily for local business search
        if CONFIG["tavily_api_key"]:
            return self._search_tavily()
        return self._search_google()
    
    def _search_tavily(self) -> list:
        """Search via Tavily AI search."""
        url = "https://api.tavily.com/search"
        query = f"{self.icp['industry']} in {self.icp['location']}"
        
        payload = {
            "api_key": CONFIG["tavily_api_key"],
            "query": query,
            "search_depth": "basic",
            "max_results": CONFIG["search_results_perQuery"],
        }
        
        resp = requests.post(url, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        
        results = []
        for item in data.get("results", []):
            results.append({
                "name": item.get("title", ""),
                "url": item.get("url", ""),
                "description": item.get("content", ""),
                "source": "tavily",
            })
        
        print(f"\n🔍 Found {len(results)} businesses via Tavily")
        return results
    
    def _search_google(self) -> list:
        """Fallback: scrape Google Maps results."""
        # Simple search simulation - in production use Google Places API
        print("\n⚠️ Using placeholder - add TAVILY_API_KEY for real search")
        return [
            {
                "name": f"{self.icp['industry'].title()} Business",
                "url": "",
                "description": "",
                "source": "google",
            }
        ]
    
    def enrich_with_apollo(self, business_name: str) -> dict:
        """Enrich business with Apollo data."""
        if not CONFIG["apollo_api_key"]:
            return {}
        
        url = "https://api.apollo.io/v1/mixed_companies/search"
        headers = {"Authorization": f"Bearer {CONFIG['apollo_api_key']}"}
        
        payload = {
            "q_organization_name": business_name,
            "page": 1,
            "per_page": 1,
        }
        
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=15)
            data = resp.json()
            orgs = data.get("organizations", [])
            if orgs:
                org = orgs[0]
                return {
                    "apollo_id": org.get("id"),
                    "domain": org.get("domain"),
                    "linkedin_url": org.get("linkedin_url"),
                    "facebook_url": org.get("facebook_url"),
                    "twitter_url": org.get("twitter_url"),
                    "employee_count": org.get("employee_count"),
                }
        except Exception as e:
            print(f"   ⚠️ Apollo enrich failed: {e}")
        
        return {}
    
    def run(self) -> list:
        """Execute discovery."""
        businesses = self.search_businesses()
        
        enriched = []
        for biz in businesses:
            print(f"  📍 {biz['name']}")
            apollo_data = self.enrich_with_apollo(biz["name"])
            biz.update(apollo_data)
            enriched.append(biz)
            time.sleep(0.5)
        
        self.results = enriched
        return enriched


# ── Platform Scanner ─────────────────────────────────────────────────────────
class PlatformScanner:
    """Check if business has social profiles but no website."""
    
    def __init__(self, icp: dict):
        self.icp = icp
    
    def scan(self, business: dict) -> dict:
        """Scan for social presence and website."""
        result = {
            "has_facebook": bool(business.get("facebook_url")),
            "has_instagram": False,  # Apollo doesn't always provide
            "has_linkedin": bool(business.get("linkedin_url")),
            "has_website": bool(business.get("domain")),
            "website_detected": False,
        }
        
        # Check if domain exists and is real website
        if result["has_website"]:
            result["website_detected"] = self._verify_website(business.get("domain"))
        
        # Check platform requirements
        platforms = self.icp.get("platforms_required", [])
        result["meets_criteria"] = True
        
        if "facebook" in platforms and not result["has_facebook"]:
            result["meets_criteria"] = False
        if "instagram" in platforms and not result["has_instagram"]:
            result["meets_criteria"] = False
        if self.icp.get("exclude_website") and result["has_website"]:
            result["meets_criteria"] = False
        
        return result
    
    def _verify_website(self, domain: str) -> bool:
        """Verify domain resolves to real website."""
        if not domain:
            return False
        try:
            resp = requests.get(f"https://{domain}", timeout=5, allow_redirects=True)
            return resp.status_code == 200
        except:
            return False
    
    def filter_businesses(self, businesses: list) -> list:
        """Filter businesses matching criteria."""
        filtered = []
        
        for biz in businesses:
            scan_result = self.scan(biz)
            biz["platform_scan"] = scan_result
            
            if scan_result["meets_criteria"]:
                print(f"    ✅ {biz['name']} - meets criteria (has {['FB' if scan_result['has_facebook'] else '', 'LI' if scan_result['has_linkedin'] else '']} no website: {not scan_result['has_website']})")
                filtered.append(biz)
            else:
                print(f"    ❌ {biz['name']} - skipped")
        
        print(f"\n📊 Filtered: {len(filtered)}/{len(businesses)} businesses meet criteria")
        return filtered


# ── Contact Enrichment ───────────────────────────────────────────────────────
class ContactEnrich:
    """Find contact info for business."""
    
    def __init__(self, icp: dict):
        self.icp = icp
    
    def find_contacts(self, business: dict) -> list:
        """Find contacts at business."""
        if not CONFIG["apollo_api_key"]:
            return [self._manual_fallback(business)]
        
        url = "https://api.apollo.io/v1/contacts/search"
        headers = {"Authorization": f"Bearer {CONFIG['apollo_api_key']}"}
        
        org_id = business.get("apollo_id")
        if not org_id:
            return [self._manual_fallback(business)]
        
        payload = {
            "organization_id": org_id,
            "q_titles": self.icp.get("target_contact_titles", []),
            "page": 1,
            "per_page": 5,
        }
        
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=15)
            data = resp.json()
            contacts = data.get("contacts", [])
            
            return [
                {
                    "name": f"{c.get('first_name','')} {c.get('last_name','')}".strip(),
                    "title": c.get("title", ""),
                    "email": c.get("email", ""),
                    "linkedin": c.get("linkedin_url", ""),
                    "phone": c.get("phone_number", ""),
                }
                for c in contacts if c.get("email")
            ]
        except Exception as e:
            print(f"   ⚠️ Contact search failed: {e}")
            return [self._manual_fallback(business)]
    
    def _manual_fallback(self, business: dict) -> dict:
        """Fallback when no Apollo data."""
        return {
            "name": "Owner",
            "title": "Owner",
            "email": "",
            "linkedin": "",
            "phone": "",
        }
    
    def run(self, businesses: list) -> list:
        """Enrich all businesses with contacts."""
        for biz in businesses:
            contacts = self.find_contacts(biz)
            biz["contacts"] = contacts
            for c in contacts:
                print(f"    👤 {c['name']} - {c['title']} - {c['email']}")
            time.sleep(0.5)
        
        return businesses


# ── Personalization Engine ───────────────────────────────────────────────────
class Personalizer:
    """Generate personalized outreach emails."""
    
    OFFERS = {
        "website-audit": {
            "subject": "Quick question about your {business_name}",
            "body": """Hi {first_name},

I noticed {business_name} is doing great work in the {industry} space in {location}.

Quick question — do you have a website that showcases your services? If not, I'd love to show you how a simple presence could help you capture more local customers searching for {industry} services.

We help {industry} businesses just like yours generate more leads with a professional website + local SEO package. No big budget required.

Would you be open to a 10-minute call this week to discuss?

Best,
{ sender_name }
{ sender_email }""",
        },
        "google-business": {
            "subject": "Help more customers find {business_name}",
            "body": """Hi {first_name},

Are you showing up when people search for "{industry} near me" in {location}?

If not, we'd love to help you claim and optimize your Google Business Profile — it's free and can double your inbound calls.

Let me know if you'd like a quick audit.

Best,
{ sender_name }""",
        },
        "social-marketing": {
            "subject": "Boosting {business_name}'s social presence",
            "body": """Hi {first_name},

Love what {business_name} is doing on social media! 

We help local businesses like yours turn followers into customers with automated posting, engagement, and lead capture.

Interested in a free social audit?

Best,
{ sender_name }""",
        },
    }
    
    def __init__(self, icp: dict, offer: str = "website-audit"):
        self.icp = icp
        self.offer = offer
        self.template = self.OFFERS.get(offer, self.OFFERS["website-audit"])
    
    def generate(self, prospect: dict) -> dict:
        """Generate personalized email for prospect."""
        first_name = prospect.get("contacts", [{}])[0].get("name", "there").split()[0]
        if not first_name:
            first_name = "there"
        
        business_name = prospect.get("name", "your business")
        industry = self.icp.get("industry", "local")
        location = self.icp.get("location", "area")
        
        subject = self.template["subject"].format(
            business_name=business_name,
            first_name=first_name,
            industry=industry,
            location=location,
        )
        
        body = self.template["body"].format(
            first_name=first_name,
            business_name=business_name,
            industry=industry,
            location=location,
            sender_name="Sable",
            sender_email="sable@omii.ai",
        )
        
        return {
            "prospect_id": prospect.get("id", ""),
            "to_name": first_name,
            "to_email": prospect.get("contacts", [{}])[0].get("email", ""),
            "subject": subject,
            "body": body,
            "offer_type": self.offer,
        }
    
    def run(self, businesses: list) -> list:
        """Generate emails for all prospects."""
        outreach = []
        
        for biz in businesses:
            if not biz.get("contacts"):
                continue
            
            email = self.generate(biz)
            if email["to_email"]:
                outreach.append(email)
                print(f"    ✉️ Ready: {email['to_email']}")
        
        print(f"\n📬 Generated {len(outreach)} outreach emails")
        return outreach


# ── Main Pipeline ────────────────────────────────────────────────────────────
def run_pipeline(
    industry: str,
    location: str,
    platforms: str = "facebook,instagram",
    exclude_website: bool = True,
    offer: str = "website-audit",
    sender_name: str = "Sable",
    sender_email: str = "sable@omii.ai",
):
    """Execute full prospecting pipeline."""
    
    print("\n" + "="*60)
    print("🚀 OUTBOUND PROSPECTING PIPELINE v2")
    print("="*60)
    
    # Step 1: Generate ICP
    print("\n[1/5] 🎯 Generating ICP...")
    platform_list = [p.strip() for p in platforms.split(",")]
    icp = ICPGenerator.generate(industry, location, platform_list, exclude_website)
    
    # Step 2: Discovery
    print("\n[2/5] 🔍 Discovering businesses...")
    discovery = Discovery(icp)
    businesses = discovery.run()
    
    if not businesses:
        print("❌ No businesses found. Check API keys or try different query.")
        return
    
    # Step 3: Platform Scan
    print("\n[3/5] 📱 Scanning for social presence...")
    scanner = PlatformScanner(icp)
    qualified = scanner.filter_businesses(businesses)
    
    if not qualified:
        print("❌ No businesses meet criteria. Try relaxing filters.")
        return
    
    # Step 4: Contact Enrich
    print("\n[4/5] 👤 Enriching with contact info...")
    enricher = ContactEnrich(icp)
    enriched = enricher.run(qualified)
    
    # Step 5: Personalize
    print("\n[5/5] ✉️ Generating personalized outreach...")
    personalizer = Personalizer(icp, offer)
    outreach = personalizer.run(enriched)
    
    # Save outputs
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    
    icp_file = CONFIG["output_dir"] / f"icp_{timestamp}.json"
    with open(icp_file, "w") as f:
        json.dump(icp, f, indent=2)
    
    prospects_file = CONFIG["output_dir"] / f"prospects_{timestamp}.json"
    with open(prospects_file, "w") as f:
        json.dump(enriched, f, indent=2)
    
    outreach_file = CONFIG["output_dir"] / f"outreach_{timestamp}.jsonl"
    with open(outreach_file, "w") as f:
        for email in outreach:
            f.write(json.dumps(email) + "\n")
    
    print("\n" + "="*60)
    print("✅ PIPELINE COMPLETE")
    print("="*60)
    print(f"📁 ICP: {icp_file}")
    print(f"📁 Prospects: {prospects_file}")
    print(f"📁 Outreach: {outreach_file}")
    print(f"📊 Summary: {len(enriched)} businesses → {len(outreach)} emails ready")
    
    # Write to Google Sheets
    try:
        print("\n[6/6] 📊 Writing to Google Sheets...")
        gs = GoogleSheetsOutput()
        spreadsheet_id = gs.create_spreadsheet(f"Prospects - {industry} {location}")
        gs.write_prospects(enriched)
        gs.write_outreach(outreach)
        sheets_url = gs.get_spreadsheet_url()
        print(f"📊 Google Sheets: {sheets_url}")
    except Exception as e:
        print(f"⚠️ Google Sheets write failed: {e}")
        sheets_url = None
    
    return {
        "icp": icp,
        "businesses": enriched,
        "outreach": outreach,
        "sheets_url": sheets_url,
    }


# ── CLI Entry Point ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Outbound Prospecting Pipeline")
    parser.add_argument("--industry", required=True, help="Target industry (e.g., plumbers)")
    parser.add_argument("--location", required=True, help="Target location (e.g., Brooklyn, NY)")
    parser.add_argument("--platforms", default="facebook,instagram", help="Required platforms")
    parser.add_argument("--exclude-website", type=bool, default=True, help="Exclude businesses with website")
    parser.add_argument("--offer", default="website-audit", help="Offer type (website-audit, google-business, social-marketing)")
    parser.add_argument("--sender-name", default="Sable", help="Sender name")
    parser.add_argument("--sender-email", default="sable@omii.ai", help="Sender email")
    
    args = parser.parse_args()
    
    run_pipeline(
        industry=args.industry,
        location=args.location,
        platforms=args.platforms,
        exclude_website=args.exclude_website,
        offer=args.offer,
        sender_name=args.sender_name,
        sender_email=args.sender_email,
    )
