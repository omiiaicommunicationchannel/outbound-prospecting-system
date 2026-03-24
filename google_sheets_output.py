"""
Google Sheets Output Module
Writes prospect data to Google Sheets for easy viewing/editing.
Uses Service Account with pre-shared spreadsheet access.
"""

import os
import json
from datetime import datetime
from pathlib import Path

# Google Sheets API imports
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ── Configuration ────────────────────────────────────────────────────────────
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SERVICE_ACCOUNT_FILE = Path(__file__).parent / "credentials" / "service-account.json"

# Default spreadsheet ID (shared via Drive permissions)
DEFAULT_SPREADSHEET_ID = "1T5CKzf1NFibpW3DGN9vTbRXHhgo9Obn7pusT1KX79b8"


class GoogleSheetsOutput:
    """Write pipeline output to Google Sheets."""
    
    def __init__(self, spreadsheet_id: str = None):
        self.spreadsheet_id = spreadsheet_id or DEFAULT_SPREADSHEET_ID
        self.service = None
        self._authenticate()
    
    def _authenticate(self):
        """Authenticate with Google Sheets API via Service Account."""
        
        if not SERVICE_ACCOUNT_FILE.exists():
            print("No Service Account credentials found.")
            return
        
        credentials = service_account.Credentials.from_service_account_file(
            str(SERVICE_ACCOUNT_FILE), scopes=SCOPES
        )
        
        self.service = build("sheets", "v4", credentials=credentials)
        print("Authenticated with Google Sheets API")
    
    def write_prospects(self, prospects: list, sheet_name: str = "Prospects"):
        """Write prospects data to sheet."""
        if not self.service or not self.spreadsheet_id:
            print("Not connected to Google Sheets")
            return
        
        # Build header row
        headers = [
            "Business Name", "Location", "Industry", "Contact Name", 
            "Contact Title", "Email", "Phone", "LinkedIn", "Website",
            "Has Facebook", "Has Instagram", "Has LinkedIn", "Source", "Discovered At"
        ]
        
        rows = [headers]
        for p in prospects:
            contacts = p.get("contacts", [{}])
            contact = contacts[0] if contacts else {}
            scan = p.get("platform_scan", {})
            
            rows.append([
                p.get("name", ""),
                p.get("location", ""),
                p.get("industry", ""),
                contact.get("name", ""),
                contact.get("title", ""),
                contact.get("email", ""),
                contact.get("phone", ""),
                contact.get("linkedin", ""),
                p.get("domain", ""),
                "Yes" if scan.get("has_facebook") else "No",
                "Yes" if scan.get("has_instagram") else "No",
                "Yes" if scan.get("has_linkedin") else "No",
                p.get("source", ""),
                p.get("discovered_at", datetime.utcnow().isoformat()),
            ])
        
        body = {"values": rows}
        
        # Clear existing data and write new (use Sheet1 as default)
        sheet_range = f"{sheet_name}!A:Z"
        try:
            self.service.spreadsheets().values().clear(
                spreadsheetId=self.spreadsheet_id,
                range=sheet_range
            ).execute()
        except:
            pass  # Sheet may not exist
        
        self.service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range="Sheet1!A1",
            valueInputOption="USER_ENTERED",
            body=body
        ).execute()
        
        print(f"Wrote {len(rows)} rows to {sheet_name}")
    
    def write_outreach(self, outreach: list, sheet_name: str = "Outreach"):
        """Write outreach emails to sheet."""
        if not self.service or not self.spreadsheet_id:
            print("Not connected to Google Sheets")
            return
        
        # Build header row
        headers = ["To Name", "To Email", "Subject", "Body", "Offer Type", "Status"]
        
        rows = [headers]
        for o in outreach:
            rows.append([
                o.get("to_name", ""),
                o.get("to_email", ""),
                o.get("subject", ""),
                o.get("body", ""),
                o.get("offer_type", ""),
                "Pending"
            ])
        
        body = {"values": rows}
        
        # Clear and write to Sheet1 (default)
        try:
            self.service.spreadsheets().values().clear(
                spreadsheetId=self.spreadsheet_id,
                range="Sheet1!A:Z"
            ).execute()
        except:
            pass
        
        self.service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range="Sheet1!A1",
            valueInputOption="USER_ENTERED",
            body=body
        ).execute()
        
        print(f"Wrote {len(rows)} rows to {sheet_name}")
    
    def get_spreadsheet_url(self) -> str:
        """Return shareable URL."""
        if self.spreadsheet_id:
            return f"https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}"
        return None


# ── CLI Test ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    gs = GoogleSheetsOutput()
    if gs.service:
        print(f"URL: {gs.get_spreadsheet_url()}")
        
        # Test write
        gs.write_prospects([{
            "name": "Test Business",
            "location": "Brooklyn, NY",
            "industry": "Plumbing",
            "contacts": [{"name": "John", "title": "Owner", "email": "john@test.com"}],
            "platform_scan": {"has_facebook": True, "has_website": False},
            "source": "test"
        }])
