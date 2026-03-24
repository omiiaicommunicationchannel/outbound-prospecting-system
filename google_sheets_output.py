"""
Google Sheets Output Module
Writes prospect data to Google Sheets for easy viewing/editing.
"""

import os
import json
from datetime import datetime
from pathlib import Path

# Google Sheets API imports
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ── Configuration ────────────────────────────────────────────────────────────
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
CREDENTIALS_FILE = Path(__file__).parent / "credentials" / "google-sheets-credentials.json"
TOKEN_FILE = Path(__file__).parent / "credentials" / "google-sheets-token.json"


class GoogleSheetsOutput:
    """Write pipeline output to Google Sheets."""
    
    def __init__(self, spreadsheet_id: str = None):
        self.spreadsheet_id = spreadsheet_id
        self.service = None
        self._authenticate()
    
    def _authenticate(self):
        """Authenticate with Google Sheets API."""
        creds = None
        
        # Load existing token
        if TOKEN_FILE.exists():
            creds = Credentials.from_authorized_user_info(
                json.loads(TOKEN_FILE.read_text()), SCOPES
            )
        
        # If no valid credentials, run OAuth flow
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            elif CREDENTIALS_FILE.exists():
                flow = InstalledAppFlow.from_client_secrets_file(
                    str(CREDENTIALS_FILE), SCOPES
                )
                creds = flow.run_local_server(port=8080)
            else:
                print("❌ No Google Sheets credentials found.")
                print("   1. Go to https://console.cloud.google.com/apis/credentials")
                print("   2. Create OAuth 2.0 Client ID (Desktop app)")
                print("   3. Download as credentials.json → place in pipeline/credentials/")
                return
            
            # Save token for future use
            TOKEN_FILE.write_text(json.dumps(json.loads(creds.to_json())))
        
        self.service = build("sheets", "v4", credentials=creds)
        print("✅ Authenticated with Google Sheets API")
    
    def create_spreadsheet(self, title: str = "Prospects Pipeline Output") -> str:
        """Create new spreadsheet and return its ID."""
        spreadsheet = {
            "properties": {"title": title},
            "sheets": [
                {
                    "properties": {"title": "Prospects"},
                    "data": [self._prospects_grid()],
                },
                {
                    "properties": {"title": "Outreach"},
                    "data": [self._outreach_grid()],
                },
            ],
        }
        
        spreadsheet = self.service.spreadsheets().create(
            body=spreadsheet, fields="spreadsheetId"
        ).execute()
        
        self.spreadsheet_id = spreadsheet.get("spreadsheetId")
        print(f"✅ Created spreadsheet: https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}")
        return self.spreadsheet_id
    
    def _prospects_grid(self) -> dict:
        """Define Prospects sheet grid."""
        return {
            "startRow": 1,
            "startColumn": 1,
            "rowData": [
                {
                    "values": [
                        {"userEnteredValue": {"stringValue": "Business Name"}},
                        {"userEnteredValue": {"stringValue": "Location"}},
                        {"userEnteredValue": {"stringValue": "Industry"}},
                        {"userEnteredValue": {"stringValue": "Contact Name"}},
                        {"userEnteredValue": {"stringValue": "Contact Title"}},
                        {"userEnteredValue": {"stringValue": "Email"}},
                        {"userEnteredValue": {"stringValue": "Phone"}},
                        {"userEnteredValue": {"stringValue": "LinkedIn"}},
                        {"userEnteredValue": {"stringValue": "Website"}},
                        {"userEnteredValue": {"stringValue": "Has Facebook"}},
                        {"userEnteredValue": {"stringValue": "Has Instagram"}},
                        {"userEnteredValue": {"stringValue": "Has LinkedIn"}},
                        {"userEnteredValue": {"stringValue": "Source"}},
                        {"userEnteredValue": {"stringValue": "Discovered At"}},
                    ]
                }
            ],
        }
    
    def _outreach_grid(self) -> dict:
        """Define Outreach sheet grid."""
        return {
            "startRow": 1,
            "startColumn": 1,
            "rowData": [
                {
                    "values": [
                        {"userEnteredValue": {"stringValue": "To Name"}},
                        {"userEnteredValue": {"stringValue": "To Email"}},
                        {"userEnteredValue": {"stringValue": "Subject"}},
                        {"userEnteredValue": {"stringValue": "Body"}},
                        {"userEnteredValue": {"stringValue": "Offer Type"}},
                        {"userEnteredValue": {"stringValue": "Status"}},
                    ]
                }
            ],
        }
    
    def write_prospects(self, prospects: list):
        """Write prospects data to sheet."""
        if not self.service or not self.spreadsheet_id:
            print("❌ Not connected to Google Sheets")
            return
        
        rows = []
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
        
        self.service.spreadsheets().values().append(
            spreadsheetId=self.spreadsheet_id,
            range="Prospects!A2",
            valueInputOption="USER_ENTERED",
            body=body,
        ).execute()
        
        print(f"✅ Wrote {len(rows)} prospects to Google Sheets")
    
    def write_outreach(self, outreach: list):
        """Write outreach emails to sheet."""
        if not self.service or not self.spreadsheet_id:
            print("❌ Not connected to Google Sheets")
            return
        
        rows = []
        for o in outreach:
            rows.append([
                o.get("to_name", ""),
                o.get("to_email", ""),
                o.get("subject", ""),
                o.get("body", ""),
                o.get("offer_type", ""),
                "Pending",
            ])
        
        body = {"values": rows}
        
        self.service.spreadsheets().values().append(
            spreadsheetId=self.spreadsheet_id,
            range="Outreach!A2",
            valueInputOption="USER_ENTERED",
            body=body,
        ).execute()
        
        print(f"✅ Wrote {len(rows)} outreach emails to Google Sheets")
    
    def get_spreadsheet_url(self) -> str:
        """Return shareable URL."""
        if self.spreadsheet_id:
            return f"https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}"
        return None


# ── CLI Test ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    gs = GoogleSheetsOutput()
    if gs.service:
        spreadsheet_id = gs.create_spreadsheet("Prospects Pipeline Test")
        print(f"📊 Spreadsheet URL: {gs.get_spreadsheet_url()}")
