import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, firestore
import json
import re
from datetime import datetime, timezone, timedelta
import os # Import the os module

# Initialize Firebase Admin SDK
# For Cloud Run, the service account attached to the function will handle authentication
# You generally don't need a serviceAccountKey.json file during deployment on Cloud Run.
# The code below handles both Cloud Run (ApplicationDefault) and local testing (if GOOGLE_APPLICATION_CREDENTIALS is set)
try:
    if not firebase_admin._apps: # Check if app is already initialized
        # Try to initialize with Application Default Credentials first (for Cloud Run)
        cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred)
        print("Firebase initialized with Application Default Credentials.")
except Exception as e:
    # Fallback for local testing or specific environments
    if not firebase_admin._apps:
        # Check if GOOGLE_APPLICATION_CREDENTIALS environment variable is set
        if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
            print("Firebase initialization fallback: GOOGLE_APPLICATION_CREDENTIALS found.")
            cred = credentials.ApplicationDefault() # Will pick up path from env var
            firebase_admin.initialize_app(cred)
        else:
            print("Firebase initialization fallback: Using local serviceAccountKey.json path (for development only).")
            # IMPORTANT: Replace with the actual path to your serviceAccountKey.json for LOCAL testing only
            # This path is ignored in Cloud Run deployment
            cred = credentials.Certificate('serviceAccountKey.json') 
            firebase_admin.initialize_app(cred)


db = firestore.client()
APP_ID = 'bantay-pwa-live' # Ensure this matches your project ID or desired path

def parse_pagasa_bulletin(url):
    """
    Parses the PAGASA bulletin to extract structured typhoon data.
    """
    # ... (rest of your parse_pagasa_bulletin function remains UNCHANGED) ...
    try:
        response = requests.get(url, timeout=15)
        response.raise_raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        content = soup.find('div', id='content-left-info')
        if not content:
            print("No main content found for parsing.")
            return None
        
        full_text = content.get_text("\n", strip=True)

        typhoon_data = {
            "name": "Unknown",
            "internationalName": "Unknown",
            "isActive": True,
            "lastUpdated": datetime.now(timezone.utc).isoformat(),
            "currentLocation": {"lat": None, "lon": None},
            "windSpeedKph": 0,
            "gustinessKph": 0,
            "movement": "Unknown",
            "track": [],
            "signalLevels": { "1": [], "2": [], "3": [], "4": [], "5": [] },
            "sourceUrl": url
        }

        name_match = re.search(r"TROPICAL CYCLONE BULLETIN NO\. \d+\s+FOR\s+(?:Super\s)?Typhoon\s+\"([A-Z\s]+)\"", full_text, re.IGNORECASE)
        if name_match:
            typhoon_data["name"] = name_match.group(1).strip().title()

        intl_name_match = re.search(r"\(([A-Z\s]+)\)", full_text, re.IGNORECASE)
        if intl_name_match:
            typhoon_data["internationalName"] = intl_name_match.group(1).strip().title()
        
        for i in range(5, 0, -1):
            pattern = re.compile(r"TCWS No\. {}\s*\n([\s\S]*?)(?=(?:\nTCWS No\.)|(?:\nHAZARDS AFFECTING LAND AREAS)|(?:\nTRACK AND INTENSITY OUTLOOK)|$)".format(i), re.IGNORECASE)
            signal_section = pattern.search(full_text)
            
            if signal_section:
                locations_text = signal_section.group(1)
                locations = re.split(r'\s*\n\s*|\s*,\s*|\s+and\s+', locations_text)
                cleaned_locations = [loc.strip(" ,.\n\t-") for loc in locations if loc and len(loc.strip(" ,.\n\t-")) > 3]
                typhoon_data["signalLevels"][str(i)] = list(set(cleaned_locations))

        return typhoon_data

    except requests.RequestException as e:
        print(f"Error fetching URL: {e}")
        return None
    except Exception as e:
        print(f"An error occurred during parsing: {e}")
        return None


def update_firestore(typhoon_data):
    """
    Updates Firestore with the latest typhoon data.
    The document ID is based on the typhoon's name.
    """
    if not typhoon_data or typhoon_data['name'] == 'Unknown':
        print("No valid typhoon data to update.")
        return

    # Use the specific path including 'artifacts/{APP_ID}/public/data'
    doc_id = f"TYPHOON_{typhoon_data['name'].upper()}"
    doc_ref = db.collection(f'artifacts/{APP_ID}/public/data/typhoon-alerts').document(doc_id)
    
    try:
        doc_ref.set(typhoon_data, merge=True)
        print(f"Successfully updated Firestore for typhoon: {typhoon_data['name']} (Doc ID: {doc_id})")
    except Exception as e:
        print(f"Error updating Firestore for {typhoon_data['name']}: {e}")


def scrape_pagasa(request):
    """
    Main Cloud Run Service entry point.
    It scrapes the main PAGASA page to find the latest bulletin link.
    """
    # Request is a Flask request object in Cloud Run
    
    base_url = "https://www.pagasa.dost.gov.ph"
    bulletin_url = f"{base_url}/tropical-cyclone-bulletin"
    
    print("Starting PAGASA scrape job...")
    
    try:
        response = requests.get(bulletin_url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        latest_bulletin_link = soup.select_one("#content-left-info ul > li > a")

        if latest_bulletin_link and latest_bulletin_link['href']:
            full_bulletin_url = f"{base_url}{latest_bulletin_link['href']}"
            print(f"Found latest bulletin: {full_bulletin_url}")
            
            typhoon_data = parse_pagasa_bulletin(full_bulletin_url)
            
            if typhoon_data and typhoon_data['name'] != 'Unknown':
                update_firestore(typhoon_data)
                return ("Scrape successful", 200)
            elif typhoon_data and typhoon_data['name'] == 'Unknown':
                print("Parsed data but typhoon name is 'Unknown', likely inactive or parsing error.")
                # We could add logic here to mark existing typhoons as inactive
                return ("Parsing result: Typhoon name unknown", 200)
            else:
                print("Failed to parse bulletin from the identified link.")
                return ("Parsing failed for specific bulletin", 500)
        else:
            print("No active typhoon bulletin link found on main page. Assuming no active typhoon.")
            # IMPORTANT: Here we need to implement logic to set all existing typhoons in Firestore to isActive: false
            # This ensures old typhoons don't stay on the list.
            # For now, we'll just log this.
            return ("No active bulletin link found, likely no active typhoon", 200)

    except requests.RequestException as e:
        print(f"Error fetching PAGASA page or bulletin: {e}")
        return (f"Failed to fetch PAGASA page: {e}", 500)
    except Exception as e:
        print(f"An unexpected error occurred in scrape_pagasa: {e}")
        return (f"An unexpected error occurred: {e}", 500)


# For local testing, ensure GOOGLE_APPLICATION_CREDENTIALS is set or serviceAccountKey.json is present
if __name__ == '__main__':
    # When running locally, simulate a request object
    class MockRequest:
        def __init__(self):
            self.headers = {}
            self.args = {}
            self.get_json = lambda: {}
    
    print("--- Running scrape_pagasa locally ---")
    scrape_pagasa(MockRequest())
    print("--- Local run finished ---")

