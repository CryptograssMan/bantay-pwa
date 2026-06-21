"""
SAFE ka ba? — Typhoon Data Pipeline (Cloud Run)

Multi-source architecture:
  PRIMARY:    GDACS API  → position, wind, track, severity (reliable JSON)
  ENRICHMENT: PAGASA scraper → TCWS signal levels, local area names (best-effort)

Writes to Firestore: artifacts/{APP_ID}/public/data/typhoon-alerts
Does NOT touch earthquake collections (live-alerts, national-alerts).

Entry point: scrape_typhoons(request)
Deploy: Cloud Run + Cloud Scheduler (every 15 min)
"""

import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, firestore
import json
import re
import traceback
from datetime import datetime, timezone
import os

# ── Firebase Init ───────────────────────────────────────────

try:
    if not firebase_admin._apps:
        cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred)
except Exception:
    if not firebase_admin._apps:
        if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
            firebase_admin.initialize_app(credentials.ApplicationDefault())
        else:
            firebase_admin.initialize_app(credentials.Certificate('serviceAccountKey.json'))

db = firestore.client()
APP_ID = 'bantay-pwa-live'
TYPHOON_COLLECTION = f'artifacts/{APP_ID}/public/data/typhoon-alerts'

# Philippines bounding box (generous to catch approaching storms)
PH_LAT_MIN, PH_LAT_MAX = 2.0, 28.0
PH_LON_MIN, PH_LON_MAX = 110.0, 140.0

HEADERS = {
    'User-Agent': 'SAFE-ka-ba/1.0 (+https://app.safekaba.com)',
    'Accept': 'application/json, text/html',
}

# ════════════════════════════════════════════════════════════
#  SOURCE 1: GDACS API (primary — reliable JSON)
# ════════════════════════════════════════════════════════════

GDACS_EVENTS_URL = "https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH"
GDACS_EVENT_URL = "https://www.gdacs.org/gdacsapi/api/events/geteventdata"

def fetch_gdacs_cyclones():
    """Fetch active tropical cyclones from GDACS near the Philippines."""
    typhoons = []
    try:
        params = {
            "eventtype": "TC",
            "alertlevel": "Green;Orange;Red",
            # NOTE: Do NOT use "country": "PHL" — GDACS tags storms as affecting
            # PHL only when they're very close or signals are raised. Our own
            # geo bounding box (PH_LAT/LON_MIN/MAX) handles proximity filtering.
        }
        resp = requests.get(GDACS_EVENTS_URL, params=params, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        data = resp.json()

        features = data.get("features", [])
        print(f"[GDACS] Found {len(features)} TC event(s)")

        for feature in features:
            props = feature.get("properties", {})
            geo = feature.get("geometry", {})
            coords = geo.get("coordinates", [None, None])

            # coords are [lon, lat] in GeoJSON
            lon = coords[0] if coords[0] else None
            lat = coords[1] if coords[1] else None

            # Filter to Philippines region
            if lat and lon:
                if not (PH_LAT_MIN <= lat <= PH_LAT_MAX and PH_LON_MIN <= lon <= PH_LON_MAX):
                    print(f"  Skipping {props.get('eventname', '?')} — outside PH region ({lat}, {lon})")
                    continue

            is_current = props.get("iscurrent", False)
            if not is_current:
                continue

            event_id = str(props.get("eventid", ""))
            event_name = props.get("eventname", "Unknown")
            alert_level = props.get("alertlevel", "")
            severity = props.get("severitydata", {})

            typhoon = {
                "gdacsId": event_id,
                "name": clean_tc_name(event_name),
                "internationalName": "",
                "category": severity.get("severitytext", "Tropical Cyclone"),
                "isActive": True,
                "lastUpdated": datetime.now(timezone.utc).isoformat(),
                "currentLocation": {"lat": lat, "lon": lon},
                "windSpeedKph": parse_wind_speed(severity),
                "gustinessKph": 0,
                "movementDirection": "",
                "movementSpeedKph": 0,
                "forecastTrack": [],
                "signalLevels": {"1": [], "2": [], "3": [], "4": [], "5": []},
                "rainfallWarning": "",
                "stormSurgeWarning": "",
                "alertLevel": alert_level,
                "sourceUrl": props.get("url", {}).get("report", ""),
                "source": "GDACS",
            }

            # Try to get detailed event data (track, etc.)
            detail = fetch_gdacs_event_detail(event_id)
            if detail:
                typhoon["forecastTrack"] = detail.get("track", [])
                if detail.get("windKph"):
                    typhoon["windSpeedKph"] = detail["windKph"]

            typhoons.append(typhoon)
            print(f"  ✔ {typhoon['name']} | {lat:.1f}°N {lon:.1f}°E | Wind: {typhoon['windSpeedKph']} km/h | Alert: {alert_level}")

    except requests.RequestException as e:
        print(f"[GDACS] Network error: {e}")
    except Exception as e:
        print(f"[GDACS] Error: {e}")
        traceback.print_exc()

    return typhoons


def fetch_gdacs_event_detail(event_id):
    """Fetch detailed data for a single GDACS TC event (track points, etc.)."""
    try:
        params = {"eventtype": "TC", "eventid": event_id}
        resp = requests.get(GDACS_EVENT_URL, params=params, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        result = {"track": [], "windKph": 0}

        # Extract track from GeoJSON features
        features = data.get("features", [])
        for feat in features:
            geo = feat.get("geometry", {})
            props = feat.get("properties", {})
            if geo.get("type") == "Point" and props.get("isforecast"):
                coords = geo.get("coordinates", [])
                if len(coords) >= 2:
                    result["track"].append({
                        "hoursAhead": props.get("forecasthour", 0),
                        "lat": coords[1],
                        "lon": coords[0],
                    })

        result["track"].sort(key=lambda x: x["hoursAhead"])
        return result

    except Exception as e:
        print(f"  [GDACS detail] Could not fetch event {event_id}: {e}")
        return None


def clean_tc_name(raw_name):
    """Extract a clean typhoon name from GDACS event name."""
    # GDACS names like "Tropical Cyclone CARINA" or "Typhoon JULIAN (KRATHON)"
    raw = raw_name.strip()
    # Remove prefix
    for prefix in ["Super Typhoon", "Typhoon", "Tropical Storm", "Tropical Depression", "Tropical Cyclone"]:
        if raw.lower().startswith(prefix.lower()):
            raw = raw[len(prefix):].strip()
            break
    # Take first word (Filipino name), strip parens
    parts = raw.split("(")
    name = parts[0].strip().strip('"').strip("'").title()
    return name if name else "Unknown"


def parse_wind_speed(severity_data):
    """Extract wind speed in km/h from GDACS severity data."""
    try:
        val = severity_data.get("severity", 0)
        unit = severity_data.get("severityunit", "").lower()
        if "km" in unit:
            return int(float(val))
        elif "kt" in unit or "knot" in unit:
            return int(float(val) * 1.852)
        elif "mph" in unit:
            return int(float(val) * 1.609)
        return int(float(val)) if val else 0
    except (ValueError, TypeError):
        return 0


# ════════════════════════════════════════════════════════════
#  SOURCE 2: PAGASA scraper (enrichment — best-effort)
# ════════════════════════════════════════════════════════════

PAGASA_BASE = "https://www.pagasa.dost.gov.ph"
PAGASA_BULLETIN_URL = f"{PAGASA_BASE}/tropical-cyclone/severe-weather-bulletin"


def enrich_with_pagasa(typhoons):
    """
    Best-effort enrichment: scrape PAGASA for TCWS signal levels.
    If scraping fails, typhoons still have GDACS data. Non-fatal.
    """
    print("\n[PAGASA] Attempting signal level enrichment...")
    try:
        resp = requests.get(PAGASA_BULLETIN_URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, 'html.parser')

        # Find bulletin links — STRICT: only actual numbered bulletins, not nav links
        content = soup.find('div', id='content-left-info') or soup
        bulletin_links = []
        for a in content.select('a[href]'):
            href = a.get('href', '')
            text = a.get_text(strip=True).lower()
            # Must be an actual bulletin link (contains a bulletin number or date pattern)
            # Real bulletins: "Severe Weather Bulletin #5" or links with /severe-weather-bulletin/ + sub-path
            is_real_bulletin = (
                re.search(r'bulletin\s*#?\s*\d+', text) or
                re.search(r'severe-weather-bulletin/\w+', href) or
                (re.search(r'bulletin.*(?:no|number|#)\s*\d', text, re.IGNORECASE))
            )
            # Reject nav links (they point to category pages, not actual bulletins)
            is_nav_link = (
                href.rstrip('/') == PAGASA_BULLETIN_URL.rstrip('/') or
                '/tropical-cyclone-bulletin' == href.rstrip('/').split('pagasa.dost.gov.ph')[-1] or
                'publications' in href or 'annual-report' in href or
                'preliminary-report' in href or 'agriculture' in href
            )
            if is_real_bulletin and not is_nav_link:
                url = href if href.startswith('http') else f"{PAGASA_BASE}{href}"
                if url not in bulletin_links:
                    bulletin_links.append(url)

        if not bulletin_links:
            # FALLBACK: PAGASA listing page loads bulletin links via JavaScript,
            # so BeautifulSoup can't see them. Try direct URL patterns instead.
            print("  No links found on listing page (JS-rendered). Trying direct URLs...")
            for i in range(1, 4):  # Try up to 3 active typhoon slots
                direct_url = f"{PAGASA_BULLETIN_URL}/{i}"
                try:
                    head = requests.head(direct_url, headers=HEADERS, timeout=10, allow_redirects=True)
                    if head.status_code == 200:
                        bulletin_links.append(direct_url)
                        print(f"    Found active bulletin at {direct_url}")
                except Exception:
                    pass

        if not bulletin_links:
            print("  No active bulletins found on PAGASA.")
            return

        for url in bulletin_links[:3]:
            signals, pagasa_name, extras = parse_pagasa_signals(url)
            if not pagasa_name or pagasa_name == "Unknown":
                continue
            if not is_valid_typhoon_name(pagasa_name):
                print(f"  Skipping invalid name: '{pagasa_name}'")
                continue

            # Match to a GDACS typhoon by name (fuzzy)
            matched = match_typhoon(typhoons, pagasa_name)
            if matched:
                matched["signalLevels"] = signals
                matched["name"] = pagasa_name  # Use Filipino name
                if extras.get("internationalName") and is_valid_typhoon_name(extras["internationalName"]):
                    matched["internationalName"] = extras["internationalName"]
                if extras.get("rainfallWarning"):
                    matched["rainfallWarning"] = extras["rainfallWarning"]
                if extras.get("stormSurgeWarning"):
                    matched["stormSurgeWarning"] = extras["stormSurgeWarning"]
                matched["sourceUrl"] = url
                print(f"  ✔ Enriched '{matched['name']}' with TCWS signals")
            else:
                # PAGASA has a typhoon GDACS doesn't — add it (only if valid)
                if extras.get("windSpeedKph", 0) > 0 or any(signals[s] for s in signals):
                    print(f"  + Adding PAGASA-only typhoon: {pagasa_name}")
                    typhoons.append({
                        "name": pagasa_name,
                        "internationalName": extras.get("internationalName", ""),
                        "category": extras.get("category", ""),
                        "isActive": True,
                        "lastUpdated": datetime.now(timezone.utc).isoformat(),
                        "currentLocation": extras.get("location", {"lat": None, "lon": None}),
                        "windSpeedKph": extras.get("windSpeedKph", 0),
                        "gustinessKph": extras.get("gustinessKph", 0),
                        "movementDirection": extras.get("movementDirection", ""),
                        "movementSpeedKph": 0,
                        "forecastTrack": [],
                        "signalLevels": signals,
                        "rainfallWarning": extras.get("rainfallWarning", ""),
                        "stormSurgeWarning": extras.get("stormSurgeWarning", ""),
                        "alertLevel": "Orange",
                        "sourceUrl": url,
                        "source": "PAGASA",
                    })
                else:
                    print(f"  Skipping '{pagasa_name}' — no signal data or wind speed")

    except Exception as e:
        print(f"  [PAGASA] Enrichment failed (non-fatal): {e}")


def is_valid_typhoon_name(name):
    """Reject names that are clearly nav text, common words, or too short."""
    if not name or len(name) < 3:
        return False
    invalid_names = {
        'and', 'the', 'for', 'about', 'climate', 'annual', 'report', 'forecast',
        'warning', 'advisory', 'bulletin', 'tropical', 'cyclone', 'typhoon',
        'storm', 'depression', 'publications', 'monitoring', 'information',
        'habagat', 'amihan', 'monsoon', 'outlook', 'weather', 'marine',
        'aviation', 'general', 'daily', 'weekly', 'monthly', 'associated',
        'agriculture', 'preliminary', 'potential', 'temperature', 'rainfall',
    }
    return name.lower().strip() not in invalid_names


# Known PAGASA navigation text that pollutes scraping results
PAGASA_NAV_JUNK = [
    "Tropical Cyclone Publications", "Annual Report on Philippine Tropical Cyclones",
    "Tropical Cyclone Preliminary Report", "About Tropical Cyclone",
    "Climate Monitoring", "Daily Rainfall and Temperature",
    "Tropical Cyclone Warning for Agriculture", "TC-Threat Potential Forecast",
    "Tropical Cyclone Associated Rainfall", "Tropical Cyclone Advisory",
    "Tropical Cyclone Bulletin", "Warning for Shipping", "Forecast Storm Surge",
    "Weather Advisory", "Aviation", "SIGMET", "METAR", "Terminal Aerodome",
    "Marine", "High Seas", "Gale Warning", "Heat Index", "Flood Information",
    "Dam Information", "Hydromet Data", "Sub Seasonal",
]


def strip_pagasa_nav_text(text):
    """Remove known PAGASA navigation menu text from scraped content."""
    for junk in PAGASA_NAV_JUNK:
        text = text.replace(junk, "")
    # Also strip repeated menu blocks (PAGASA renders nav twice)
    lines = text.split("\n")
    seen = set()
    cleaned = []
    for line in lines:
        stripped = line.strip()
        if stripped and stripped not in seen and len(stripped) > 5:
            cleaned.append(line)
            seen.add(stripped)
    return "\n".join(cleaned)


def parse_pagasa_signals(url):
    """Parse TCWS signal levels from a PAGASA bulletin page."""
    signals = {"1": [], "2": [], "3": [], "4": [], "5": []}
    name = "Unknown"
    extras = {}

    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, 'html.parser')

        # Remove nav elements before extracting text
        content = soup.find('div', id='content-left-info') or soup
        for nav in content.find_all(['nav', 'header', 'footer']):
            nav.decompose()
        for menu in content.find_all(class_=re.compile(r'menu|nav|sidebar|breadcrumb', re.IGNORECASE)):
            menu.decompose()

        text = content.get_text("\n", strip=True)
        text = strip_pagasa_nav_text(text)

        # Verify this page has actual bulletin content (not just a nav page)
        has_bulletin_content = bool(re.search(
            r'BULLETIN\s*(?:NO|#|NUMBER)\s*\.?\s*\d+|TCWS|Wind\s+Signal|sustained\s+winds',
            text, re.IGNORECASE
        ))
        if not has_bulletin_content:
            print(f"  [PAGASA parse] No bulletin content found at {url}")
            return signals, "Unknown", extras

        # Name
        for pattern in [
            r'(?:Super\s+)?(?:Typhoon|Tropical\s+(?:Storm|Depression))\s+["\u201c]?([A-Z][A-Za-z]+)["\u201d]?',
            r'Bagyong\s+"?([A-Z][A-Za-z]+)"?',
        ]:
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                name = m.group(1).strip().title()
                break

        # International name
        m = re.search(r'\(([A-Z][a-z]+)\)', text)
        if m and is_valid_typhoon_name(m.group(1)):
            extras["internationalName"] = m.group(1).title()

        # Category
        m = re.search(r'(Super\s+Typhoon|Typhoon|Severe\s+Tropical\s+Storm|Tropical\s+Storm|Tropical\s+Depression)', text, re.IGNORECASE)
        if m:
            extras["category"] = m.group(1).title()

        # Position
        m = re.search(r'(\d{1,2}\.?\d*)\s*[°]?\s*N[,;\s]+(\d{2,3}\.?\d*)\s*[°]?\s*E', text)
        if m:
            extras["location"] = {"lat": float(m.group(1)), "lon": float(m.group(2))}

        # Wind
        m = re.search(r'sustained\s+winds\s+(?:of\s+)?(?:up\s+to\s+)?(\d{2,3})\s*km', text, re.IGNORECASE)
        if m:
            extras["windSpeedKph"] = int(m.group(1))
        m = re.search(r'gust(?:iness|s)?\s+(?:of\s+)?(?:up\s+to\s+)?(\d{2,3})\s*km', text, re.IGNORECASE)
        if m:
            extras["gustinessKph"] = int(m.group(1))

        # Movement
        m = re.search(r'mov(?:ing|ement)\s+((?:North|South|East|West|Stationary)[\w\s]*?)(?:\s+at\s+(\d+)\s*km)?', text, re.IGNORECASE)
        if m:
            extras["movementDirection"] = m.group(1).strip().title()

        # Rainfall
        m = re.search(r'(?:RAINFALL)[:\s]*([\s\S]*?)(?=STORM\s+SURGE|FLOODING|TRACK|$)', text, re.IGNORECASE)
        if m:
            warning_text = m.group(1).strip()[:500]
            # Reject if it's just nav menu junk
            if not any(junk.lower() in warning_text.lower() for junk in ["Publications", "Annual Report", "Preliminary Report", "Agriculture"]):
                extras["rainfallWarning"] = warning_text

        # Storm surge
        m = re.search(r'STORM\s+SURGE[:\s]*([\s\S]*?)(?=RAINFALL|TRACK|$)', text, re.IGNORECASE)
        if m:
            warning_text = m.group(1).strip()[:500]
            if not any(junk.lower() in warning_text.lower() for junk in ["Publications", "Annual Report", "Preliminary Report", "Agriculture"]):
                extras["stormSurgeWarning"] = warning_text

        # TCWS Signal levels
        for i in range(5, 0, -1):
            pattern = rf'(?:TCWS|Wind\s+Signal|Signal)\s*(?:No\.?|#)\s*{i}\s*[\n:]([\s\S]*?)(?=(?:TCWS|Wind\s+Signal|Signal)\s*(?:No\.?|#)\s*\d|HAZARDS|TRACK|RAINFALL|STORM|$)'
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                locs = re.split(r'\n|,|;|\band\b', m.group(1))
                cleaned = [l.strip(' \t\n\r-•·') for l in locs if len(l.strip(' \t\n\r-•·')) > 3]
                cleaned = [re.sub(r'^[\d.)\-•·]+\s*', '', l).strip() for l in cleaned]
                cleaned = [l for l in cleaned if len(l) > 3]
                if cleaned:
                    signals[str(i)] = list(set(cleaned))

    except Exception as e:
        print(f"  [PAGASA parse] Error on {url}: {e}")

    return signals, name, extras


def match_typhoon(typhoons, pagasa_name):
    """Match a PAGASA name to a GDACS typhoon (fuzzy name match)."""
    pn = pagasa_name.lower().strip()
    for t in typhoons:
        # Direct match on name or international name
        if pn == t["name"].lower().strip():
            return t
        if t.get("internationalName") and pn == t["internationalName"].lower().strip():
            return t
        # GDACS sometimes uses the international name; PAGASA uses the Filipino name
        # Check if GDACS name contains the PAGASA name or vice versa
        gdacs_name = t["name"].lower()
        if pn in gdacs_name or gdacs_name in pn:
            return t
    return None


# ════════════════════════════════════════════════════════════
#  FIRESTORE OPERATIONS
# ════════════════════════════════════════════════════════════

def write_typhoon(typhoon_data):
    """Upsert a typhoon document. Returns True on success."""
    name = typhoon_data.get("name", "Unknown")
    if name == "Unknown":
        return False
    doc_id = f"TYPHOON_{name.upper().replace(' ', '_')}"
    try:
        db.collection(TYPHOON_COLLECTION).document(doc_id).set(typhoon_data, merge=True)
        return True
    except Exception as e:
        print(f"  ✖ Firestore write failed for {name}: {e}")
        return False


def deactivate_old_typhoons(active_names):
    """Mark typhoons not in active_names as inactive."""
    active_set = {n.lower() for n in active_names}
    try:
        docs = db.collection(TYPHOON_COLLECTION).where('isActive', '==', True).stream()
        for d in docs:
            data = d.to_dict()
            if data.get('name', '').lower() not in active_set:
                d.reference.update({
                    'isActive': False,
                    'deactivatedAt': datetime.now(timezone.utc).isoformat()
                })
                print(f"  ⊘ Deactivated: {data.get('name')}")
    except Exception as e:
        print(f"  Deactivation error: {e}")


# ════════════════════════════════════════════════════════════
#  ENTRY POINT
# ════════════════════════════════════════════════════════════

def scrape_typhoons(request):
    """Cloud Run entry point. Called by Cloud Scheduler every 15 min."""
    print("=" * 55)
    print(f"Typhoon pipeline started — {datetime.now(timezone.utc).isoformat()}")
    print("=" * 55)

    # Step 1: GDACS (primary)
    print("\n[1/3] Fetching from GDACS...")
    typhoons = fetch_gdacs_cyclones()

    # Step 2: PAGASA enrichment (best-effort)
    print("\n[2/3] PAGASA enrichment...")
    enrich_with_pagasa(typhoons)

    # Step 3: Write to Firestore
    print(f"\n[3/3] Writing {len(typhoons)} typhoon(s) to Firestore...")
    written = 0
    active_names = []
    for t in typhoons:
        if write_typhoon(t):
            written += 1
            active_names.append(t["name"])

    deactivate_old_typhoons(active_names)

    result = {"typhoons_written": written, "total_found": len(typhoons)}
    print(f"\nDone. {written}/{len(typhoons)} written.")
    print("=" * 55)
    return (json.dumps(result), 200, {"Content-Type": "application/json"})


# Also expose as `scrape_pagasa` for backward compat with existing Cloud Run config
scrape_pagasa = scrape_typhoons


if __name__ == '__main__':
    class MockRequest:
        headers = {}
        args = {}
        get_json = staticmethod(lambda: {})

    print("--- Local test run ---\n")
    body, status, _ = scrape_typhoons(MockRequest())
    print(f"\nHTTP {status}: {body}")
