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
import firebase_admin
from firebase_admin import credentials, firestore
import json
import re
import traceback
from datetime import datetime, timezone
from io import BytesIO
import os

try:
    from pypdf import PdfReader
except ImportError:  # pragma: no cover
    PdfReader = None

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
                "internationalName": clean_tc_name(event_name),
                "category": clean_gdacs_category(severity.get("severitytext", ""), parse_wind_speed(severity)),
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
    # Strip GDACS numeric suffixes like "-26" or "_07"
    name = re.sub(r'[-_]\d+$', '', name).strip()
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


def clean_gdacs_category(severity_text, wind_kph=0):
    """Convert raw GDACS severity text to a clean category name."""
    st = (severity_text or "").lower()
    if "super typhoon" in st or wind_kph >= 185:
        return "Super Typhoon"
    if "typhoon" in st or "hurricane" in st or wind_kph >= 118:
        return "Typhoon"
    if "severe tropical storm" in st or wind_kph >= 89:
        return "Severe Tropical Storm"
    if "tropical storm" in st or wind_kph >= 62:
        return "Tropical Storm"
    if "tropical depression" in st or wind_kph > 0:
        return "Tropical Depression"
    return "Tropical Cyclone"


# ════════════════════════════════════════════════════════════
#  SOURCE 2: PAGASA scraper (enrichment — best-effort)
# ════════════════════════════════════════════════════════════

PAGASA_BASE = "https://www.pagasa.dost.gov.ph"
# Open directory index of every Tropical Cyclone Bulletin PDF, with timestamps.
# This is PAGASA's canonical, live source. The public-facing bulletin page is
# JavaScript-rendered (invisible to scrapers) and the legacy "latest" mirrors
# (tamss/weather/bulletin.pdf, tcadvisory.pdf) are frozen on old storms — so we
# read the directory index and parse the actual bulletin PDFs instead.
PAGASA_BULLETIN_INDEX = "https://pubfiles.pagasa.dost.gov.ph/tamss/weather/bulletin/"

# Filenames look like: TCB#10_francisco.pdf  (the '#' is URL-encoded as %23)
_BULLETIN_HREF_RE = re.compile(r'TCB%23(\d+)_([A-Za-z]+)\.pdf', re.IGNORECASE)
_INDEX_DATE_RE = re.compile(r'(\d{2}-[A-Za-z]{3}-\d{4}\s+\d{2}:\d{2})')


def _fetch_pdf_text(url):
    """Download a PDF and extract all text."""
    if PdfReader is None:
        raise RuntimeError("pypdf not installed")
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    reader = PdfReader(BytesIO(resp.content))
    return "\n".join((page.extract_text() or "") for page in reader.pages)


def list_active_bulletins(max_age_hours=30):
    """
    Read PAGASA's bulletin directory index and return the latest bulletin PDF
    per *active* storm (one modified within max_age_hours).

    Returns: list of {"name", "number", "url", "modified"} sorted by recency.
    """
    resp = requests.get(PAGASA_BULLETIN_INDEX, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    html = resp.text
    now = datetime.now(timezone.utc)

    # Parse the autoindex line by line so each filename pairs with its own date.
    latest = {}  # storm -> (number, url, datetime|None)
    for line in re.split(r'<br\s*/?>|\n', html):
        fm = _BULLETIN_HREF_RE.search(line)
        if not fm:
            continue
        num = int(fm.group(1))
        storm = fm.group(2).lower()
        if storm == "unknown":
            continue
        dm = _INDEX_DATE_RE.search(line)
        dt = None
        if dm:
            try:
                dt = datetime.strptime(dm.group(1), "%d-%b-%Y %H:%M").replace(tzinfo=timezone.utc)
            except ValueError:
                dt = None
        # Skip storms whose latest bulletin is clearly stale (old, dissipated)
        if dt is not None and (now - dt).total_seconds() > max_age_hours * 3600:
            continue
        url = PAGASA_BULLETIN_INDEX + f"TCB%23{num}_{storm}.pdf"
        if storm not in latest or num > latest[storm][0]:
            latest[storm] = (num, url, dt)

    bulletins = [
        {"name": s.title(), "number": n, "url": u,
         "modified": (d.isoformat() if d else "")}
        for s, (n, u, d) in latest.items()
    ]
    bulletins.sort(key=lambda b: b["modified"], reverse=True)
    return bulletins


def enrich_with_pagasa(typhoons):
    """
    Authoritative enrichment from PAGASA Tropical Cyclone Bulletin PDFs:
    TCWS signal levels, official intensity, category, position, movement.
    Overlays GDACS data (which lags badly on intensity and has no signals).
    Non-fatal: on any failure, typhoons retain their GDACS data.
    """
    print("\n[PAGASA] Reading bulletin directory index...")
    try:
        bulletins = list_active_bulletins()
        if not bulletins:
            print("  No recent PAGASA bulletins in the index.")
            return

        for b in bulletins:
            print(f"  Active bulletin: {b['name']} TCB#{b['number']} ({b['modified'] or 'no date'})")
            try:
                text = _fetch_pdf_text(b["url"])
            except Exception as e:
                print(f"    ✖ Could not fetch/parse PDF: {e}")
                continue

            parsed = parse_bulletin_pdf(text)
            pname = parsed.get("name") or b["name"]
            if not is_valid_typhoon_name(pname):
                print(f"    Skipping invalid name: '{pname}'")
                continue
            parsed["name"] = pname

            matched = match_typhoon(typhoons, pname, parsed.get("location"))
            if matched:
                _overlay_pagasa(matched, parsed, b)
                print(f"    ✔ Enriched '{pname}' — {matched.get('category')} "
                      f"{matched.get('windSpeedKph')} km/h, highest signal "
                      f"{parsed.get('highestSignal', 0)}")
            else:
                print(f"    + Adding PAGASA-only storm: {pname}")
                typhoons.append(_build_pagasa_typhoon(parsed, b))

    except Exception as e:
        print(f"  [PAGASA] Enrichment failed (non-fatal): {e}")
        traceback.print_exc()


# Fields copied from a parsed bulletin onto a matched GDACS typhoon.
_PAGASA_OVERLAY_FIELDS = (
    "internationalName", "category", "windSpeedKph", "gustinessKph",
    "pressureHpa", "movementDirection", "movementSpeedKph",
    "signalLevels", "highestSignal", "headline", "bulletinNumber",
    "windExtentKm", "forecastOutlook",
)


def _overlay_pagasa(typhoon, parsed, bulletin):
    """Overlay authoritative PAGASA fields onto a GDACS-sourced typhoon."""
    typhoon["name"] = parsed["name"]
    for k in _PAGASA_OVERLAY_FIELDS:
        v = parsed.get(k)
        if v not in (None, "", [], {}):
            typhoon[k] = v
    if parsed.get("location"):
        typhoon["currentLocation"] = parsed["location"]
    # Always set highestSignal so the UI never shows a stale/blank signal.
    typhoon["highestSignal"] = parsed.get("highestSignal", 0)
    typhoon.setdefault("signalLevels", parsed.get("signalLevels",
                       {"1": [], "2": [], "3": [], "4": [], "5": []}))
    typhoon["sourceUrl"] = bulletin["url"]
    typhoon["pagasaIssued"] = parsed.get("issued", "")
    typhoon["pagasaBulletin"] = bulletin["number"]
    typhoon["source"] = "GDACS+PAGASA"
    typhoon["lastUpdated"] = datetime.now(timezone.utc).isoformat()


def _build_pagasa_typhoon(parsed, bulletin):
    """Construct a typhoon document from a PAGASA bulletin alone (GDACS missed it)."""
    sig = parsed.get("highestSignal", 0)
    alert = "Red" if sig >= 4 else "Orange" if sig >= 1 else "Green"
    return {
        "name": parsed["name"],
        "internationalName": parsed.get("internationalName", ""),
        "category": parsed.get("category", ""),
        "isActive": True,
        "lastUpdated": datetime.now(timezone.utc).isoformat(),
        "currentLocation": parsed.get("location", {"lat": None, "lon": None}),
        "windSpeedKph": parsed.get("windSpeedKph", 0),
        "gustinessKph": parsed.get("gustinessKph", 0),
        "pressureHpa": parsed.get("pressureHpa", 0),
        "movementDirection": parsed.get("movementDirection", ""),
        "movementSpeedKph": parsed.get("movementSpeedKph", 0),
        "windExtentKm": parsed.get("windExtentKm", 0),
        "forecastOutlook": parsed.get("forecastOutlook", ""),
        "forecastTrack": [],
        "signalLevels": parsed.get("signalLevels", {"1": [], "2": [], "3": [], "4": [], "5": []}),
        "highestSignal": sig,
        "headline": parsed.get("headline", ""),
        "bulletinNumber": parsed.get("bulletinNumber"),
        "pagasaIssued": parsed.get("issued", ""),
        "alertLevel": alert,
        "sourceUrl": bulletin["url"],
        "source": "PAGASA",
    }


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


def parse_bulletin_pdf(text):
    """
    Parse a PAGASA Tropical Cyclone Bulletin PDF (extracted text) into a dict:
    name, internationalName, category, issued, headline, location, windSpeedKph,
    gustinessKph, pressureHpa, movementDirection, movementSpeedKph,
    signalLevels {1..5: [areas]}, highestSignal, bulletinNumber.

    Whitespace-tolerant: works across PDF text extractors that may collapse or
    insert newlines differently.
    """
    out = {}

    # Bulletin number
    m = re.search(r'BULLETIN\s+NR\.?\s*(\d+)', text, re.I)
    if m:
        out["bulletinNumber"] = int(m.group(1))

    # Name + category + international name, e.g. "Super Typhoon FRANCISCO (MEKKHALA)"
    m = re.search(
        r'(Super Typhoon|Typhoon|Severe Tropical Storm|Tropical Storm|Tropical Depression)\s+'
        r'([A-Z][A-Z\-]+)\s*\(([A-Z][A-Z\-]+)\)',
        text)
    if m:
        out["category"] = m.group(1).title()
        out["name"] = m.group(2).title()
        out["internationalName"] = m.group(3).title()
    else:
        # No international name in parens (e.g. locally-formed storm)
        m = re.search(
            r'(Super Typhoon|Typhoon|Severe Tropical Storm|Tropical Storm|Tropical Depression)\s+'
            r'([A-Z][A-Z\-]{2,})', text)
        if m:
            out["category"] = m.group(1).title()
            out["name"] = m.group(2).title()

    # Issued timestamp
    m = re.search(r'Issued at\s+([\d:]+\s*[AP]M),\s*(\d{1,2}\s+\w+\s+\d{4})', text)
    if m:
        out["issued"] = f"{m.group(1)}, {m.group(2)}"

    # Headline (the all-caps sentence right before "Location of Center")
    m = re.search(r'Page 1 of \d+\s+(.+?)\s+Location of Center', text, re.S)
    if m:
        out["headline"] = re.sub(r'\s+', ' ', m.group(1)).strip().strip('"\u201c\u201d')

    # Center location — first (lat°N, lon°E) pair
    m = re.search(r'([\d.]+)\s*°?\s*N,\s*([\d.]+)\s*°?\s*E', text)
    if m:
        try:
            out["location"] = {"lat": float(m.group(1)), "lon": float(m.group(2))}
        except ValueError:
            pass

    # Intensity
    m = re.search(r'(?:Maximum\s+)?sustained\s+winds\s+of\s+(\d{2,3})\s*km/h', text, re.I)
    if m:
        out["windSpeedKph"] = int(m.group(1))
    m = re.search(r'gustiness\s+of\s+up\s+to\s+(\d{2,3})\s*km/h', text, re.I)
    if m:
        out["gustinessKph"] = int(m.group(1))
    m = re.search(r'central\s+pressure\s+of\s+(\d{3,4})\s*hPa', text, re.I)
    if m:
        out["pressureHpa"] = int(m.group(1))

    # Movement (line after "Present Movement")
    m = re.search(r'Present Movement\s+(.+?)(?:\s*Extent|\s*TROPICAL|\s*TRACK)', text, re.S)
    if m:
        mv = re.sub(r'\s+', ' ', m.group(1)).strip()
        sm = re.match(r'(.+?)\s+at\s+(\d+)\s*km/h', mv)
        if sm:
            out["movementDirection"] = sm.group(1).strip()
            out["movementSpeedKph"] = int(sm.group(2))
        else:
            out["movementDirection"] = mv  # e.g. "West northwestward Slowly"

    # Extent of tropical-cyclone winds (km from center)
    m = re.search(r'winds?\s+extend\s+outwards?\s+up\s+to\s+(\d+)\s*km', text, re.I)
    if m:
        out["windExtentKm"] = int(m.group(1))

    # Forecast narrative ("TRACK AND INTENSITY OUTLOOK"), boilerplate stripped
    m = re.search(r'TRACK AND INTENSITY OUTLOOK\s+(.+)', text, re.S | re.I)
    if m:
        outlook = re.sub(r'\s+', ' ', m.group(1)).strip()
        sentences = re.split(r'(?<=\.)\s+', outlook)
        kept = [s for s in sentences
                if not re.search(r'confidence cone|must be emphasized|Other Hazards', s, re.I)]
        outlook = " ".join(kept).strip()
        if outlook:
            out["forecastOutlook"] = outlook[:900]

    # TCWS signal levels
    signals = {"1": [], "2": [], "3": [], "4": [], "5": []}
    parts = re.split(r'\(TCWS\)\s+IN\s+EFFECT', text, maxsplit=1, flags=re.I)
    if len(parts) > 1:
        body = re.split(r'OTHER\s+HAZARDS', parts[1], maxsplit=1, flags=re.I)[0]
        for i in range(1, 6):
            # A standalone signal number, then "Wind threat: ... winds", then the
            # Luzon-column area list, ending at "- -" or "Warning lead time".
            m = re.search(
                rf'(?<!\d){i}\s+Wind threat:.*?winds\s+(.*?)(?:-\s*-|Warning lead time)',
                body, re.S | re.I)
            if m:
                area_text = re.sub(r'\s+', ' ', m.group(1)).strip()
                # Split on commas that are NOT inside parentheses
                pieces = re.split(r',(?![^(]*\))', area_text)
                areas = [p.strip(' \t\n\r-') for p in pieces
                         if len(p.strip(' \t\n\r-')) > 2]
                signals[str(i)] = areas
    out["signalLevels"] = signals
    out["highestSignal"] = max((int(k) for k, v in signals.items() if v), default=0)

    return out


def match_typhoon(typhoons, pagasa_name, pagasa_location=None):
    """Match a PAGASA name to a GDACS typhoon (fuzzy name match + coordinate proximity)."""
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

    # Fallback: coordinate proximity match (same storm, different names)
    # e.g., GDACS "Mekkhala" at 16.5°N = PAGASA "Francisco" at 16.1°N
    if pagasa_location and pagasa_location.get("lat") and pagasa_location.get("lon"):
        plat, plon = pagasa_location["lat"], pagasa_location["lon"]
        for t in typhoons:
            tloc = t.get("currentLocation", {})
            tlat, tlon = tloc.get("lat"), tloc.get("lon")
            if tlat and tlon:
                # Within ~3 degrees (~330 km) — generous to account for
                # time lag between GDACS and PAGASA position reports
                if abs(plat - tlat) < 3.0 and abs(plon - tlon) < 3.0:
                    print(f"  ⚡ Coordinate match: PAGASA '{pagasa_name}' ≈ GDACS '{t['name']}' "
                          f"({plat:.1f}°N,{plon:.1f}°E ≈ {tlat:.1f}°N,{tlon:.1f}°E)")
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
