"""
Script to fetch Brazilian FII (REIT) price history from Investidor10
and dividend history from Fundamentus, then save to Firebase.

Saves to `fiiHistory/` path — does NOT touch `stockHistory/`.
"""
import os
import sys
import time
import re
from datetime import datetime
from html.parser import HTMLParser
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import firebase_admin
from firebase_admin import credentials, db

REQUEST_TIMEOUT = 20
MAX_WORKERS = 5
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/json",
}


def init_firebase():
    """Initialize Firebase Admin SDK."""
    firebase_admin.initialize_app(
        credentials.Certificate({
            "type": "service_account",
            "project_id": os.environ["FIREBASE_PROJECT_ID"],
            "private_key": os.environ["FIREBASE_PRIVATE_KEY"].replace('\\n', '\n'),
            "client_email": os.environ["FIREBASE_CLIENT_EMAIL"],
            "token_uri": "https://oauth2.googleapis.com/token",
        }), {
            'databaseURL': os.environ["FIREBASE_DATABASE_URL"]
        })


def get_fii_company_id(ticker):
    """Extract Investidor10 companyId from FII page."""
    try:
        r = requests.get(
            f"https://investidor10.com.br/fiis/{ticker.lower()}/",
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT,
        )
        if r.status_code != 200:
            return None

        matches = re.findall(r'cotacoes/chart/(\d+)', r.text)
        if matches:
            return int(matches[0])
        return None
    except Exception:
        return None


def fetch_fii_history(company_id):
    """Fetch 6 years of daily prices from Investidor10 FII API."""
    try:
        r = requests.get(
            f"https://investidor10.com.br/api/fii/cotacoes/chart/{company_id}/2190/",
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT,
        )
        if r.status_code != 200:
            return []

        data = r.json()
        raw = data.get("real", [])
        if not raw:
            return []

        prices = []
        for p in raw:
            date_str = p["created_at"].split(" ")[0]
            try:
                dt = datetime.strptime(date_str, "%d/%m/%Y")
                prices.append({"date": dt.strftime("%Y-%m-%d"), "price": p["price"]})
            except ValueError:
                continue
        return prices
    except Exception:
        return []


def fetch_dividends(company_id):
    """Fetch dividend history from Investidor10 FII dividends API."""
    try:
        r = requests.get(
            f"https://investidor10.com.br/api/fii/dividendos/chart/{company_id}/",
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT,
        )
        if r.status_code != 200:
            return {}

        data = r.json()
        if not isinstance(data, list):
            return {}

        by_month = {}
        for item in data:
            try:
                value = item.get("price", 0)
                created = item.get("created_at", "")  # "MM/YYYY"
                parts = created.split("/")
                if len(parts) == 2:
                    month_key = f"{parts[1]}-{parts[0]}"  # "YYYY-MM"
                    by_month[month_key] = by_month.get(month_key, 0) + value
            except (ValueError, IndexError):
                continue

        return by_month
    except Exception:
        return {}


def group_daily_to_monthly(daily_prices, dividends_by_month=None):
    """Group daily prices into monthly OHLCV + dividends."""
    if dividends_by_month is None:
        dividends_by_month = {}

    monthly = {}
    for entry in daily_prices:
        month_key = entry["date"][:7]
        price = entry["price"]

        if month_key not in monthly:
            monthly[month_key] = {
                "date": entry["date"],
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 0,
                "dividend": round(dividends_by_month.get(month_key, 0), 4),
            }
        else:
            m = monthly[month_key]
            m["high"] = max(m["high"], price)
            m["low"] = min(m["low"], price)
            m["close"] = price
            m["date"] = entry["date"]

    result = list(monthly.values())
    result.sort(key=lambda x: x["date"], reverse=True)
    return result


def diff_month(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month


def get_variation_months(hist, to_month):
    if len(hist) - 1 < to_month:
        return 0
    last = hist[0]["close"]
    first = hist[to_month - 1]["close"]
    last_date = datetime.strptime(hist[0]["date"], "%Y-%m-%d")
    if diff_month(datetime.today(), last_date) > 1 or first == 0:
        return 0
    if first > last:
        return ((first / last - 1) * 100) * -1
    return (last / first - 1) * 100


def process_fii(ticker):
    """Fetch history and dividends for a FII."""
    # Get company ID from Investidor10
    company_id = get_fii_company_id(ticker)
    if not company_id:
        return ticker, None

    # Fetch price history
    daily = fetch_fii_history(company_id)
    if not daily:
        return ticker, None

    # Fetch dividends from Investidor10
    dividends = fetch_dividends(company_id)

    # Group into monthly data
    monthly = group_daily_to_monthly(daily, dividends)
    return ticker, monthly


if __name__ == "__main__":
    try:
        print("Step 1: Initializing Firebase...")
        sys.stdout.flush()
        init_firebase()

        print("Step 2: Fetching FII list from Firebase (fiiFundamentus/)...")
        sys.stdout.flush()
        fiis = db.reference("fiiFundamentus").get()
        if not fiis:
            print("  No FIIs found in fiiFundamentus/. Run get_fii_fundamentus.py first.")
            sys.exit(1)

        fii_codes = list(fiis.keys())
        print(f"  Found {len(fii_codes)} FIIs")
        sys.stdout.flush()

        print(f"Step 3: Fetching history from Investidor10 + dividends from Fundamentus ({MAX_WORKERS} workers)...")
        sys.stdout.flush()

        # IMPORTANT: Save to fiiHistory/ — NOT stockHistory/
        fii_history_ref = db.reference("fiiHistory")
        success_count = 0
        error_list = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_fii, code): code for code in fii_codes}

            for i, future in enumerate(as_completed(futures)):
                code = futures[future]
                try:
                    fii_code, historical = future.result(timeout=120)
                    if historical:
                        var12 = get_variation_months(historical, 12)
                        var6 = get_variation_months(historical, 6)

                        fii_history_ref.child(fii_code).set({
                            "historical": historical,
                            "variationTwelveMonths": round(var12, 2),
                            "variationSixMonths": round(var6, 2),
                            "volumeInLastMonth": 0,
                        })
                        success_count += 1
                except Exception as e:
                    error_list.append(f"{code}: {e}")

                if (i + 1) % 50 == 0 or (i + 1) == len(fii_codes):
                    print(f"  Progress: {i + 1}/{len(fii_codes)} ({success_count} saved, {len(error_list)} errors)")
                    sys.stdout.flush()

        skipped = len(fii_codes) - success_count - len(error_list)
        print(f"\nDone! {success_count} saved, {skipped} skipped (no data), {len(error_list)} errors")
        if error_list:
            print(f"  Errors: {', '.join(error_list[:10])}")
        sys.stdout.flush()

    except Exception as e:
        print(f"FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
