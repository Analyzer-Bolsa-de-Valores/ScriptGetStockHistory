"""
Script to fetch Brazilian stock price history from Yahoo Finance
and save to Firebase Realtime Database.

Replaces the old Alpha Vantage approach (rate limited, required API key)
with yfinance (free, no key needed, batch downloads).
"""
import os
import sys
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import yfinance as yf
import firebase_admin
from firebase_admin import credentials, db


def init_firebase():
    """Initialize Firebase Admin SDK"""
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


def get_stock_history(stock_code):
    """
    Fetch monthly history for a single stock using yfinance.
    Returns dict in the same format as the existing Firebase data.
    """
    try:
        ticker = yf.Ticker(f"{stock_code}.SA")
        hist = ticker.history(period="2y", interval="1mo")

        if hist.empty:
            return None

        historical_doc = []
        for date, row in hist.iterrows():
            historical_doc.append({
                "date": date.strftime("%Y-%m-%d"),
                "open": round(float(row["Open"]), 2),
                "high": round(float(row["High"]), 2),
                "low": round(float(row["Low"]), 2),
                "close": round(float(row["Close"]), 2),
                "volume": int(row["Volume"]),
                "dividend": round(float(row["Dividends"]), 4),
            })

        # Sort by date descending (most recent first) - matches old format
        historical_doc.sort(key=lambda x: x["date"], reverse=True)

        return historical_doc
    except Exception as e:
        print(f"  WARN: Failed to fetch {stock_code}: {e}")
        return None


def diff_month(d1, d2):
    """Get difference between two dates in months"""
    return (d1.year - d2.year) * 12 + d1.month - d2.month


def get_variation_months(historical_doc, to_month):
    """
    Calculate stock price variation over N months.
    Positive = stock went up, negative = stock went down.
    """
    if len(historical_doc) - 1 < to_month:
        return 0

    last_month_value = historical_doc[0]["close"]
    first_month_value = historical_doc[to_month - 1]["close"]

    last_month_date = datetime.strptime(historical_doc[0]["date"], "%Y-%m-%d")
    today_date = datetime.today()

    if diff_month(today_date, last_month_date) > 1:
        return 0

    if first_month_value == 0:
        return 0

    if first_month_value > last_month_value:
        return ((first_month_value / last_month_value - 1) * 100) * -1
    return (last_month_value / first_month_value - 1) * 100


def get_volume_months(historical_doc, to_month):
    """Get total volume over N months"""
    if len(historical_doc) - 1 < to_month:
        return 0

    volume_acum = sum(historical_doc[m]["volume"] for m in range(to_month))

    last_month_date = datetime.strptime(historical_doc[0]["date"], "%Y-%m-%d")
    today_date = datetime.today()

    if diff_month(today_date, last_month_date) > 1:
        return 0

    return volume_acum


def process_stock(stock_code):
    """Process a single stock: fetch history + calculate metrics"""
    historical_doc = get_stock_history(stock_code)

    if not historical_doc:
        return stock_code, None

    variation_twelve = get_variation_months(historical_doc, 12)
    variation_six = get_variation_months(historical_doc, 6)
    volume_last_month = get_volume_months(historical_doc, 1)

    result = {
        "historical": historical_doc,
        "variationTwelveMonths": round(variation_twelve, 2),
        "variationSixMonths": round(variation_six, 2),
        "volumeInLastMonth": volume_last_month,
    }

    return stock_code, result


if __name__ == "__main__":
    try:
        print("Step 1: Initializing Firebase...")
        sys.stdout.flush()
        init_firebase()

        print("Step 2: Fetching stock list from Firebase...")
        sys.stdout.flush()
        stocks = db.reference("stockFundamentus").get()
        stock_codes = list(stocks.keys())
        print(f"  Found {len(stock_codes)} stocks")
        sys.stdout.flush()

        print("Step 3: Fetching history for each stock (parallel)...")
        sys.stdout.flush()

        success_count = 0
        error_list = []
        all_stocks_ref = db.reference("stockHistory")

        # Process in batches with ThreadPool
        MAX_WORKERS = 10
        BATCH_SIZE = 50

        for batch_start in range(0, len(stock_codes), BATCH_SIZE):
            batch = stock_codes[batch_start:batch_start + BATCH_SIZE]

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(process_stock, code): code for code in batch}

                for future in as_completed(futures):
                    code = futures[future]
                    try:
                        stock_code, result = future.result(timeout=60)
                        if result:
                            all_stocks_ref.child(stock_code).set(result)
                            success_count += 1
                        else:
                            error_list.append(stock_code)
                    except Exception as e:
                        error_list.append(code)
                        print(f"  ERROR {code}: {e}")

            processed = min(batch_start + BATCH_SIZE, len(stock_codes))
            print(f"  Progress: {processed}/{len(stock_codes)} ({success_count} ok, {len(error_list)} errors)")
            sys.stdout.flush()

            # Small delay between batches to avoid rate limiting
            if batch_start + BATCH_SIZE < len(stock_codes):
                time.sleep(2)

        print(f"\nDone! {success_count} stocks saved, {len(error_list)} errors")

        if error_list:
            print(f"Failed stocks ({len(error_list)}): {', '.join(error_list[:30])}")

        sys.stdout.flush()

    except Exception as e:
        print(f"FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
