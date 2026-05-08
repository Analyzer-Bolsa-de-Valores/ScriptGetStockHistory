"""
Script to fetch Brazilian stock price history from Investidor10
and save to Firebase Realtime Database.

Investidor10: free, no API key, covers all B3 stocks including small caps.
"""
import os
import sys
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import firebase_admin
from firebase_admin import credentials, db

REQUEST_TIMEOUT = 20
MAX_WORKERS = 5
# Headers de browser real. Fundamentus (Cloudflare) retorna 403 com User-Agent
# incompleto OU com Accept: application/json em endpoints HTML — confirmado via
# debug logs de 2026-05-08 (todas as 578 chamadas retornaram 403). UA completo
# + Accept HTML resolve. Investidor10 aceita ambos sem problema.
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
}
# Headers JSON-only para chamadas à API do Investidor10 (que devolve JSON).
HEADERS_JSON = {
    "User-Agent": HEADERS["User-Agent"],
    "Accept": "application/json",
}


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


def fetch_stock_history(stock_code):
    """Fetch 6 years of daily prices from Investidor10."""
    try:
        r = requests.get(
            f"https://investidor10.com.br/api/cotacoes/acao/chart/{stock_code}/2190/true",
            headers=HEADERS_JSON,
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


def fetch_dividends(stock_code):
    """Fetch dividend history from Fundamentus."""
    # DEBUG: amostra apenas pra alguns stocks evita poluir log com 500 dumps.
    debug = stock_code in ('PETR4', 'ITUB4', 'VALE3', 'BBSE3', 'WEGE3')
    try:
        r = requests.get(
            f"https://www.fundamentus.com.br/proventos.php?papel={stock_code}&tipo=2",
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT,
        )
        if debug:
            print(f"[DEBUG-DIV {stock_code}] status={r.status_code} len={len(r.content)} content-type={r.headers.get('content-type','?')}")
            sys.stdout.flush()
        if r.status_code != 200:
            print(f"[DEBUG-DIV {stock_code}] non-200, returning empty")
            sys.stdout.flush()
            return {}

        from html.parser import HTMLParser

        class DivParser(HTMLParser):
            def __init__(self):
                super().__init__()
                self.in_td = False
                self.current_row = []
                self.rows = []

            def handle_starttag(self, tag, attrs):
                if tag == 'td':
                    self.in_td = True
                elif tag == 'tr':
                    self.current_row = []

            def handle_endtag(self, tag):
                if tag == 'td':
                    self.in_td = False
                elif tag == 'tr' and len(self.current_row) >= 4:
                    self.rows.append(self.current_row)

            def handle_data(self, data):
                if self.in_td:
                    t = data.strip()
                    if t:
                        self.current_row.append(t)

        html = r.content.decode('latin-1')
        if debug:
            has_tbody = '<tbody>' in html
            has_table = '<table' in html
            first_300 = html[:300].replace('\n', ' ')
            print(f"[DEBUG-DIV {stock_code}] has_table={has_table} has_tbody={has_tbody} first300={first_300!r}")
            sys.stdout.flush()

        parser = DivParser()
        parser.feed(html)

        if debug:
            print(f"[DEBUG-DIV {stock_code}] parsed_rows={len(parser.rows)} sample={parser.rows[:2] if parser.rows else 'NONE'}")
            sys.stdout.flush()

        # Aggregate by payment month: rows = [data_com, valor, tipo, data_pagamento, ...]
        # Payment date format: DD/MM/YYYY
        by_month = {}
        for row in parser.rows:
            try:
                valor_str = row[1].replace(',', '.')
                valor = float(valor_str)
                pagamento = row[3]  # DD/MM/YYYY
                parts = pagamento.split('/')
                if len(parts) == 3:
                    month_key = f"{parts[2]}-{parts[1]}"  # YYYY-MM
                    by_month[month_key] = by_month.get(month_key, 0) + valor
            except (ValueError, IndexError):
                continue

        if debug:
            print(f"[DEBUG-DIV {stock_code}] aggregated_months={len(by_month)} first3={dict(list(by_month.items())[:3])}")
            sys.stdout.flush()
        return by_month
    except Exception as e:
        # Log exception em vez de silenciar — sem isso, qualquer falha de rede
        # ou parsing fica invisível. ConnectionError, Timeout, SSLError etc
        # caem aqui.
        print(f"[DEBUG-DIV {stock_code}] EXCEPTION {type(e).__name__}: {e}")
        sys.stdout.flush()
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


def process_stock(stock_code):
    """Fetch history and build monthly data with dividends."""
    daily = fetch_stock_history(stock_code)
    if not daily:
        return stock_code, None
    dividends = fetch_dividends(stock_code)
    return stock_code, group_daily_to_monthly(daily, dividends)


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


def get_volume_months(hist, to_month):
    if len(hist) - 1 < to_month:
        return 0
    last_date = datetime.strptime(hist[0]["date"], "%Y-%m-%d")
    if diff_month(datetime.today(), last_date) > 1:
        return 0
    return sum(hist[m]["volume"] for m in range(to_month))


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

        print(f"Step 3: Fetching history from Investidor10 ({MAX_WORKERS} workers)...")
        sys.stdout.flush()

        all_stocks_ref = db.reference("stockHistory")
        success_count = 0
        error_list = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_stock, code): code for code in stock_codes}

            for i, future in enumerate(as_completed(futures)):
                code = futures[future]
                try:
                    stock_code, historical = future.result(timeout=120)
                    if historical:
                        var12 = get_variation_months(historical, 12)
                        var6 = get_variation_months(historical, 6)
                        vol1 = get_volume_months(historical, 1)

                        all_stocks_ref.child(stock_code).set({
                            "historical": historical,
                            "variationTwelveMonths": round(var12, 2),
                            "variationSixMonths": round(var6, 2),
                            "volumeInLastMonth": vol1,
                        })
                        success_count += 1
                except Exception as e:
                    error_list.append(f"{code}: {e}")

                if (i + 1) % 100 == 0 or (i + 1) == len(stock_codes):
                    print(f"  Progress: {i + 1}/{len(stock_codes)} ({success_count} saved, {len(error_list)} errors)")
                    sys.stdout.flush()

        skipped = len(stock_codes) - success_count - len(error_list)
        print(f"\nDone! {success_count} saved, {skipped} skipped (no data/delisted), {len(error_list)} errors")
        if error_list:
            print(f"  Errors: {', '.join(error_list[:10])}")
        sys.stdout.flush()

    except Exception as e:
        print(f"FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
