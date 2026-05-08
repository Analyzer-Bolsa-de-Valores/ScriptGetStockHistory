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
from lxml.html import fromstring
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
    """Fetch dividend history from Investidor10 stock page.

    Fundamentus.com.br foi descartado: retorna 403 do IP do GitHub Actions
    (Cloudflare TLS fingerprint detection) — confirmado em runs de 2026-05-08.
    Investidor10 já é a fonte de dailies (fetch_stock_history) e devolve a
    tabela completa de proventos inline no HTML da página da ação.

    Estratégia de agregação: cada declaração de provento aparece N vezes
    quando há N parcelas (mesma data_com + tipo + valor, datas de pagamento
    diferentes). O valor declarado é total — distribuímos uniformemente entre
    parcelas pra não dobrar a soma anual.
    """
    # 1 retry cobre ~3% de timeouts pontuais do Investidor10 (medido em
    # 2026-05-08). Sleep curto entre tentativas pra não amplificar lentidão.
    last_err = None
    r = None
    for attempt in range(2):
        try:
            r = requests.get(
                f"https://investidor10.com.br/acoes/{stock_code.lower()}/",
                headers=HEADERS,
                timeout=30,
            )
            break
        except requests.exceptions.RequestException as e:
            last_err = e
            if attempt == 0:
                time.sleep(1)
    if r is None:
        print(f"[fetch_dividends {stock_code}] giving up after retry: {type(last_err).__name__}: {last_err}")
        sys.stdout.flush()
        return {}
    try:
        if r.status_code != 200:
            # 404 esperado pra stocks delistadas que não têm página no I10.
            return {}

        page = fromstring(r.text)
        tables = page.xpath('//table[@id="table-dividends-history"]')
        if not tables:
            return {}

        rows = tables[0].xpath('tbody/tr')

        # Agrupa parcelas: (data_com, tipo, valor) → [datas_pagamento]. Quando
        # PETR4 paga JCP em 2 parcelas, a tabela mostra 2 linhas com mesmo
        # valor declarado e datas de pagamento diferentes. Tratamos como UMA
        # declaração e dividimos o valor entre as parcelas.
        from collections import defaultdict
        groups = defaultdict(list)
        for tr in rows:
            tds = [td.xpath('string(.)').strip() for td in tr.xpath('td')]
            if len(tds) < 4:
                continue
            tipo, data_com, pagamento, valor_str = tds[0], tds[1], tds[2], tds[3]
            try:
                valor = float(valor_str.replace('.', '').replace(',', '.'))
            except ValueError:
                continue
            groups[(data_com, tipo, valor)].append(pagamento)

        by_month = {}
        for (_, _, valor), pagamentos in groups.items():
            if not pagamentos or valor <= 0:
                continue
            valor_por_parcela = valor / len(pagamentos)
            for pag in pagamentos:
                parts = pag.split('/')
                if len(parts) == 3:
                    month_key = f"{parts[2]}-{parts[1]}"  # YYYY-MM
                    by_month[month_key] = by_month.get(month_key, 0) + valor_por_parcela

        return by_month
    except Exception as e:
        # Loga em vez de silenciar — sem isso falhas de parse ficam invisíveis.
        print(f"[fetch_dividends {stock_code}] {type(e).__name__}: {e}")
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
