"""
this script is used to get Brazil stock history
"""
import os
from datetime import datetime
import time

import requests
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

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

stocks = db.reference('stockFundamentus').get()
all_stocks = db.reference('stockHistory')


def get_information(stock_code):
    """
    call the api to get stock informations
    """
    try:
        func = "TIME_SERIES_MONTHLY_ADJUSTED"
        response = requests.get(
            f'https://www.alphavantage.co/query?function={func}&symbol={stock_code}.sa&apikey=KEY',
            timeout=60)
        if response.status_code == 200:
            if 'Monthly Adjusted Time Series' in response.text:
                return response.json()
            if "Invalid API call" in response.text:
                return 'invalid'
        print("Aguardando 60 seg...")
        time.sleep(60)
        return get_information(stock_code)
    except requests.exceptions.RequestException as error:
        print(f'Aguardando 60s - (Error {error.errno})')
        time.sleep(60)
        return get_information(stock_code)

# Formata retorno do serviço


def get_historical_information(historical):
    """
    format and set stock history
    """
    historical_doc = []
    for key, historic in historical.items():
        historic_doc = {
            "date": key,
            "open": float(historic["1. open"]),
            "high": float(historic["2. high"]),
            "low": float(historic["3. low"]),
            "close": float(historic["5. adjusted close"]),
            "volume": float(historic["6. volume"]),
            "dividend": float(historic["7. dividend amount"])
        }
        historical_doc.append(historic_doc)
    return historical_doc


stock_error_list = []


def diff_month(d_1, d_2):
    """
    get difference between two dates in months
    """
    return (d_1.year - d_2.year) * 12 + d_1.month - d_2.month


def get_variation_months(historical_doc, to_month):
    """
    get stock price variation in months (passed as param)
    """
    if len(historical_doc) - 1 < to_month:
        return 0

    last_month_value = historical_doc[0]['close']
    first_month_value = historical_doc[to_month - 1]['close']

    last_month_date = datetime.strptime(historical_doc[0]['date'], '%Y-%m-%d')
    today_date = datetime.today()

    months_difference = diff_month(today_date, last_month_date)

    if months_difference > 1:
        return 0

    if first_month_value > last_month_value:
        return ((first_month_value / last_month_value - 1) * 100) * -1
    return (last_month_value / first_month_value - 1) * 100


def get_volume_months(historical_doc, to_month):
    """
    get stock volume variation in months (passed as param)
    """
    if len(historical_doc) - 1 < to_month:
        return 0

    volume_acum = 0

    for month in range(0, to_month):
        volume_acum = volume_acum + historical_doc[month]['volume']

    last_month_date = datetime.strptime(historical_doc[0]['date'], '%Y-%m-%d')
    today_date = datetime.today()

    months_difference = diff_month(today_date, last_month_date)

    if months_difference > 1:
        return 0

    return volume_acum


def function_main(stock_code):
    """
    inital point of execution of each stock
    """
    data = get_information(stock_code)
    if data != "invalid":
        historical = data['Monthly Adjusted Time Series']
        historical_doc = get_historical_information(historical)

        variation_twelve_months = get_variation_months(historical_doc, 12)
        variation_six_months = get_variation_months(historical_doc, 6)
        volume_in_last_month = get_volume_months(historical_doc, 1)

        new_stock = {
            "historical": historical_doc,
            "variationTwelveMonths": variation_twelve_months,
            "variationSixMonths": variation_six_months,
            "volumeInLastMonth": volume_in_last_month,
        }

        all_stocks.child(stock_code).set(new_stock)
        print(f'Recuperou o historico de {stock_code}')
    else:
        stock_error_list.append(stock_code)
        print(f'Recuperou o historico de {stock_code} - error')


#################### INICIO ####################
totalAtivos = len(stocks)

print(f'Recuperou todos ativos os {len(stocks)} ativos')

for stock_code_i in stocks:
    function_main(stock_code_i)

TOTAL_ATIVOS_COM_ERRO = len(stock_error_list)

print(f'Lista dos {TOTAL_ATIVOS_COM_ERRO} ativos com erro:')
for stockerror in stock_error_list:
    print(f'{stockerror} - error')

print()
print(f'Total de ativos com sucesso: {totalAtivos - TOTAL_ATIVOS_COM_ERRO}')
