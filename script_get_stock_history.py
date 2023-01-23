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
    try:
        response = requests.get(
            f'https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY_ADJUSTED&symbol={stock_code}.sa&apikey=YOUR_API_KEY')
        if response.status_code == 200:
            if 'Monthly Adjusted Time Series' in response.text:
                return response.json()
            if "Invalid API call" in response.text:
                return 'invalid'
        print("Aguardando 60 seg...")
        time.sleep(60)
        return get_information(stock_code)
    except:
        print("Aguardando 60 seg...")
        time.sleep(60)
        return get_information(stock_code)

# Formata retorno do serviço


def get_historical_information(historical):
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
    return (d_1.year - d_2.year) * 12 + d_1.month - d_2.month


def get_variation_months(historical_doc, to_month):
    if len(historical_doc) - 1 < to_month:
        return 0

    lastMonthValue = historical_doc[0]['close']
    firstMonthValue = historical_doc[to_month - 1]['close']

    last_month_date = datetime.strptime(historical_doc[0]['date'], '%Y-%m-%d')
    today_date = datetime.today()

    months_difference = diff_month(today_date, last_month_date)

    if months_difference > 1:
        return 0

    if firstMonthValue > lastMonthValue:
        return ((firstMonthValue / lastMonthValue - 1) * 100) * -1
    return (lastMonthValue / firstMonthValue - 1) * 100


def getVolumeMonths(historical_doc, to_month):
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
    data = get_information(stock_code)
    if data != "invalid":
        historical = data['Monthly Adjusted Time Series']
        historical_doc = get_historical_information(historical)

        variation_twelve_months = get_variation_months(historical_doc, 12)
        variation_six_months = get_variation_months(historical_doc, 6)
        volume_in_last_month = getVolumeMonths(historical_doc, 1)

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

for stock_code in stocks:
    function_main(stock_code)

total_ativos_com_erro = len(stock_error_list)

print(f'Lista dos {total_ativos_com_erro} ativos com erro:')
for stockerror in stock_error_list:
    print(f'{stockerror} - error')

print()
print(f'Total de ativos com sucesso: {totalAtivos - total_ativos_com_erro}')