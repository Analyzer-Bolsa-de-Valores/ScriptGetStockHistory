import requests
import time
import os

import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from datetime import datetime

mongoClient = os.environ["DB_SOURCE"]
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


def get_information(stockCode):
    try:
        response = requests.get(
            f'https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY_ADJUSTED&symbol={stockCode}.sa&apikey=YOUR_API_KEY')
        if response.status_code == 200:
            if 'Monthly Adjusted Time Series' in response.text:
                return response.json()
            elif "Invalid API call" in response.text:
                return 'invalid'
        print("Aguardando 60 seg...")
        time.sleep(60)
        return get_information(stockCode)
    except expression as identifier:
        print("Aguardando 60 seg...")
        time.sleep(60)
        return get_information(stockCode)

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


def diff_month(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month


def getVariationMonths(historical_doc, toMonth):
    if len(historical_doc) - 1 < toMonth:
        return 0

    lastMonthValue = historical_doc[0]['close']
    firstMonthValue = historical_doc[toMonth - 1]['close']

    lastMonthDate = datetime.strptime(historical_doc[0]['date'], '%Y-%m-%d')
    todayDate = datetime.today()

    monthsDifference = diff_month(todayDate, lastMonthDate)

    if monthsDifference > 1:
        return 0

    if firstMonthValue > lastMonthValue:
        return ((firstMonthValue / lastMonthValue - 1) * 100) * -1
    return (lastMonthValue / firstMonthValue - 1) * 100


def getVolumeMonths(historical_doc, toMonth):
    if len(historical_doc) - 1 < toMonth:
        return 0

    volumeAcum = 0

    for month in range(0, toMonth):
        volumeAcum = volumeAcum + historical_doc[month]['volume']

    lastMonthDate = datetime.strptime(historical_doc[0]['date'], '%Y-%m-%d')
    todayDate = datetime.today()

    monthsDifference = diff_month(todayDate, lastMonthDate)

    if monthsDifference > 1:
        return 0

    return volumeAcum


def function_main(stockCode):
    data = get_information(stockCode)
    if data != "invalid":
        historical = data['Monthly Adjusted Time Series']
        historical_doc = get_historical_information(historical)

        variationTwelveMonths = getVariationMonths(historical_doc, 12)
        variationSixMonths = getVariationMonths(historical_doc, 6)
        volumeInLastMonth = getVolumeMonths(historical_doc, 1)

        newStock = {
            "historical": historical_doc,
            "variationTwelveMonths": variationTwelveMonths,
            "variationSixMonths": variationSixMonths,
            "volumeInLastMonth": volumeInLastMonth,
        }

        all_stocks.child(stockCode).set(newStock)
        print(f'Recuperou o historico de {stockCode}')
    else:
        stock_error_list.append(stockCode)
        print(f'Recuperou o historico de {stockCode} - error')


#################### INICIO ####################
totalAtivos = len(stocks)

print(f'Recuperou todos ativos os {len(stocks)} ativos')

for stockCode in stocks:
    function_main(stockCode)

totalAtivosComErro = len(stock_error_list)

print(f'Lista dos {totalAtivosComErro} ativos com erro:')
for stockerror in stock_error_list:
    print(f'{stockerror} - error')

print()
print(f'Total de ativos com sucesso: {totalAtivos - totalAtivosComErro}')
