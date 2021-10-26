import requests
import time
import os

import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

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

# Função responsável por salvar os dados


def getVariationMonths(historical_doc, toMonth):
    if len(historical_doc) - 1 < toMonth:
        return 0

    actualMonth = historical_doc[0]['close']
    lastMonth = historical_doc[toMonth - 1]['close']

    if lastMonth > actualMonth:
        return ((lastMonth / actualMonth - 1) * 100) * -1
    return (actualMonth / lastMonth - 1) * 100


def function_main(stockCode):
    data = get_information(stockCode)
    if data != "invalid":
        historical = data['Monthly Adjusted Time Series']
        historical_doc = get_historical_information(historical)

        variationTwelveMonths = getVariationMonths(historical_doc, 12)
        variationEightMonths = getVariationMonths(historical_doc, 8)
        variationSixMonths = getVariationMonths(historical_doc, 6)

        newStock = {
            "historical": historical_doc,
            "variationTwelveMonths": variationTwelveMonths,
            "variationEightMonths": variationEightMonths,
            "variationSixMonths": variationSixMonths
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
