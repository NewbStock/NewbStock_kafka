import requests
import json
import time
from confluent_kafka import Producer
import secrets

# 토큰 발급 URL
token_url = "https://openapi.koreainvestment.com:9443/oauth2/tokenP"

# OAuth 인증을 위한 헤더와 데이터
headers = {
    "Content-Type": "application/x-www-form-urlencoded"
}
data = {
    "grant_type": "client_credentials",
    "appkey": secrets.app_key_2,
    "appsecret": secrets.app_secret_2
}

# 토큰 요청
# response = requests.post(token_url, data=json.dumps(data))
# if response.status_code == 200:
#     token_info = response.json()
#     access_token = token_info['access_token']
#     print(access_token)
# else:
#     print("Failed to obtain access token: ", response.status_code, response.text)
#     time.sleep(1000)
#     exit()

# Kafka 프로듀서 설정
access_token = secrets.access_token
producer = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    """메시지 전달 보고 콜백 함수"""
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def fetch_and_send_stock_prices():
    """주식 가격 데이터를 가져와 Kafka로 전송"""
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appKey": secrets.app_key_2,
        "appsecret": secrets.app_secret_2,
        "tr_id": "HHDFS76200200"
    }
    # 여러 종목을 설정
    stock_symbols = [
    "MSFT", "AAPL", "NVDA", "GOOGL", "AMZN", "META", "TSM", "BRK-A", "LLY", "AVGO",
    "NVO", "TSLA", "JPM", "V", "WMT", "XOM", "UNH", "MA", "ASML", "PG",
    "ORCL", "COST", "JNJ", "HD", "MRK", "BAC", "ABBV", "NFLX", "CVX", "KO",
    "TM", "AMD", "QCOM", "AZN", "CRM", "ADBE", "PEP", "SAP", "TMO", "NVS",
    "LIN", "TMUS", "WFC", "PDD", "AMAT", "ACN", "FMX", "CSCO", "DHR", "MCD",
    "DIS", "ABT", "GE", "BABA", "TXN", "INTU", "VZ", "AXP", "AMGN", "HSBC",
    "CAT", "HDB", "IBM", "PFE", "PTR", "MS", "PM", "TTE", "MU", "NOW",
    "ISRG", "CMCSA", "BX", "NEE", "GS", "UBER", "NKE", "BHP", "UL", "RTX",
    "HON", "SPGI", "UNP", "LRCX", "BKNG", "SCHW", "INTC", "T", "COP", "SYK",
    "LOW", "ETN", "TJX", "ANTM", "PGR", "VRTX", "SNY", "BLK", "UPS", "BUD"
]
    stock_exchanges = [
    "NAS", "NAS", "NAS", "NAS", "NAS", "NAS", "NYS", "NYS", "NYS", "NAS",
    "NYS", "NAS", "NYS", "NYS", "NYS", "NYS", "NYS", "NYS", "NAS", "NYS",
    "NYS", "NAS", "NYS", "NYS", "NYS", "NYS", "NYS", "NAS", "NYS", "NYS",
    "NYS", "NAS", "NAS", "NAS", "NYS", "NAS", "NAS", "NYS", "NYS", "NYS",
    "NYS", "NAS", "NYS", "NAS", "NAS", "NYS", "NYS", "NAS", "NYS", "NYS",
    "NYS", "NYS", "NYS", "NYS", "NAS", "NAS", "NYS", "NYS", "NAS", "NYS",
    "NYS", "NYS", "NYS", "NYS", "NYS", "NYS", "NYS", "NYS", "NYS", "NAS",
    "NAS", "NAS", "NYS", "NYS", "NYS", "NYS", "NYS", "NYS", "NYS", "NYS",
    "NAS", "NYS", "NYS", "NYS", "NYS", "NYS", "NAS", "NYS", "NAS", "NYS",
    "NYS", "NYS", "NYS", "NYS", "NYS", "NAS", "NYS", "NYS", "NYS", "NYS"
]


    while True:
        for symbol, exchange in zip(stock_symbols, stock_exchanges):
            api_url = "https://openapi.koreainvestment.com:9443/uapi/overseas-price/v1/quotations/price-detail?AUTH=&EXCD="
            api_url += exchange + "&SYMB="
            api_url += symbol
            time.sleep(0.5)
            params = {
                "AUTH": "",
                "EXCD": exchange,
                "SYMB": symbol
            }
            response = requests.request("GET", api_url, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                if 'output' not in data or data['rt_cd'] != '0':
                    print(f"API 응답에 'output' 키가 없거나 오류 발생: {data}")
                    continue
                timestamp = int(time.time())
                low_price = float(data['output']['low']) if data['output']['low'] else 0
                high_price = float(data['output']['high']) if data['output']['high'] else 0
                percent = 0 if low_price == 0 or high_price == 0 else low_price / high_price
                stock_data = {
                    'symbol': symbol,
                    'exchange': exchange,
                    'price': data['output']['last'],
                    'percent': percent,
                    'stck_hgpr': high_price,
                    'stck_lwpr': low_price,
                    'timestamp': timestamp
                }
                producer.produce('stock-prices', key=stock_data['symbol'], value=json.dumps(stock_data), callback=delivery_report)
            else:
                print("API 호출 실패: ", response.status_code, response.text)
        producer.flush()
        time.sleep(1)

fetch_and_send_stock_prices()
