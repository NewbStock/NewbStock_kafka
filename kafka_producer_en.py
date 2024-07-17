import requests
import json
import time
from confluent_kafka import Producer

# 앱 등록 후 발급받은 키
app_key = "PS6x0bG79mb4K6QNMIk51XQPxDj4LtMXf2io"
app_secret = "ZuLEcN/qYEN7qguwuulWuEdccHZEk0X3s3hetbdN202X4CkcWY+Swip6YC7YGgFWCRQbpZk7ApZtXsLqGsCbHBQ+LAJkzF5eCsl6GQob4V7XLLzcg41AVcVSRhhIN4+ftX7l5neUFgowfdKlugFMzdhAtkIPfi7Z2hsdYLLrUk3qxZ8c268="

# 토큰 발급 URL
token_url = "https://openapivts.koreainvestment.com:29443/oauth2/token"

# OAuth 인증을 위한 헤더와 데이터
headers = {
    "Content-Type": "application/x-www-form-urlencoded"
}
data = {
    "grant_type": "client_credentials",
    "appkey": app_key,
    "appsecret": app_secret
}

# 토큰 요청
response = requests.post(token_url, headers=headers, data=data)
if response.status_code == 200:
    token_info = response.json()
    access_token = token_info['access_token']
    print("Access Token: ", access_token)
else:
    print("Failed to obtain access token: ", response.status_code, response.text)
    exit()

# Kafka 프로듀서 설정
producer = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    """메시지 전달 보고 콜백 함수"""
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def fetch_and_send_stock_prices():
    """주식 가격 데이터를 가져와 Kafka로 전송"""
    api_url = "https://openapivts.koreainvestment.com:29443/uapi/overseas-price/v1/quotations/dailyprice"
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appKey": app_key,
        "appSecret": app_secret,
        "tr_id": "FHKST01010100"
    }
    # 여러 종목을 설정
    stock_symbols = ["TSLA"]

    while True:
        for symbol in stock_symbols:
            time.sleep(0.5)
            params = {
                "AUTH": "",
                "EXCD": "NAS",
                "SYMB": symbol
            }
            response = requests.get(api_url, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                if 'output' not in data or data['rt_cd'] != '0':
                    print(f"API 응답에 'output' 키가 없거나 오류 발생: {data}")
                    continue
                timestamp = int(time.time())
                low_price = float(data['output']['stck_lwpr'])
                high_price = float(data['output']['stck_hgpr'])
                percent = 0 if low_price == 0 or high_price == 0 else low_price / high_price
                stock_data = {
                    'symbol': symbol,
                    'price': data['output']['stck_prpr'],
                    'percent': percent,
                    'stck_hgpr': high_price,
                    'stck_lwpr': low_price,
                    'timestamp': timestamp
                }
                producer.produce('stock-prices', key=stock_data['symbol'], value=json.dumps(stock_data), callback=delivery_report)
            else:
                print("API 호출 실패: ", response.status_code, response.text)
        producer.flush()
        time.sleep(1)  # 1분 간격으로 데이터 수집

fetch_and_send_stock_prices()
