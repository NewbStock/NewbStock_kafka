import requests
import json
import time
from confluent_kafka import Producer
import secrets

# 토큰 발급 URL
token_url = "https://openapivts.koreainvestment.com:29443/oauth2/tokenP"

# OAuth 인증을 위한 헤더와 데이터
headers = {
    "Content-Type": "application/x-www-form-urlencoded"
}
data = {
    "grant_type": "client_credentials",
    "appkey": secrets.app_key,
    "appsecret": secrets.app_secret
}

# 토큰 요청
response = requests.post(token_url, data=data)
if response.status_code == 200:
    token_info = response.json()
    access_token = token_info['access_token']
else:
    print("Failed to obtain access token: ", response.status_code, response.text)
    time.sleep(1000)
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
    api_url = "https://openapivts.koreainvestment.com:29443/uapi/domestic-stock/v1/quotations/inquire-price"
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appKey": secrets.app_key,
        "appSecret": secrets.app_secret,
        "tr_id": "FHKST01010100"
    }
    # 여러 종목을 설정
    stock_symbols = [
    "005930", "000660", "373220", "035420", "051910", "207940", "005380", "012330", "105560", "028260",
    "066570", "003670", "096770", "035720", "017670", "003550", "018260", "090430", "086790", "251270",
    "034020", "009540", "001040", "042660", "005935", "000270", "086280", "004170", "009150", "008770",
    "005490", "042660", "015760", "128940", "032830", "006400", "030200", "036570", "010130", "001800",
    "069500", "302440", "011170", "003550", "005830", "004990", "009415", "025560", "011790", "008000",
    "001450", "014820", "001750", "035250", "013700", "003495", "003490", "000720", "241560", "051600",
    "009540", "010120", "012450", "004020", "030210", "016360", "005680", "010950", "090430", "006360",
    "012510", "003550", "005420", "071840", "012100", "042420", "020150", "001745", "003520", "069620",
    "003850", "093240", "007330", "004170", "000140", "027740", "000225", "030610", "028050", "010140",
    "009830", "000225", "024110", "008020", "009270", "001450", "006060", "017670", "005930", "051915"
]

    while True:
        for symbol in stock_symbols:
            time.sleep(0.5)
            params = {
                "fid_cond_mrkt_div_code": "J",
                "fid_input_iscd": symbol
            }
            response = requests.get(api_url, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
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
        time.sleep(1)

fetch_and_send_stock_prices()
