import requests
import json
import time
from confluent_kafka import Producer
import secrets_key

# 여러 종목을 설정
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
stock_dict = {
    "MSFT": "Microsoft",
    "AAPL": "Apple",
    "NVDA": "NVIDIA",
    "GOOGL": "Alphabet",
    "AMZN": "Amazon",
    "META": "Meta Platforms",
    "TSM": "Taiwan Semiconductor Manufacturing",
    "BRK-A": "Berkshire Hathaway",
    "LLY": "Eli Lilly",
    "AVGO": "Broadcom",
    "NVO": "Novo Nordisk",
    "TSLA": "Tesla",
    "JPM": "JPMorgan Chase",
    "V": "Visa",
    "WMT": "Walmart",
    "XOM": "ExxonMobil",
    "UNH": "UnitedHealth Group",
    "MA": "Mastercard",
    "ASML": "ASML Holding",
    "PG": "Procter & Gamble",
    "ORCL": "Oracle",
    "COST": "Costco Wholesale",
    "JNJ": "Johnson & Johnson",
    "HD": "Home Depot",
    "MRK": "Merck",
    "BAC": "Bank of America",
    "ABBV": "AbbVie",
    "NFLX": "Netflix",
    "CVX": "Chevron",
    "KO": "Coca-Cola",
    "TM": "Toyota Motor",
    "AMD": "Advanced Micro Devices",
    "QCOM": "Qualcomm",
    "AZN": "AstraZeneca",
    "CRM": "Salesforce",
    "ADBE": "Adobe",
    "PEP": "PepsiCo",
    "SAP": "SAP",
    "TMO": "Thermo Fisher Scientific",
    "NVS": "Novartis",
    "LIN": "Linde",
    "TMUS": "T-Mobile US",
    "WFC": "Wells Fargo",
    "PDD": "Pinduoduo",
    "AMAT": "Applied Materials",
    "ACN": "Accenture",
    "FMX": "Fomento Economico Mexicano",
    "CSCO": "Cisco Systems",
    "DHR": "Danaher",
    "MCD": "McDonald's",
    "DIS": "Disney",
    "ABT": "Abbott Laboratories",
    "GE": "General Electric",
    "BABA": "Alibaba",
    "TXN": "Texas Instruments",
    "INTU": "Intuit",
    "VZ": "Verizon Communications",
    "AXP": "American Express",
    "AMGN": "Amgen",
    "HSBC": "HSBC Holdings",
    "CAT": "Caterpillar",
    "HDB": "HDFC Bank",
    "IBM": "IBM",
    "PFE": "Pfizer",
    "PTR": "PetroChina",
    "MS": "Morgan Stanley",
    "PM": "Philip Morris International",
    "TTE": "TotalEnergies",
    "MU": "Micron Technology",
    "NOW": "ServiceNow",
    "ISRG": "Intuitive Surgical",
    "CMCSA": "Comcast",
    "BX": "Blackstone",
    "NEE": "NextEra Energy",
    "GS": "Goldman Sachs",
    "UBER": "Uber Technologies",
    "NKE": "Nike",
    "BHP": "BHP Group",
    "UL": "Unilever",
    "RTX": "Raytheon Technologies",
    "HON": "Honeywell",
    "SPGI": "S&P Global",
    "UNP": "Union Pacific",
    "LRCX": "Lam Research",
    "BKNG": "Booking Holdings",
    "SCHW": "Charles Schwab",
    "INTC": "Intel",
    "T": "AT&T",
    "COP": "ConocoPhillips",
    "SYK": "Stryker",
    "LOW": "Lowe's",
    "ETN": "Eaton",
    "TJX": "TJX Companies",
    "ANTM": "Anthem",
    "PGR": "Progressive",
    "VRTX": "Vertex Pharmaceuticals",
    "SNY": "Sanofi",
    "BLK": "BlackRock",
    "UPS": "United Parcel Service",
    "BUD": "Anheuser-Busch InBev"
}

# 토큰 발급 URL
token_url = "https://openapi.koreainvestment.com:9443/oauth2/tokenP"

data = {
    "grant_type": "client_credentials",
    "appkey": secrets_key.app_key_2,
    "appsecret": secrets_key.app_secret_2
}

# 토큰 요청
response = requests.post(token_url, data=json.dumps(data))
if response.status_code == 200:
    token_info = response.json()
    access_token = token_info['access_token']
else:
    print("Failed to obtain access token: ", response.status_code, response.text)
    time.sleep(1000)
    exit()

headers2 = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appKey": secrets_key.app_key_2,
        "appsecret": secrets_key.app_secret_2,
        "tr_id": "HHDFS76200200"
    }

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
    keys = list(stock_dict.keys())
    values = list(stock_dict.values())

    for (symbol, name), exchange in zip(zip(keys, values), stock_exchanges):
        api_url = "https://openapi.koreainvestment.com:9443/uapi/overseas-price/v1/quotations/price-detail?AUTH=&EXCD="
        api_url += exchange + "&SYMB="
        api_url += symbol
        time.sleep(0.5)
        params = {
            "AUTH": "",
            "EXCD": exchange,
            "SYMB": symbol
        }
        response = requests.request("GET", api_url, headers=headers2, params=params)
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
                'country' : "EN",
                'symbol': symbol,
                'name': name,
                'exchange': exchange,
                'sector': data['output']['e_icod'],
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
    
while True:
    fetch_and_send_stock_prices()
    print("[en_stock_producer]: One cycle done.")
    time.sleep(1)
