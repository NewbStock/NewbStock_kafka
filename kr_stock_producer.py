import requests
import json
import time
from confluent_kafka import Producer
import secrets_key

    # 여러 종목을 설정
stock_dict = {
    "005930": "삼성전자",
    "005935": "삼성전자우",
    "000660": "SK하이닉스",
    "373220": "LG에너지솔루션",
    "005380": "현대차",
    "005389": "현대차3우B",
    "005385": "현대차우",
    "005387": "현대차2우B",
    "207940": "삼성바이오로직스",
    "000270": "기아",
    "068270": "셀트리온",
    "051910": "LG화학",
    "005490": "POSCO홀딩스",
    "051915": "LG화학우",
    "035420": "NAVER",
    "006400": "삼성SDI",
    "006405": "삼성SDI우",
    "105560": "KB금융",
    "028260": "삼성물산",
    "012330": "현대모비스",
    "055550": "신한지주",
    "035720": "카카오",
    "003670": "포스코퓨처엠",
    "066570": "LG전자",
    "086790": "하나금융지주",
    "066575": "LG전자우",
    "032830": "삼성생명",
    "042700": "한미약품",
    "138040": "메리츠금융지주",
    "015760": "한국전력",
    "000815": "삼성화재우",
    "000810": "삼성화재",
    "003555": "LG우",
    "323410": "카카오뱅크",
    "003550": "LG",
    "018260": "삼성에스디에스",
    "009155": "LG화학2우",
    "009150": "삼성전기",
    "259960": "크래프톤",
    "017670": "SK텔레콤",
    "012450": "한화에어로스페이스",
    "011200": "HMM",
    "402340": "SK바이오사이언스",
    "096775": "SK이노베이션우",
    "329180": "현대중공업",
    "096770": "SK이노베이션",
    "024110": "기업은행",
    "010130": "고려아연",
    "316140": "우리금융지주",
    "033780": "KT&G",
    "034020": "두산에너빌리티",
    "010950": "S-Oil",
    "010955": "S-Oil우",
    "352820": "하이브",
    "042660": "대우조선해양",
    "034730": "SK",
    "030200": "KT",
    "090430": "아모레퍼시픽",
    "009540": "한국조선해양",
    "090435": "아모레퍼시픽우",
    "267260": "현대엔지니어링",
    "047050": "포스코인터내셔널",
    "010140": "삼성중공업",
    "003495": "대한항공우",
    "003490": "대한항공",
    "161390": "한국타이어앤테크놀로지",
    "086280": "현대글로비스",
    "326030": "SK바이오팜",
    "051905": "LG화학우",
    "001570": "롯데관광개발",
    "051900": "LG생활건강",
    "000060": "메리츠화재",
    "241560": "두산밥캣",
    "005830": "DB손해보험",
    "000100": "유한양행",
    "000105": "유한양행우",
    "028050": "삼성엔지니어링",
    "034220": "LG디스플레이",
    "010145": "삼성중공업우",
    "097950": "CJ제일제당",
    "267250": "현대건설기계",
    "047810": "한국항공우주",
    "011070": "LG이노텍",
    "097955": "CJ제일제당우",
    "011170": "롯데케미칼",
    "005070": "코웨이",
    "377300": "카카오페이",
    "251270": "넷마블",
    "361610": "SK아이이테크놀로지",
    "302440": "SK바이오팜",
    "064350": "현대제철",
    "009835": "한화솔루션우",
    "009830": "한화솔루션",
    "088980": "맥쿼리인프라",
    "011790": "SKC",
    "078930": "GS",
    "004020": "현대제철",
    "078935": "GS우",
    "010120": "LS ELECTRIC",
    "032640": "LG유플러스"
}

# 토큰 발급 URL
token_url = "https://openapivts.koreainvestment.com:29443/oauth2/token"

data = {
    "grant_type": "client_credentials",
    "appkey": secrets_key.app_key,
    "appsecret": secrets_key.app_secret
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

headers2 = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appKey": secrets_key.app_key,
        "appSecret": secrets_key.app_secret,
        "tr_id": "FHKST01010100"
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
    api_url = "https://openapivts.koreainvestment.com:29443/uapi/domestic-stock/v1/quotations/inquire-price"

    while True:
        for symbol, name in stock_dict.items():
            time.sleep(0.5)
            params = {
                "fid_cond_mrkt_div_code": "J",
                "fid_input_iscd": symbol
            }
            response = requests.get(api_url, headers=headers2, params=params)
            if response.status_code == 200:
                data = response.json()
                timestamp = int(time.time())
                low_price = float(data['output']['stck_lwpr'])
                high_price = float(data['output']['stck_hgpr'])
                percent = 0 if low_price == 0 or high_price == 0 else low_price / high_price
                stock_data = {
                    'country': "KR",
                    'symbol': symbol,
                    'name': name,
                    'sector' : data['output'].get('bstp_kor_isnm', 'Unknown'),
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

while True:
    fetch_and_send_stock_prices()
    print("One cycle done.")
    time.sleep(1)
