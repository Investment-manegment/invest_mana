import requests, json, os, yaml, copy, time
from datetime import datetime

config_root = os.getcwd() + "\\"
# config_root = 'd:\\KIS\\config\\'  # 토큰 파일이 저장될 폴더, 제3자가 찾지 어렵도록 경로 설정하시기 바랍니다.
#token_tmp = config_root + 'KIS000000'  # 토큰 로컬저장시 파일 이름 지정, 파일이름을 토큰값이 유추가능한 파일명은 삼가바랍니다.
#token_tmp = config_root + 'KIS' + datetime.today().strftime("%Y%m%d%H%M%S")  # 토큰 로컬저장시 파일명 년월일시분초
token_tmp = config_root + 'KIS' + datetime.today().strftime("%Y%m%d")  # 토큰 로컬저장시 파일명 년월일

# 접근토큰 관리하는 파일 존재여부 체크, 없으면 생성
if os.path.exists(token_tmp) == False:
    f = open(token_tmp, "w+")

# 앱키, 앱시크리트, 토큰, 계좌번호 등 저장관리, 자신만의 경로와 파일명으로 설정하시기 바랍니다.
# pip install PyYAML (패키지설치)
with open(config_root + 'kis_devlp.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)

_TRENV = tuple()
_last_auth_time = datetime.now()
_autoReAuth = False
_DEBUG = False
_isPaper = False

# 기본 헤더값 정의
_base_headers = {
    "Content-Type": "application/json",
    "Accept": "text/plain",
    "charset": "UTF-8",
    'User-Agent': _cfg['my_agent']
}

# 토큰 발급 받아 저장 (토큰값, 토큰 유효시간,1일, 6시간 이내 발급신청시는 기존 토큰값과 동일, 발급시 알림톡 발송)
def save_token(my_token, my_expired):
    valid_date = datetime.strptime(my_expired, '%Y-%m-%d %H:%M:%S')
    print('Save token date: ', valid_date)
    with open(token_tmp, 'w', encoding='utf-8') as f:
        f.write(f'token: {my_token}\n')
        f.write(f'valid-date: {valid_date}\n')


# 토큰 확인 (토큰값, 토큰 유효시간_1일, 6시간 이내 발급신청시는 기존 토큰값과 동일, 발급시 알림톡 발송)
def read_token():
    try:
        # 토큰이 저장된 파일 읽기
        with open(token_tmp, encoding='UTF-8') as f:
            tkg_tmp = yaml.load(f, Loader=yaml.FullLoader)

        # 토큰 만료 일,시간
        exp_dt = datetime.strftime(tkg_tmp['valid-date'], '%Y-%m-%d %H:%M:%S')
        # 현재일자,시간
        now_dt = datetime.today().strftime("%Y-%m-%d %H:%M:%S")

        # print('expire dt: ', exp_dt, ' vs now dt:', now_dt)
        # 저장된 토큰 만료일자 체크 (만료일시 > 현재일시 인경우 보관 토큰 리턴)
        if exp_dt > now_dt:
            return tkg_tmp['token']
        else:
            # print('Need new token: ', tkg_tmp['valid-date'])
            return None
    except Exception as e:
        # print('read token error: ', e)
        return None

# 토큰 유효시간 체크해서 만료된 토큰이면 재발급처리
def _getBaseHeader():
    if _autoReAuth: reAuth()
    return copy.deepcopy(_base_headers)

# 가져오기 : 앱키, 앱시크리트, 종합계좌번호(계좌번호 중 숫자8자리), 계좌상품코드(계좌번호 중 숫자2자리), 토큰, 도메인
def _setTRENV(cfg):
    global _TRENV
    _TRENV = cfg

def isPaperTrading():  # 모의투자 매매
    return _isPaper

def _getResultObject(json_data):
    return json_data

# Token 발급, 유효기간 1일, 6시간 이내 발급시 기존 token값 유지, 발급시 알림톡 무조건 발송
def auth(svr='vps', product='01', url=None):
    p = {
        "grant_type": "client_credentials",
    }
    # 개인 환경파일 "kis_devlp.yaml" 파일을 참조하여 앱키, 앱시크리트 정보 가져오기
    # 개인 환경파일명과 위치는 고객님만 아는 위치로 설정 바랍니다.

    # 앱키, 앱시크리트 가져오기
    p["appkey"] = _cfg["my_app"]
    p["appsecret"] = _cfg["my_sec"]
    _setTRENV(_cfg)
    # 기존 발급된 토큰이 있는지 확인
    saved_token = read_token()  # 기존 발급 토큰 확인
    # print("saved_token: ", saved_token)
    if saved_token is None:  # 기존 발급 토큰 확인이 안되면 발급처리
        url = f'{_cfg[svr]}/oauth2/tokenP'
        res = requests.post(url, data=json.dumps(p), headers=_getBaseHeader())  # 토큰 발급
        rescode = res.status_code
        if rescode == 200:  # 토큰 정상 발급
            my_token = res.json()['access_token']
            my_expired = res.json()['access_token_token_expired']
            save_token(my_token, my_expired)  # 새로 발급 받은 토큰 저장
        else:
            print('Get Authentification token fail!\nYou have to restart your app!!!')
            return
    else:
        my_token = saved_token  # 기존 발급 토큰 확인되어 기존 토큰 사용


    _base_headers["authorization"] = _TRENV["my_token"]
    _base_headers["appkey"] = _TRENV["my_app"]
    _base_headers["appsecret"] = _TRENV["my_sec"]

    global _last_auth_time
    _last_auth_time = datetime.now()

    if (_DEBUG):
        print(f'[{_last_auth_time}] => get AUTH Key completed!')

# end of initialize, 토큰 재발급, 토큰 발급시 유효시간 1일
# 프로그램 실행시 _last_auth_time에 저장하여 유효시간 체크, 유효시간 만료시 토큰 발급 처리
def reAuth(svr='prod', product='01'):
    n2 = datetime.now()
    if (n2 - _last_auth_time).seconds >= 86400:  # 유효시간 1일
        auth(svr, product)

# 접근토큰발급 저장
auth()
api_key = read_token()

# API 정보
url = _cfg["my_url"]
tr_id = "HHDFS00000300"  # 거래 ID (현재가 조회: HHDFS00000300)

# 요청 헤더
headers = {
    "Content-Type": "application/json",
    "Accept": "text/plain",
    "charset": "UTF-8",
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "authorization": f"Bearer {api_key}",
    "appKey": _cfg["my_app"],
    "appSecret": _cfg["my_sec"],
    "tr_id": tr_id,
}

trade_list = _cfg["trading_volume"]

'''# 요청 데이터 (JSON)
params = {
    "fid_cond_mrkt_div_code": "N",
    "fid_input_date_1": "20220401",
    "fid_input_date_2": "20220613",
    "fid_input_iscd": ".DJI",
    "fid_period_div_code": "D"
}
response = requests.get(url, headers=headers, params=params)
print(response.json())'''

# API 요청 결과 저장할 리스트
all_responses = []

# API 요청
for list in trade_list:
    params = list
    response = requests.get(url, headers=headers, params=params)

    # 응답 데이터를 리스트에 추가
    all_responses.append(response.json())

    time.sleep(0.5)

# 모든 응답 데이터를 하나의 딕셔너리로 합치기
combined_data = {"results": all_responses}

# JSON 파일로 저장
with open("combined_responses.json", "w", encoding="utf-8") as f:
    json.dump(combined_data, f, ensure_ascii=False, indent=4)