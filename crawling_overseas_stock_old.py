import requests, re, json
from bs4 import BeautifulSoup

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

response = requests.get("https://finance.naver.com/world/")
soup = BeautifulSoup(response.content, 'html.parser')

def extracted_json_str(data):
    start_index = data.find("{", data.find("jindo.$H(") + len("jindo.$H("))

    # Find the ending index of the last closing curly brace before the final ");"
    end_index = data.rfind("}", 0, data.rfind(");")) + 1  # Include the closing brace

    # Extract the substring between the start and end indices
    json_str = data[start_index:end_index]
    return(json_str)

def extract_js_variable_value(var_name):
    script_tags = soup.find_all('script', string=re.compile(var_name))
    for tag in script_tags:
        match = re.search(fr'{var_name} = (.*?);', tag.string, re.DOTALL)
        if match:
            return match.group(1)

    return None

dict_data = []
var_list =["americaData", "asiaData", "europeAfricaData",]

for i in var_list:
    var_name = i

    # 함수 호출 및 결과 출력
    stock_data = extract_js_variable_value(var_name)

    data_str = extracted_json_str(stock_data)
    data_dict = json.loads(data_str)

    for key, value in data_dict.items():
        dict_data.append({
            'natcKnam': value['natcKnam'],
            'knam': value['knam'],
            'monthCloseVal': value['monthCloseVal'],
            'diff': value['diff'],
            'rate': value['rate']
        })

json_data = json.dumps(dict_data, ensure_ascii=False, indent=4)
print(json_data)