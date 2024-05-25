import requests
import json
data_list =[]
i = 0
for i in range(0,1):

    _url = f'https://api.odcloud.kr/api/15001743/v1/uddi:f5124071-9bc1-48bf-8b16-ab43815eba69?serviceKey=eEvY5rw3KhX0Q%2Fau%2FJznLrul9agKg%2Br4bhNMVpwj%2BJeUkofgcyNTQIZkzdSQBmkbMcnsaPUTAEsH5xTZUYtmXA%3D%3D&pageNo={i}&numOfRows=10&type=json'

    response = requests.get(_url)

# 응답 코드 확인
    if 200 <= response.status_code <= 300:
        _content = response.content.decode('utf-8')
    else:
        _content = response.content.decode('utf-8')

    _public_data = json.loads(_content)
    
    for i in _public_data['data']:
        data_list.append(i)
    
_file_path ='/home/kim/python/test_data/house_price.json'

     
with open(_file_path,'w',encoding='utf-8') as f:
    json.dump(data_list, f, ensure_ascii=False, indent=4)