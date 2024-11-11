import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
from login_data import LOGIN_URL, LOGIN_HEADERS, LOGIN_COOKIES, LOGIN_PAYLOAD

# def get_headers_as_dict(headers: str) -> dict:
#     dic = {}
#     for line in headers.split("\n"):
#         if line.startswith(("GET", "POST")):
#             continue
#         point_index = line.find(":")
#         dic[line[:point_index].strip()] = line[point_index+1:].strip()
#     return dic

def login(session: requests.sessions.Session):
    # headers = get_headers_as_dict(LOGIN_HEADERS)
    response = session.post(LOGIN_URL, headers=LOGIN_HEADERS, cookies=LOGIN_COOKIES, data=LOGIN_PAYLOAD)
    # print(response.content)
    response.raise_for_status()

    return session.cookies.get_dict()


DATA_URL = "https://beursrally-game.tijd.be/service/Game/SearchApi/GetSearchAssets?"

def get_assets_raw():
    session = requests.Session()
    retry = Retry(connect = 3, backoff_factor = 0.5)
    adapter = HTTPAdapter(max_retries = retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    cookies = login(session)
    print(cookies)
    data_response = session.get(DATA_URL, cookies=cookies)
    if data_response.status_code != 200:
        print(f"Something went wrong: status code {data_response.status_code}\n{data_response.text}")
    else:
        with open('./assets_raw.json', 'w+') as f:
            json.dump(data_response.json(), f, indent=4)




if __name__ == "__main__":
    get_assets_raw()
    #
    # session = requests.Session()
    # retry = Retry(connect=3, backoff_factor=0.5)
    # adapter = HTTPAdapter(max_retries=retry)
    # session.mount('http://', adapter)
    # session.mount('https://', adapter)
    #
    # print(login(session))