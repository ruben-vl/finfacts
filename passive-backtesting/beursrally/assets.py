import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
from cookie import COOKIE


class BeursrallyAssets:
    DATA_URL = "https://beursrally-game.tijd.be/service/Game/SearchApi/GetSearchAssets"

    @classmethod
    def stock_isins(cls) -> list[str]:
        data = cls._load_data()
        return list(data["Aandelen"].keys())

    @classmethod
    def fund_isins(cls) -> list[str]:
        data = cls._load_data()
        return list(data["Beleggingsfondsen"].keys())

    @classmethod
    def etf_isins(cls) -> list[str]:
        data = cls._load_data()
        return list(data["ETF"].keys())

    @classmethod
    def _load_data(cls) -> dict[str, dict[str, dict[str, str]]]:
        with open('./assets.json', 'r') as file:
            data = json.load(file)
        return data

    @classmethod
    def _get_assets_raw(cls):
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        data_response = session.get(cls.DATA_URL, cookies={"Cookie": COOKIE})
        if data_response.status_code != 200:
            print(f"Something went wrong: status code {data_response.status_code}\n{data_response.text}")
        else:
            with open('./assets_raw.json', 'w+') as file:
                # noinspection PyTypeChecker
                json.dump(data_response.json(), file, indent=4)

    @classmethod
    def _filter_assets(cls):
        with open('./assets_raw.json', 'r') as file:
            data = json.load(file)

        assets = dict()
        for item in data:
            if item["AsseType"] not in assets.keys():
                assets[item["AsseType"]] = dict()
            if item["ISIN"] not in assets[item["AsseType"]].keys():
                assets[item["AsseType"]][item["ISIN"]] = dict()
            for key in ["Name", "Ticker"]:
                assets[item["AsseType"]][item["ISIN"]][key] = item[key]

        with open('./assets.json', 'w+') as file:
            # noinspection PyTypeChecker
            json.dump(assets, file, indent=4)
