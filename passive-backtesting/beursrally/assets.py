import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json


class BeursrallyAssets:
    DATA_URL = "https://beursrally-game.tijd.be/service/Game/SearchApi/GetSearchAssets"

    @classmethod
    def stock_isins(cls) -> list[str]:
        data = cls._load_data()
        res = list(data["Aandelen"].keys())
        res.remove("BE0974380124")
        res.remove("BE0974386188")
        res.remove("FR0013447729")
        res.remove("NL0013654809")
        res.remove("NL00150002Q7")
        return res

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
        with open('/home/ruben/Projects/finfacts/passive-backtesting/beursrally/assets.json', 'r') as file:
            data = json.load(file)
        return data

    @classmethod
    def save_and_filter_data(cls):
        cls._get_assets_raw()
        cls._filter_assets()

    @classmethod
    def _get_assets_raw(cls):
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        with open('/home/ruben/Projects/finfacts/passive-backtesting/beursrally/cookies.json', 'r') as file:
            cookies = json.load(file)
        data_response = session.get(cls.DATA_URL, cookies=cookies["Request Cookies"])
        if data_response.status_code != 200:
            print(f"Something went wrong: status code {data_response.status_code}\n{data_response.text}")
        else:
            with open('/home/ruben/Projects/finfacts/passive-backtesting/beursrally/assets_raw.json', 'w+') as file:
                # noinspection PyTypeChecker
                json.dump(data_response.json(), file, indent=4)

    @classmethod
    def _filter_assets(cls):
        with open('/home/ruben/Projects/finfacts/passive-backtesting/beursrally/assets_raw.json', 'r') as file:
            data = json.load(file)

        assets = dict()
        for item in data:
            if item["AsseType"] not in assets.keys():
                assets[item["AsseType"]] = dict()
            if item["ISIN"] not in assets[item["AsseType"]].keys():
                assets[item["AsseType"]][item["ISIN"]] = dict()
            for key in ["Name", "Ticker"]:
                assets[item["AsseType"]][item["ISIN"]][key] = item[key]

        with open('/home/ruben/Projects/finfacts/passive-backtesting/beursrally/assets.json', 'w+') as file:
            # noinspection PyTypeChecker
            json.dump(assets, file, indent=4)


if __name__ == "__main__":
    BeursrallyAssets.save_and_filter_data()
