from beursrally.assets import BeursrallyAssets
from stock_metadata import StockMetadata

if __name__ == "__main__":
    beursrally_stocks = BeursrallyAssets.stock_isins()[:5]
    stock_metadata = StockMetadata()
    for isin in beursrally_stocks:
        stock_metadata.store_metadata(isin)