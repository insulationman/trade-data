from data_generator.db_helpers.calculations.country_product_yearly_value import NetMarketCountryTradeBalance
from sqlalchemy import func


def market_sizes_per_year(session, export_path):
    """Export market sizes per year as JSON."""

    # for each market, sum the market sizes per year
    # do one value for all countries with positive net trade balance
    # and one for all countries with negative net trade balance

    positive_market_sizes = session.query(
        NetMarketCountryTradeBalance.year,
        NetMarketCountryTradeBalance.product_code,
        func.sum(NetMarketCountryTradeBalance.net_trade_dollar_value).label("total_market_size")
    ).filter(
        NetMarketCountryTradeBalance.net_trade_dollar_value > 0
    ).group_by(
        NetMarketCountryTradeBalance.year,
        NetMarketCountryTradeBalance.product_code
    ).all()

    negative_market_sizes = session.query(
        NetMarketCountryTradeBalance.year,
        NetMarketCountryTradeBalance.product_code,
        func.sum(NetMarketCountryTradeBalance.net_trade_dollar_value).label("total_market_size")
    ).filter(
        NetMarketCountryTradeBalance.net_trade_dollar_value < 0
    ).group_by(
        NetMarketCountryTradeBalance.year,
        NetMarketCountryTradeBalance.product_code
    ).all()

    import json
    from collections import defaultdict
    import os
    from tqdm import tqdm

    # Create the directory if it does not exist
    dir_path = os.path.abspath(export_path)
    os.makedirs(dir_path, exist_ok=True)    
    # organize data by product_code and year 
    data = defaultdict(lambda: {"positive_market_sizes": {}, "negative_market_sizes": {}})
    for entry in positive_market_sizes:
        data[entry.product_code]["positive_market_sizes"][entry.year] = float(entry.total_market_size)
    for entry in negative_market_sizes:
        data[entry.product_code]["negative_market_sizes"][entry.year] = float(entry.total_market_size)
    # export each year's data as a separate JSON file
    years = set()
    for entry in positive_market_sizes + negative_market_sizes:
        years.add(entry.year)

    for year in tqdm(years, desc="Exporting market sizes per year"):
        year_data = []
        for product_code, sizes in data.items():
            pos_size = sizes["positive_market_sizes"].get(year, 0.0)
            neg_size = sizes["negative_market_sizes"].get(year, 0.0)
            if pos_size != 0.0 or neg_size != 0.0:
                year_data.append({
                    "product_code": product_code,
                    "year": year,
                    "export_market_size": pos_size,
                    "import_market_size": -neg_size
                })
        # write to JSON file
        file_path = os.path.join(dir_path, f"{year}.json")
        with open(file_path, 'w') as f:
            json.dump(year_data, f, indent=2)