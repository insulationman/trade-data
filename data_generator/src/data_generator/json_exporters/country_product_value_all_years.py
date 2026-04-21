
from data_generator.db_helpers.calculations.country_product_yearly_value import NetMarketCountryTradeBalance


def export_country_product_value_all_years(session, export_path):
    """Generate a JSON file with country product values for all years."""
    import json
    from collections import defaultdict

    ##create the directory if it does not exist
    ##the export_path is a directory path, not a file path
    import os
    dir_path = os.path.abspath(export_path)
    os.makedirs(dir_path, exist_ok=True)
    
    country_product_values = defaultdict(list)
    
    balances = session.query(
        NetMarketCountryTradeBalance.year,
        NetMarketCountryTradeBalance.country_code,
        NetMarketCountryTradeBalance.product_code,
        NetMarketCountryTradeBalance.net_trade_dollar_value,
        NetMarketCountryTradeBalance.net_trade_metric_tonnes
    ).all()
    
    for balance in balances:
        country_product_values[balance.product_code].append({
            "year": balance.year,
            "country_code": balance.country_code,
            "net_trade_dollar_value": balance.net_trade_dollar_value,
            "net_trade_metric_tonnes": float(balance.net_trade_metric_tonnes)
        })
    #write one json file per product
    for product_code, country_values in country_product_values.items():
        file_path = os.path.join(dir_path, f"{product_code}.json")
        with open(file_path, 'w') as f:
            json.dump(country_values, f, indent=2)