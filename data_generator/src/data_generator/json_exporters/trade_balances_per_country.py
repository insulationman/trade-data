def trade_balances_per_country(session, export_path):
    """Export trade balances per country as JSON."""
    from data_generator.db_helpers.calculations.country_product_yearly_value import NetMarketCountryTradeBalance
    import json
    from collections import defaultdict
    import os
    from tqdm import tqdm

    # Create the directory if it does not exist
    dir_path = os.path.abspath(export_path)
    os.makedirs(dir_path, exist_ok=True)

    countries = session.query(NetMarketCountryTradeBalance.country_code).distinct().all()

    for country_tuple in tqdm(countries, desc="Exporting countries", unit="country"):
        country_code = country_tuple[0]

        balances = session.query(
            NetMarketCountryTradeBalance.year,
            NetMarketCountryTradeBalance.product_code,
            NetMarketCountryTradeBalance.net_trade_dollar_value,
            NetMarketCountryTradeBalance.net_trade_metric_tonnes
        ).filter(
            NetMarketCountryTradeBalance.country_code == country_code
        ).all()

        
        data = []
        for balance in balances:
            data.append({
                "year": balance.year,
                "product_code": balance.product_code,
                "net_trade_dollar_value": balance.net_trade_dollar_value,
                "net_trade_metric_tonnes": float(balance.net_trade_metric_tonnes)
            })

        file_path = os.path.join(dir_path, f"{country_code}.json")
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
