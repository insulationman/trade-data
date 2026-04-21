from data_generator.core import calculate_yearly_country_per_product_value, initalize_db, import_baci_products as import_baci_products_core, import_baci_countries as import_baci_countries_core
from data_generator.config import *
from data_generator.core import import_baci_trade_rows as import_baci_trade_rows_core
from data_generator.core import export_product_trades_per_year as export_product_trades_per_year_core
from data_generator.core import export_product_trades


def create_db():
    config = load_config()
    initalize_db(config)

def import_baci_products():
    config = load_config()
    import_baci_products_core(config)

def import_baci_countries():
    config = load_config()
    import_baci_countries_core(config)

def import_baci_trade_rows():
    config = load_config()
    import_baci_trade_rows_core(config)

def export_trade_by_product():
    config = load_config()
    export_product_trades(config)

def export_product_trades_per_year():
    config = load_config()
    export_product_trades_per_year_core(config)

def export_countries_and_products():
    config = load_config()
    from data_generator.core import export_countries_and_products
    export_countries_and_products(config)

def calculate_year_country_per_product_value():
    config = load_config()
    calculate_yearly_country_per_product_value(config)

def calculate_yearly_product_market_concentration():
    config = load_config()
    from data_generator.core import calculate_yearly_product_market_concentration
    calculate_yearly_product_market_concentration(config)

def calculate_betweenness_centrality():
    config = load_config()
    from data_generator.core import calculate_betweenness_centrality
    calculate_betweenness_centrality(config)

def export_country_trade_value_all_years():
    config = load_config()
    from data_generator.core import export_country_trade_value_all_years
    export_country_trade_value_all_years(config)

def export_market_concentrations():
    config = load_config()
    from data_generator.core import export_market_concentrations
    export_market_concentrations(config)

def export_trade_balances_per_country():
    config = load_config()
    from data_generator.core import export_trade_balances_per_country
    export_trade_balances_per_country(config)

def export_market_sizes_per_year():
    config = load_config()
    from data_generator.core import export_market_sizes_per_year
    export_market_sizes_per_year(config)

def export_betweenness_centrality():
    config = load_config()
    from data_generator.core import export_betweenness_centrality
    export_betweenness_centrality(config)