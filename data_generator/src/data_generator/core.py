from data_generator.config import AppConfig
from data_generator.db_helpers import init_db, get_session, import_products
import os
from data_generator.db_helpers.baci_import import import_countries
from data_generator.db_helpers.baci_import import import_trade_rows
from data_generator.db_helpers.calculations.country_product_yearly_value import add_year_country_product_value_tables, calculate_net_market_country_trade_balance
from data_generator.db_helpers.calculations.market_concentration import add_market_concentration_tables, calculate_market_concentration, clear_market_concentration_data
from data_generator.json_exporters.betweenness_centrality_per_country import betweenness_centrality_per_country


def initalize_db(config:AppConfig):
    dir = config.db_folder
    db_name = config.db_name
    db_path = os.path.join(dir, db_name)
    init_db(db_path)

def import_baci_products(config:AppConfig):
    session = get_session(config.db_folder + '/' + config.db_name)
    baci_products_file_path = os.path.join(config.baci_directory, 'product_codes_HS12_V202501.csv')
    import_products(session, baci_products_file_path)

def import_baci_countries(config:AppConfig):
    session = get_session(config.db_folder + '/' + config.db_name)
    baci_countries_file_path = os.path.join(config.baci_directory, 'country_codes_V202501.csv')
    import_countries(session, baci_countries_file_path)

def import_baci_trade_rows(config:AppConfig):
    session = get_session(config.db_folder + '/' + config.db_name)
    dir = config.baci_directory
    import_trade_rows(session, dir)

def export_product_trades(config:AppConfig):
    from data_generator.json_exporters.product_trades import product_trades
    session = get_session(config.db_folder + '/' + config.db_name)
    product_trades(session, config.json_export_path + '/trades_by_product')

def export_product_trades_per_year(config:AppConfig):
    from data_generator.json_exporters.product_trades_per_year import product_trades_per_year
    session = get_session(config.db_folder + '/' + config.db_name)
    product_trades_per_year(session, config.json_export_path + '/product_trades_per_year')

def export_countries_and_products(config:AppConfig):
    from data_generator.json_exporters.countries_and_products import export_country_and_product_data
    session = get_session(config.db_folder + '/' + config.db_name)
    export_country_and_product_data(session, config.json_export_path + '/countries_and_products')

def calculate_yearly_country_per_product_value(config:AppConfig):
    session = get_session(config.db_folder + '/' + config.db_name)
    add_year_country_product_value_tables(session)
    calculate_net_market_country_trade_balance(session)
    
def calculate_yearly_product_market_concentration(config:AppConfig):
    session = get_session(config.db_folder + '/' + config.db_name)
    add_market_concentration_tables(session)
    clear_market_concentration_data(session)
    calculate_market_concentration(session)

def calculate_betweenness_centrality(config:AppConfig):
    from data_generator.db_helpers.calculations.betweenness_centrality import add_betweenness_centrality_tables, clear_betweenness_centrality_data, calculate_betweenness_centrality
    session = get_session(config.db_folder + '/' + config.db_name)
    add_betweenness_centrality_tables(session)
    clear_betweenness_centrality_data(session)
    calculate_betweenness_centrality(session)

def export_country_trade_value_all_years(config:AppConfig):
    from data_generator.json_exporters.country_product_value_all_years import export_country_product_value_all_years
    session = get_session(config.db_folder + '/' + config.db_name)
    export_country_product_value_all_years(session, config.json_export_path + '/country_product_value_all_years')

def export_market_concentrations(config:AppConfig):
    from data_generator.json_exporters.market_concentrations import market_concentrations
    session = get_session(config.db_folder + '/' + config.db_name)
    market_concentrations(session, config.json_export_path + '/market_concentrations')

def export_trade_balances_per_country(config:AppConfig):
    from data_generator.json_exporters.trade_balances_per_country import trade_balances_per_country
    session = get_session(config.db_folder + '/' + config.db_name)
    trade_balances_per_country(session, config.json_export_path + '/trade_balances_per_country')

def export_market_sizes_per_year(config:AppConfig):
    from data_generator.json_exporters.market_sizes_per_year import market_sizes_per_year
    session = get_session(config.db_folder + '/' + config.db_name)
    market_sizes_per_year(session, config.json_export_path + '/market_sizes_per_year')

def export_betweenness_centrality(config:AppConfig):
    from data_generator.json_exporters.betweenness_centrality_per_year import betweenness_centrality
    session = get_session(config.db_folder + '/' + config.db_name)
    betweenness_centrality_per_country(session, config.json_export_path + '/betweenness_centrality_per_country')


