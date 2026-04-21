from itertools import product
from data_generator.db_helpers.calculations.country_product_yearly_value import NetMarketCountryTradeBalance
from data_generator.db_helpers.models import BaciTradeRow, Base
from sqlalchemy import BigInteger, Numeric, all_, func, Column, Integer, String


class MarketConcentration(Base):
    __tablename__ = 'market_concentrations'
    year = Column(Integer, nullable=False, primary_key=True)
    product_code = Column(String, nullable=False, primary_key=True)
    exporter_hhi_index = Column(Numeric(4, 3), nullable=False)
    importer_hhi_index = Column(Numeric(4, 3), nullable=False)

def add_market_concentration_tables(session):
    """Create the market concentration related tables in the database."""
    # Create the tables in the database
    MarketConcentration.__table__.create(bind=session.get_bind(), checkfirst=True)

def clear_market_concentration_data(session):
    """Clear existing market concentration data from the database."""
    session.query(MarketConcentration).delete()
    session.commit()

def calculate_market_concentration(session):
    """Calculate and store market concentration indices."""
    from tqdm import tqdm
    from collections import defaultdict
    
    print("Calculating market concentration indices...")
    
    # Get distinct years and product codes
    years = [row[0] for row in session.query(BaciTradeRow.year).distinct().all()]
    product_codes = [row[0] for row in session.query(BaciTradeRow.product_code).distinct().all()]
    
    total_iterations = len(years) * len(product_codes)
    pbar = tqdm(total=total_iterations, desc="Calculating market concentration", unit="calculations")
    
    for year in years:
        # Query all balances for this year once
        print(f"\nLoading data for year {year}...")
        all_balances_for_year = session.query(
            NetMarketCountryTradeBalance.country_code,
            NetMarketCountryTradeBalance.product_code,
            NetMarketCountryTradeBalance.net_trade_dollar_value
        ).filter(
            NetMarketCountryTradeBalance.year == year
        ).all()
        
        # Group by product_code in memory
        balances_by_product = defaultdict(list)
        for balance in all_balances_for_year:
            balances_by_product[balance.product_code].append(balance)
        
        # Process each product for this year
        for product_code in product_codes:
            all_countries_balances = balances_by_product.get(product_code, [])

            # Calculate total exports by summing only positive net trade dollar values
            total_exports = sum(balance.net_trade_dollar_value for balance in all_countries_balances if balance.net_trade_dollar_value > 0)
            # Calculate total imports by summing only negative net trade dollar values
            total_imports = -sum(balance.net_trade_dollar_value for balance in all_countries_balances if balance.net_trade_dollar_value < 0)

            # Calculate HHI for exporters
            exporter_hhi = 0.0
            if total_exports > 0:
                for balance in all_countries_balances:
                    if balance.net_trade_dollar_value > 0:
                        market_share = balance.net_trade_dollar_value / total_exports
                        exporter_hhi += (market_share * market_share)
            # Calculate HHI for importers
            importer_hhi = 0.0
            if total_imports > 0:
                for balance in all_countries_balances:
                    if balance.net_trade_dollar_value < 0:
                        market_share = -balance.net_trade_dollar_value / total_imports
                        importer_hhi += (market_share * market_share)
            # Store in MarketConcentration table
            concentration = MarketConcentration(
                year=year,
                product_code=product_code,
                exporter_hhi_index=round(exporter_hhi, 3),
                importer_hhi_index=round(importer_hhi, 3)
            )
            session.add(concentration)
            pbar.update(1)
    pbar.close()
    print("Committing market concentration indices to database...")
    session.commit()
    print("Market concentration calculation complete.")
