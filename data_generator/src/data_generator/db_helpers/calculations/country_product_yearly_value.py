from itertools import product
from data_generator.db_helpers.models import BaciTradeRow, Base
from sqlalchemy import BigInteger, Numeric, all_, func, Column, Integer, String

class NetMarketCountryTradeBalance(Base):
    __tablename__ = 'net_market_country_trade_balances'
    year = Column(Integer, nullable=False, primary_key=True)
    country_code = Column(String, nullable=False, primary_key=True)
    product_code = Column(String, nullable=False, primary_key=True)
    net_trade_dollar_value = Column(BigInteger, nullable=False)
    net_trade_metric_tonnes = Column(Numeric(20, 3), nullable=False)

def add_year_country_product_value_tables(session):
    """Create the country product yearly value related tables in the database."""
    # Create the tables in the database
    NetMarketCountryTradeBalance.__table__.create(bind=session.get_bind(), checkfirst=True)    

def calculate_net_market_country_trade_balance(session):
    """Calculate and store net market country trade balances."""
    from tqdm import tqdm
    
    # Get all unique years
    years = session.query(BaciTradeRow.year).distinct().order_by(BaciTradeRow.year).all()
    years = [year[0] for year in years]
    
    print(f"Processing {len(years)} years: {years}")
    
    # Process each year separately
    for year in tqdm(years, desc="Processing years", unit="year"):
        print(f"\nProcessing year {year}...")
        
        # Export aggregations for this year
        subquery_exports = session.query(
            BaciTradeRow.year.label('year'),
            BaciTradeRow.exporter_code.label('country_code'),
            BaciTradeRow.product_code.label('product_code'),
            func.sum(BaciTradeRow.dollar_value).label('export_dollar_value'),
            func.sum(BaciTradeRow.metric_tonne_quantity).label('export_metric_tonnes')
        ).filter(
            BaciTradeRow.year == year
        ).group_by(
            BaciTradeRow.year,
            BaciTradeRow.exporter_code,
            BaciTradeRow.product_code
        ).subquery()
        
        # Import aggregations for this year
        subquery_imports = session.query(
            BaciTradeRow.year.label('year'),
            BaciTradeRow.importer_code.label('country_code'),
            BaciTradeRow.product_code.label('product_code'),
            func.sum(BaciTradeRow.dollar_value).label('import_dollar_value'),
            func.sum(BaciTradeRow.metric_tonne_quantity).label('import_metric_tonnes')
        ).filter(
            BaciTradeRow.year == year
        ).group_by(
            BaciTradeRow.year,
            BaciTradeRow.importer_code,
            BaciTradeRow.product_code
        ).subquery()
        
        join_condition = (
            (subquery_exports.c.year == subquery_imports.c.year) &
            (subquery_exports.c.country_code == subquery_imports.c.country_code) &
            (subquery_exports.c.product_code == subquery_imports.c.product_code)
        )
        
        # Calculate net balances for this year
        results = session.query(
            func.coalesce(subquery_exports.c.year, subquery_imports.c.year).label('year'),
            func.coalesce(subquery_exports.c.country_code, subquery_imports.c.country_code).label('country_code'),
            func.coalesce(subquery_exports.c.product_code, subquery_imports.c.product_code).label('product_code'),
            (func.coalesce(subquery_exports.c.export_dollar_value, 0) - func.coalesce(subquery_imports.c.import_dollar_value, 0)).label('net_trade_dollar_value'),
            (func.coalesce(subquery_exports.c.export_metric_tonnes, 0) - func.coalesce(subquery_imports.c.import_metric_tonnes, 0)).label('net_trade_metric_tonnes')
        ).outerjoin(
            subquery_imports,
            join_condition
        ).all()
        
        # Store results for this year
        print(f"  Inserting {len(results):,} records for year {year}...")
        for row in results:
            balance = NetMarketCountryTradeBalance(
                year=row.year,
                country_code=row.country_code,
                product_code=row.product_code,
                net_trade_dollar_value=row.net_trade_dollar_value,
                net_trade_metric_tonnes=row.net_trade_metric_tonnes
            )
            session.add(balance)
        
        # Commit after each year to free memory
        session.commit()
        print(f"  Year {year} complete.")
    
    print("\nNet market country trade balance calculation complete.")