from itertools import product
from data_generator.db_helpers.calculations.country_product_yearly_value import NetMarketCountryTradeBalance
from data_generator.db_helpers.models import BaciTradeRow, Base
from sqlalchemy import BigInteger, Numeric, all_, func, Column, Integer, String


class BetweenessCentrality(Base):
    __tablename__ = 'betweenness_centralities'
    year = Column(Integer, nullable=False, primary_key=True)
    product_code = Column(String, nullable=False, primary_key=True)
    country_code = Column(String, nullable=False, primary_key=True)
    betweenness_index = Column(Numeric(10, 6), nullable=False)
    betweenness_index_including_endpoints = Column(Numeric(10, 6), nullable=False)

def add_betweenness_centrality_tables(session):
    """Create the betweenness centrality related tables in the database."""
    # Create the tables in the database
    BetweenessCentrality.__table__.create(bind=session.get_bind(), checkfirst=True)
    

def clear_betweenness_centrality_data(session):
    """Clear existing betweenness centrality data from the database."""
    session.query(BetweenessCentrality).delete()
    session.commit()

def calculate_betweenness_centrality(session):
    """Calculate and store betweenness centrality indices."""
    from tqdm import tqdm
    from collections import defaultdict
    
    print("Calculating betweenness centrality indices...")
    
    # Get distinct years and product codes
    years = [row[0] for row in session.query(BaciTradeRow.year).distinct().all()]
    #order so latest years first
    years.sort(reverse=True)
    product_codes = [row[0] for row in session.query(BaciTradeRow.product_code).distinct().all()]
    
    import networkx as nx
    
    
    for year in tqdm(years, desc="Processing years", position=0):
        for product_code in tqdm(product_codes, desc=f"Year {year}", position=1, leave=False):
                
            trades = session.query(BaciTradeRow).filter(
                BaciTradeRow.year == year,
                BaciTradeRow.product_code == product_code
            ).all()

            max_trade_value = trades and max(trade.dollar_value for trade in trades) or 0
            
            # Calculate betweenness centrality for each country using networkx
            G = nx.DiGraph()
            for trade in trades:
                inverse_weight = max_trade_value - trade.dollar_value + 1  # to avoid zero weight
                G.add_edge(trade.exporter_code, trade.importer_code, weight=inverse_weight)
            betweenness = nx.betweenness_centrality(G, weight='weight', normalized=True)
            betweenness_including_endpoints = nx.betweenness_centrality(G, weight='weight', normalized=True, endpoints=True)
            
            # Store results in the database
            for country_code, index in betweenness.items():
                bc_entry = BetweenessCentrality(
                    year=year,
                    product_code=product_code,
                    country_code=country_code,
                    betweenness_index=index,
                    betweenness_index_including_endpoints=betweenness_including_endpoints.get(country_code, 0)
                )
                session.add(bc_entry)
            session.commit()
    print("Betweenness centrality calculation completed.")
