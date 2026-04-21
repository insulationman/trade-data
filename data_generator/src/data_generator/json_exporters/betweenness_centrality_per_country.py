from data_generator.db_helpers.calculations.betweenness_centrality import BetweenessCentrality


def betweenness_centrality_per_country(session, export_path):
    """Export market concentrations as JSON."""
    from data_generator.db_helpers.calculations import market_concentration
    import orjson
    import os
    from tqdm import tqdm
    
    dir_path = os.path.abspath(export_path)
    os.makedirs(dir_path, exist_ok=True)

    # Get distinct country codes
    countries = [row[0] for row in session.query(BetweenessCentrality.country_code).distinct().all()]
    
    for country in tqdm(countries, desc="Exporting betweenness centrality per country"):
        # Query centralities for this country only
        centralities = session.query(BetweenessCentrality).filter(
            BetweenessCentrality.country_code == country
        ).all()
        
        country_data = [
            {
                "product_code": entry.product_code,
                "year": entry.year,
                "betweenness_index": float(entry.betweenness_index),
                "betweenness_index_including_endpoints": float(entry.betweenness_index_including_endpoints)
            }
            for entry in centralities
        ]
        
        file_path = os.path.join(dir_path, f"{country}.json")
        with open(file_path, 'wb') as f:
            f.write(orjson.dumps(country_data, option=orjson.OPT_INDENT_2))