from data_generator.db_helpers.calculations.betweenness_centrality import BetweenessCentrality


def betweenness_centrality(session, export_path):
    """Export market concentrations as JSON."""
    from data_generator.db_helpers.calculations import market_concentration
    import orjson
    import os
    from tqdm import tqdm
    
    dir_path = os.path.abspath(export_path)
    os.makedirs(dir_path, exist_ok=True)

    # Get distinct years
    years = [row[0] for row in session.query(BetweenessCentrality.year).distinct().all()]
    
    for year in tqdm(years, desc="Exporting betweenness centrality"):
        # Query centralities for this year only
        centralities = session.query(BetweenessCentrality).filter(
            BetweenessCentrality.year == year
        ).all()
        
        year_data = [
            {
                "product_code": entry.product_code,
                "country_code": entry.country_code,
                "betweenness_index": float(entry.betweenness_index)
            }
            for entry in centralities
        ]
        
        file_path = os.path.join(dir_path, f"{year}.json")
        with open(file_path, 'wb') as f:
            f.write(orjson.dumps(year_data, option=orjson.OPT_INDENT_2))
    
    print("Betweenness centrality data export completed.")

    