def market_concentrations(session, export_path):
    """Export market concentrations as JSON."""
    from data_generator.db_helpers.calculations import market_concentration

    concentrations = session.query(market_concentration.MarketConcentration).all()
    
    import orjson
    import os
    dir_path = os.path.abspath(export_path)
    os.makedirs(dir_path, exist_ok=True)
    file_path = os.path.join(dir_path, "market_concentrations.json")
    data = [
        {
            "year": mc.year,
            "product_code": mc.product_code,
            "exporter_hhi_index": float(mc.exporter_hhi_index),
            "importer_hhi_index": float(mc.importer_hhi_index)
        }
        for mc in concentrations
    ]
    with open(file_path, 'wb') as f:
        f.write(orjson.dumps(data, option=orjson.OPT_INDENT_2))
    print(f"Exported {len(data)} market concentrations to {file_path}")