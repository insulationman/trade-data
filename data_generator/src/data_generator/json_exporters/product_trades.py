from data_generator.db_helpers.models import BaciProduct, BaciTradeRow
import orjson
import time
import os
from sqlalchemy import text
from tqdm import tqdm


def product_trades(session, export_path, batch_size=1):
    """Generate a JSON file with all trades for a product."""
    dir_path = os.path.abspath(export_path)
    os.makedirs(dir_path, exist_ok=True)
    
    print("Fetching distinct products...")
    products = session.query(BaciProduct.code).distinct().all()
    products = [p[0] for p in products]
    print(f"Found {len(products)} products to process")

    total_batches = (len(products) + batch_size - 1) // batch_size
    
    with tqdm(total=len(products), desc="Exporting products", unit="product") as pbar:
        for batch_idx in range(0, len(products), batch_size):
            batch_start_time = time.time()
            batch_products = products[batch_idx:batch_idx + batch_size]
            batch_num = (batch_idx // batch_size) + 1
            
            # Use SQL to group and aggregate data directly in the database
            # Create placeholders for IN clause (SQLite doesn't support binding lists directly)
            placeholders = ','.join([':p' + str(i) for i in range(len(batch_products))])
            sql = text(f"""
                SELECT 
                    product_code,
                    json_group_array(
                        json_object(
                            'year', year,
                            'exporter_code', exporter_code,
                            'importer_code', importer_code,
                            'product_code', product_code,
                            'trade_dollar_value', CAST(dollar_value AS INTEGER),
                            'quantity_metric_tonnes', CAST(metric_tonne_quantity AS REAL)
                        )
                    ) as trade_data
                FROM baci_trade_rows
                WHERE product_code IN ({placeholders})
                GROUP BY product_code
            """)
            
            # Create parameter dict with individual parameters
            params = {f'p{i}': code for i, code in enumerate(batch_products)}
            result = session.execute(sql, params)
            
            # Write files directly from SQL results
            files_written = 0
            for row in result:
                product_code, trade_data_json = row
                file_name = f"{product_code}.json"
                file_path = os.path.join(dir_path, file_name)
                with open(file_path, 'wb') as f:
                    # Parse and re-format JSON with indentation
                    import json
                    data = json.loads(trade_data_json)
                    f.write(json.dumps(data, indent=2).encode('utf-8'))
                files_written += 1
            
            pbar.update(len(batch_products))
            batch_elapsed = time.time() - batch_start_time
            pbar.set_postfix({"files": files_written, "time": f"{batch_elapsed:.1f}s"})
            
    print(f"Export completed to {dir_path}")
                