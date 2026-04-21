from .models import BaciProduct


def import_products(session, baci_products_file_path):
    """Import BACI products from a CSV file into the database."""
    import csv
    with open(baci_products_file_path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        for row in reader:
            product = BaciProduct(code=row[0], description=row[1])
            session.add(product)
    session.commit()

def import_countries(session, baci_countries_file_path):
    """Import BACI countries from a CSV file into the database."""
    import csv
    from .models import BaciCountry
    with open(baci_countries_file_path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        for row in reader:
            country = BaciCountry(code=row[0], name=row[1], iso2=row[2], iso3=row[3])
            session.add(country)
    session.commit()

def import_trade_rows(session, baci_trade_rows_dir, chunk_size=100_000):
    """Import BACI trade rows from all CSV files that start with BACI_ into the database."""
    import csv
    from .models import BaciTradeRow
    import os
    from decimal import Decimal
    from tqdm import tqdm
    
    # Get list of files to process
    files = [f for f in os.listdir(baci_trade_rows_dir) if f.startswith('BACI_') and f.endswith('.csv')]
    ##log and end if no files found
    if not files:
        print(f"No BACI trade row files found in the specified directory ({baci_trade_rows_dir})")
        return

    processed_files = 0
    for filename in files:
        print(f"Processing file {processed_files + 1}/{len(files)}")
        file_path = os.path.join(baci_trade_rows_dir, filename)
        
        # Count total rows in file
        with open(file_path, 'r', encoding='utf-8') as file:
            row_count = sum(1 for _ in file) - 1  # Subtract 1 for header
        
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)  # Skip header
            
            bulk_data = []
            total_rows = 0
            
            # Progress bar for this file
            with tqdm(total=row_count, desc=f"Importing {filename}", unit="rows") as pbar:
                for row in reader:
                    bulk_data.append({
                        'year': int(row[0]),
                        'exporter_code': row[1],
                        'importer_code': row[2],
                        'product_code': row[3],
                        'dollar_value': int(Decimal(row[4]) * 1000),
                        'metric_tonne_quantity': Decimal(row[5]) if row[5] else Decimal('0')
                    })
                    
                    # Insert when chunk size is reached
                    if len(bulk_data) >= chunk_size:
                        session.bulk_insert_mappings(BaciTradeRow, bulk_data)
                        session.commit()
                        total_rows += len(bulk_data)
                        pbar.update(len(bulk_data))
                        bulk_data = []
                
                # Insert remaining rows
                if bulk_data:
                    session.bulk_insert_mappings(BaciTradeRow, bulk_data)
                    session.commit()
                    total_rows += len(bulk_data)
                    pbar.update(len(bulk_data))
