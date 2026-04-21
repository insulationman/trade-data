from data_generator.db_helpers.models import BaciCountry, BaciProduct


def export_country_and_product_data(session, export_path):
    """Generate JSON files for countries and products."""
    import os
    import orjson
    from sqlalchemy import text
    from tqdm import tqdm

    dir_path = os.path.abspath(export_path)
    os.makedirs(dir_path, exist_ok=True)

    # Export countries data
    countries = session.query(BaciCountry).all()
    countries_data = [
        {
            "code": country.code,
            "name": country.name,
            "iso2": country.iso2,
            "iso3": country.iso3
        }
        for country in countries
    ]
    countries_file_path = os.path.join(dir_path, "countries.json")
    with open(countries_file_path, 'wb') as f:
        f.write(orjson.dumps(countries_data, option=orjson.OPT_INDENT_2))
    print(f"Exported {len(countries_data)} countries to {countries_file_path}")

    # Export products data
    print("Fetching distinct products...")
    products = session.query(BaciProduct).all()
    products_data = [
        {
            "code": product.code,
            "description": product.description
        }
        for product in products
    ]
    products_file_path = os.path.join(dir_path, "products.json")
    with open(products_file_path, 'wb') as f:
        f.write(orjson.dumps(products_data, option=orjson.OPT_INDENT_2))
    print(f"Exported {len(products_data)} products to {products_file_path}")
    print("Country and product data export completed.")