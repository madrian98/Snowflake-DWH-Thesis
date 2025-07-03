import random
import boto3
import pandas as pd
import os
from datetime import datetime
from faker import Faker
from dotenv import load_dotenv
from io import BytesIO


class DataGenerator:
    def __init__(self):
        self.fake = Faker(['pl_PL', 'en_US'])
        self.s3_client = None

        self.categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys', 'Food']
        self.subcategories = {
            'Electronics': ['Smartphones', 'Laptops', 'Tablets', 'Headphones', 'Cameras'],
            'Clothing': ['Shirts', 'Pants', 'Dresses', 'Shoes', 'Accessories'],
            'Home & Garden': ['Furniture', 'Kitchen', 'Bathroom', 'Garden Tools', 'Decor'],
            'Sports': ['Fitness', 'Outdoor', 'Team Sports', 'Water Sports', 'Winter Sports'],
            'Books': ['Fiction', 'Non-fiction', 'Educational', 'Children', 'Comics'],
            'Toys': ['Action Figures', 'Board Games', 'Educational', 'Outdoor', 'Electronic'],
            'Food': ['Snacks', 'Beverages', 'Frozen', 'Fresh', 'Canned']
        }
        self.brands = ['Apple', 'Samsung', 'Nike', 'Adidas', 'Sony', 'LG', 'Dell', 'HP', 'Canon', 'Nikon']
        self.regions = ['North', 'South', 'East', 'West', 'Central']
        self.order_statuses = ['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled']

    def setup_s3_client(self, aws_access_key: str, aws_secret_key: str, region: str = 'us-east-1'):
        """Configure S3 client"""
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region
        )

    def generate_customers(self, count: int = 1000) -> pd.DataFrame:
        """Generate customer data"""
        customers_data = []
        load_timestamp = datetime.now().isoformat()

        for i in range(count):
            customer = {
                "customer_id": f"CUST_{i + 1:06d}",
                "customer_name": self.fake.name(),
                "email": self.fake.email(),
                "phone": self.fake.phone_number(),
                "address": self.fake.address().replace('\\n', ', '),
                "city": self.fake.city(),
                "country": self.fake.country(),
                "registration_date": self.fake.date_between(start_date='-2y', end_date='today'),
                "load_timestamp": load_timestamp,
            }
            customers_data.append(customer)

        return pd.DataFrame(customers_data)

    def generate_suppliers(self, count: int = 100) -> pd.DataFrame:
        """Generate supplier data"""
        suppliers_data = []
        load_timestamp = datetime.now().isoformat()

        for i in range(count):
            supplier = {
                "supplier_id": f"SUPP_{i + 1:04d}",
                "supplier_name": self.fake.company(),
                "contact_name": self.fake.name(),
                "contact_email": self.fake.email(),
                "phone": self.fake.phone_number(),
                "address": self.fake.address().replace('\\n', ', '),
                "city": self.fake.city(),
                "country": self.fake.country(),
                "load_timestamp": load_timestamp,
            }
            suppliers_data.append(supplier)

        return pd.DataFrame(suppliers_data)

    def generate_products(self, count: int = 500, supplier_count: int = 100) -> pd.DataFrame:
        """Generate product data"""
        products_data = []
        load_timestamp = datetime.now().isoformat()

        for i in range(count):
            category = random.choice(self.categories)
            subcategory = random.choice(self.subcategories[category])
            cost_price = round(random.uniform(10, 500), 2)
            list_price = round(cost_price * random.uniform(1.2, 2.5), 2)

            supplier_id = f"SUPP_{random.randint(1, supplier_count):04d}"

            product = {
                "product_id": f"PROD_{i + 1:06d}",
                "product_name": f"{random.choice(self.brands)} {subcategory} {self.fake.word().title()}",
                "category": category,
                "subcategory": subcategory,
                "brand": random.choice(self.brands),
                "supplier_id": supplier_id,
                "cost_price": cost_price,
                "list_price": list_price,
                "load_timestamp": load_timestamp,
            }
            products_data.append(product)

        return pd.DataFrame(products_data)

    def generate_orders(self, customer_count: int = 1000, count: int = 2000) -> pd.DataFrame:
        """Generate order data"""
        orders_data = []
        load_timestamp = datetime.now().isoformat()

        for i in range(count):
            customer_id = f"CUST_{random.randint(1, customer_count):06d}"
            order = {
                "order_id": f"ORD_{i + 1:08d}",
                "customer_id": customer_id,
                "order_date": self.fake.date_between(start_date='-1y', end_date='today'),
                "order_status": random.choice(self.order_statuses),
                "total_amount": round(random.uniform(50, 2000), 2),
                "shipping_address": self.fake.address().replace('\\n', ', '),
                "load_timestamp": load_timestamp,
            }
            orders_data.append(order)

        return pd.DataFrame(orders_data)

    def generate_sales(self, customer_count: int = 1000, product_count: int = 500, count: int = 5000) -> pd.DataFrame:
        """Generate sales data"""
        sales_reps = [self.fake.name() for _ in range(50)]
        sales_data = []
        load_timestamp = datetime.now().isoformat()

        for i in range(count):
            customer_id = f"CUST_{random.randint(1, customer_count):06d}"
            product_id = f"PROD_{random.randint(1, product_count):06d}"
            quantity = random.randint(1, 10)
            unit_price = round(random.uniform(20, 800), 2)
            total_amount = round(quantity * unit_price, 2)

            sale = {
                "sale_id": f"SALE_{i + 1:08d}",
                "customer_id": customer_id,
                "product_id": product_id,
                "sale_date": self.fake.date_between(start_date='-1y', end_date='today'),
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": total_amount,
                "sales_rep": random.choice(sales_reps),
                "region": random.choice(self.regions),
                "load_timestamp": load_timestamp,
            }
            sales_data.append(sale)

        return pd.DataFrame(sales_data)

    def upload_parquet_to_s3(self, df: pd.DataFrame, bucket_name: str, file_key: str):
        """Upload DataFrame as Parquet to S3"""
        if not self.s3_client:
            raise Exception("S3 client not configured. Use setup_s3_client() first.")

        try:
            # Convert DataFrame to Parquet in memory
            buffer = BytesIO()
            df.to_parquet(buffer, engine='pyarrow', index=False)
            buffer.seek(0)

            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=file_key,
                Body=buffer.getvalue(),
                ContentType='application/octet-stream'
            )
            print(f" Successfully uploaded {file_key} to bucket {bucket_name}")
            print(f" Number of records: {len(df):,}")
            print(f" File size: {len(buffer.getvalue()) / 1024 / 1024:.2f} MB")

        except Exception as e:
            print(f" Error uploading {file_key}: {str(e)}")

    def save_parquet_locally(self, df: pd.DataFrame, file_path: str):
        """Save DataFrame as Parquet file locally"""
        try:
            df.to_parquet(file_path, engine='pyarrow', index=False)
            print(f" Successfully saved {file_path} locally")
            print(f" Number of records: {len(df):,}")

            file_size = os.path.getsize(file_path) / 1024 / 1024
            print(f"   File size: {file_size:.2f} MB")

        except Exception as e:
            print(f" Error saving {file_path}: {str(e)}")

    def generate_and_upload_all_data(self, bucket_name: str,
                                     customer_count: int = 1000,
                                     supplier_count: int = 100,
                                     product_count: int = 500,
                                     order_count: int = 2000,
                                     sales_count: int = 5000,
                                     save_locally: bool = False):
        """Generate and upload all data"""

        print(" Starting data generation...")
        print(" Generating customers...")
        customers_df = self.generate_customers(customer_count)

        print(" Generating suppliers...")
        suppliers_df = self.generate_suppliers(supplier_count)

        print(" Generating products...")
        products_df = self.generate_products(product_count)

        print(" Generating orders...")
        orders_df = self.generate_orders(customer_count, order_count)

        print(" Generating sales...")
        sales_df = self.generate_sales(customer_count, product_count, sales_count)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if save_locally:
            print("\\n Saving files locally...")
            os.makedirs("data", exist_ok=True)

            self.save_parquet_locally(customers_df, f"data/customers_{timestamp}.parquet")
            self.save_parquet_locally(suppliers_df, f"data/suppliers_{timestamp}.parquet")
            self.save_parquet_locally(products_df, f"data/products_{timestamp}.parquet")
            self.save_parquet_locally(orders_df, f"data/orders_{timestamp}.parquet")
            self.save_parquet_locally(sales_df, f"data/sales_{timestamp}.parquet")

        print(" Uploading data to S3...")

        self.upload_parquet_to_s3(customers_df, bucket_name, f"raw_data/customers/customers_{timestamp}.parquet")
        self.upload_parquet_to_s3(suppliers_df, bucket_name, f"raw_data/suppliers/suppliers_{timestamp}.parquet")
        self.upload_parquet_to_s3(products_df, bucket_name, f"raw_data/products/products_{timestamp}.parquet")
        self.upload_parquet_to_s3(orders_df, bucket_name, f"raw_data/orders/orders_{timestamp}.parquet")
        self.upload_parquet_to_s3(sales_df, bucket_name, f"raw_data/sales/sales_{timestamp}.parquet")

        print(" All data has been generated and uploaded!")

        print(" Data Summary:")
        print(f" Customers: {len(customers_df):,} records")
        print(f" Suppliers: {len(suppliers_df):,} records")
        print(f" Products: {len(products_df):,} records")
        print(f" Orders: {len(orders_df):,} records")
        print(f" Sales: {len(sales_df):,} records")
        print(
            f" Total records: {len(customers_df) + len(suppliers_df) + len(products_df) + len(orders_df) + len(sales_df):,}")


def load_config():
    """Load configuration from .env file"""
    load_dotenv()

    config = {
        'aws_access_key': os.getenv('AWS_ACCESS_KEY_ID'),
        'aws_secret_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
        'aws_region': os.getenv('AWS_REGION', 'us-east-1'),
        's3_bucket': os.getenv('S3_BUCKET_NAME')
    }

    required_params = ['aws_access_key', 'aws_secret_key', 's3_bucket']
    missing_params = [param for param in required_params if not config[param]]

    if missing_params:
        print(f" Error: Missing required parameters in .env file: {', '.join(missing_params)}")
        print(" Check your .env file and make sure all required variables are set.")
        return None

    return config


def main():
    """Main function to run the data generator"""

    CUSTOMER_COUNT = 20000
    SUPPLIER_COUNT = 20000
    PRODUCT_COUNT = 30000
    ORDER_COUNT = 20000
    SALES_COUNT = 30000


    SAVE_LOCALLY = False

    print(" Loading configuration from .env file...")


    config = load_config()
    if not config:
        return

    print(" Configuration loaded successfully!")
    print(f"   AWS Region: {config['aws_region']}")
    print(f"   S3 Bucket: {config['s3_bucket']}")
    print(f"   Number of customers: {CUSTOMER_COUNT:,}")
    print(f"   Number of suppliers: {SUPPLIER_COUNT:,}")
    print(f"   Number of products: {PRODUCT_COUNT:,}")
    print(f"   Number of orders: {ORDER_COUNT:,}")
    print(f"   Number of sales: {SALES_COUNT:,}")
    print(f"   Save locally: {SAVE_LOCALLY}")

    generator = DataGenerator()

    try:
        generator.setup_s3_client(
            config['aws_access_key'],
            config['aws_secret_key'],
            config['aws_region']
        )

    
        generator.generate_and_upload_all_data(
            bucket_name=config['s3_bucket'],
            customer_count=CUSTOMER_COUNT,
            supplier_count=SUPPLIER_COUNT,
            product_count=PRODUCT_COUNT,
            order_count=ORDER_COUNT,
            sales_count=SALES_COUNT,
            save_locally=SAVE_LOCALLY
        )

    except Exception as e:
        print(f" An error occurred: {str(e)}")


if __name__ == "__main__":
    main()