import csv
import os
from faker import Faker
import random
from datetime import datetime, timedelta
import s3fs
from pathlib import Path
from cloud_utils import S3_DIR
from db_utils import get_roasted_products

fake = Faker()

# Constants
NUM_USERS = 100
NUM_PRODUCTS = 50
NUM_TRANSACTIONS = 500
TODAY = datetime.today()

def get_true_cwd(s3=False):
    if s3:
        return S3_DIR
    else:
        return os.getcwd() + "/generated_data/"

def append_or_write(filename):
    """Return 'a' (append) or 'w' (write) depending on the file's existence."""
    return 'a' if os.path.exists(filename) else 'w'

def generate_id():
    """Generate a unique 5-digit integer ID."""
    return str(random.randint(10000, 99999))

def generate_users(filename, num_users, use_s3, fs):
    filename = get_true_cwd(use_s3) + filename
    mode = append_or_write(filename)
    user_ids = []
    
    with fs.open(filename, mode, newline='') if use_s3 else open(filename, mode, newline='') as csvfile:
        writer = csv.writer(csvfile)
        if mode == 'w':
            writer.writerow(["UserID", "Name", "Email", "SignUpDate"])
        
        for _ in range(num_users):
            user_id = generate_id()
            user_ids.append(user_id)
            writer.writerow([user_id, fake.name(), fake.email(), fake.date_between_dates(date_start=TODAY - timedelta(days=365), date_end=TODAY)])
    return user_ids

def generate_products(filename, num_products, use_s3, fs):
    filename = get_true_cwd(use_s3) + filename
    mode = append_or_write(filename)
    product_ids = []
    
    with fs.open(filename, mode, newline='') if use_s3 else open(filename, mode, newline='') as csvfile:
        writer = csv.writer(csvfile)
        if mode == 'w':
            writer.writerow(["ProductID", "ProductName", "Category", "Price"])
        
        categories = ["Electronics", "Clothing", "Groceries", "Books", "Toys"]
        
        for _ in range(num_products):
            product_id = generate_id()
            product_ids.append(product_id)
            writer.writerow([product_id, fake.unique.first_name(), random.choice(categories), round(random.uniform(5.0, 100.0), 2)])
    return product_ids

def generate_transactions(filename, num_transactions, user_ids, use_s3, fs):
    filename = get_true_cwd(use_s3) + filename
    mode = append_or_write(filename)
    
    with open(filename, mode, newline='') if not use_s3 else fs.open(filename, mode, newline='') as csvfile:
        writer = csv.writer(csvfile)
        if mode == 'w':
            writer.writerow(["TransactionID", "UserID", "ProductID", "OrderStatus", "TransactionDate"])
        products = [product["id"] for product in get_roasted_products()]
        statuses = ["Ordered", "Payment Processing", "Payment Recieved", "Roasting", "Roasted", "Packaged", "Shipped"]
        for _ in range(num_transactions):
            writer.writerow([generate_id(), random.choice(user_ids), random.choice(products), random.randint(1,5), random.choice(statuses), fake.date_between_dates(date_start=TODAY - timedelta(days=30), date_end=TODAY)])

def generate_data(user_path, retail_products_path, transactions_path, target=None):
    use_s3 = False
    s3 = None
    if target == "s3":
        use_s3 = True
        s3 = s3fs.S3FileSystem(anon=False)
    user_ids = generate_users(user_path, NUM_USERS,  use_s3, fs=s3)
    generate_products(retail_products_path, NUM_PRODUCTS,  use_s3, fs=s3 )
    generate_transactions(transactions_path, NUM_TRANSACTIONS, user_ids, use_s3, fs=s3 )


