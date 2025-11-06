import os
import json
import boto3
import pymysql
from decimal import Decimal
import logging

# ------------------------
# Setup Logging
# ------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ------------------------
# Environment Variables
# ------------------------
RDS_HOST = os.environ.get('RDS_HOST')
RDS_DATABASE = os.environ.get('RDS_DATABASE')
DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE')

# ------------------------
# AWS Clients
# ------------------------
ssm = boto3.client('ssm')
dynamo = boto3.resource('dynamodb')
table = dynamo.Table(DYNAMO_TABLE)

# Cache for SSM values (so SSM isn't called on every Lambda execution)
_cached_rds_user = None
_cached_rds_password = None

# ------------------------
# Fetch Secure Values from SSM Parameter Store
# ------------------------
def get_ssm_parameter(name):
    response = ssm.get_parameter(
        Name=name,
        WithDecryption=True  # Ensures SecureString values are decrypted
    )
    return response['Parameter']['Value']

def get_db_credentials():
    global _cached_rds_user, _cached_rds_password

    if not _cached_rds_user:
        _cached_rds_user = get_ssm_parameter('/ubuntucrafts/db_username')
        logger.info("Fetched RDS username from SSM")

    if not _cached_rds_password:
        _cached_rds_password = get_ssm_parameter('/ubuntucrafts/db_password')
        logger.info("Fetched RDS password from SSM")

    return _cached_rds_user, _cached_rds_password

# ------------------------
# RDS Connection
# ------------------------
def get_rds_connection():
    try:
        rds_user, rds_password = get_db_credentials()
        conn = pymysql.connect(
            host=RDS_HOST,
            user=rds_user,
            password=rds_password,
            database=RDS_DATABASE,
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=5
        )
        logger.info("RDS connection established")
        return conn
    except pymysql.MySQLError as e:
        logger.error(f"RDS connection failed: {str(e)}")
        raise

# ------------------------
# Helper to serialize Decimal in DynamoDB
# ------------------------
def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

# ------------------------
# Product Handlers (DynamoDB)
# ------------------------
def list_products(event, context):
    try:
        response = table.scan()
        items = response.get('Items', [])
        logger.info(f"Fetched {len(items)} products")
        return {'statusCode': 200, 'body': json.dumps(items, default=decimal_default)}
    except Exception as e:
        logger.error(f"Error listing products: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps({'error': 'Failed to list products', 'details': str(e)})}

def get_product(event, context):
    try:
        product_id = event['pathParameters']['id']
        response = table.get_item(Key={'product_id': product_id})
        item = response.get('Item')
        if not item:
            logger.warning(f"Product {product_id} not found")
            return {'statusCode': 404, 'body': json.dumps({'error': 'Product not found'})}
        return {'statusCode': 200, 'body': json.dumps(item, default=decimal_default)}
    except Exception as e:
        logger.error(f"Error fetching product: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps({'error': 'Failed to get product', 'details': str(e)})}

def create_product(event, context):
    try:
        body = json.loads(event.get('body', '{}'))
        if 'price' in body:
            body['price'] = Decimal(str(body['price']))
        if 'stock' in body:
            body['stock'] = Decimal(str(body['stock']))
        table.put_item(Item=body)
        logger.info(f"Created product {body.get('product_id')}")
        return {'statusCode': 200, 'body': json.dumps({'message': 'Product created successfully'})}
    except Exception as e:
        logger.error(f"Error creating product: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps({'error': 'Failed to create product', 'details': str(e)})}

def update_product(event, context):
    try:
        product_id = event['pathParameters']['id']
        body = json.loads(event.get('body', '{}'))
        price = Decimal(str(body.get('price', 0)))
        stock = Decimal(str(body.get('stock', 0)))
        name = body.get('name', '')
        table.update_item(
            Key={'product_id': product_id},
            UpdateExpression="SET #n=:n, price=:p, stock=:s",
            ExpressionAttributeNames={'#n': 'name'},
            ExpressionAttributeValues={':n': name, ':p': price, ':s': stock}
        )
        logger.info(f"Updated product {product_id}")
        return {'statusCode': 200, 'body': json.dumps({'message': 'Product updated successfully'})}
    except Exception as e:
        logger.error(f"Error updating product {product_id}: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps({'error': 'Failed to update product', 'details': str(e)})}

def delete_product(event, context):
    try:
        product_id = event['pathParameters']['id']
        table.delete_item(Key={'product_id': product_id})
        logger.info(f"Deleted product {product_id}")
        return {'statusCode': 200, 'body': json.dumps({'message': 'Product deleted successfully'})}
    except Exception as e:
        logger.error(f"Error deleting product {product_id}: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps({'error': 'Failed to delete product', 'details': str(e)})}

# ------------------------
# Checkout Handler (RDS)
# ------------------------
def checkout(event, context):
    try:
        body = json.loads(event.get('body', '{}'))
        user_id = body['user_id']
        items = body['items']
        total_amount = sum([i['price'] * i['quantity'] for i in items])

        conn = get_rds_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("INSERT INTO Orders (user_id, total_amount) VALUES (%s, %s)", (user_id, total_amount))
                order_id = cursor.lastrowid

                for item in items:
                    cursor.execute(
                        "INSERT INTO OrderItems (order_id, product_id, quantity, price) VALUES (%s, %s, %s, %s)",
                        (order_id, item['product_id'], item['quantity'], item['price'])
                    )
                conn.commit()
        finally:
            conn.close()

        logger.info(f"Checkout successful for order {order_id}")
        return {'statusCode': 200, 'body': json.dumps({'order_id': order_id, 'message': 'Checkout successful'})}
    except Exception as e:
        logger.error(f"Checkout failed: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps({'error': 'Checkout failed', 'details': str(e)})}

# ------------------------
# Router
# ------------------------
def lambda_handler(event, context):
    path = event.get("path", "")
    method = event.get("httpMethod", "")
    logger.info(f"Received request: {method} {path}")

    if path == "/products" and method == "GET":
        return list_products(event, context)
    elif path.startswith("/products/") and method == "GET":
        return get_product(event, context)
    elif path == "/products" and method == "POST":
        return create_product(event, context)
    elif path.startswith("/products/") and method == "PUT":
        return update_product(event, context)
    elif path.startswith("/products/") and method == "DELETE":
        return delete_product(event, context)
    elif path == "/checkout" and method == "POST":
        return checkout(event, context)
    else:
        return {'statusCode': 400, 'body': json.dumps({'error': 'Invalid route or method'})}
