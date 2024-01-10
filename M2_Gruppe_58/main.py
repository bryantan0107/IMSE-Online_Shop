from datetime import datetime, date, timedelta
from flask import Flask, render_template, request, redirect, url_for
from pymongo import MongoClient
from decimal import Decimal
import mysql.connector
import pymongo
import random

app = Flask(__name__)


db = mysql.connector.connect(
    user="user", password="password", host="sql", port="3306", database="sql_db")

mongodb_client = pymongo.MongoClient('mongodb://user:password@mongodb:27017/')
mongo_db = mongodb_client["mongo_db"]

is_migrated = 0


def create_tables(db):

    create_sql_tables = [
        '''
            CREATE TABLE user (
                user_id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(50) UNIQUE,
                email VARCHAR(50) UNIQUE,
                password VARCHAR(50)
            );
        ''',
        '''
            CREATE TABLE customer (
                phone_number BIGINT,
                delivery_address VARCHAR(100),
                bonus_points INT,
                user_id INT AUTO_INCREMENT,
                FOREIGN KEY (user_id) REFERENCES user(user_id)
            );
        ''',
        '''
           CREATE TABLE normal_account (
                delivery_fee DECIMAL(10,2),
                point_limit INT,
                user_id INT AUTO_INCREMENT,
                FOREIGN KEY (user_id) REFERENCES user(user_id)
            );
        ''',

        '''
            CREATE TABLE premium_account (
                invitation VARCHAR(50),
                discount DECIMAL(10,2),
                user_id INT,
                FOREIGN KEY (user_id) REFERENCES user(user_id)
            );
        ''',
        '''
            CREATE TABLE merchant (
                merchant_name VARCHAR(100),
                website VARCHAR(50) ,
                user_id INT AUTO_INCREMENT,
                FOREIGN KEY (user_id) REFERENCES user(user_id)
            );

        ''',
        '''
            CREATE TABLE item (
                item_id INT PRIMARY KEY,
                description VARCHAR(200),
                price DECIMAL(10,2),
                category VARCHAR(200)     

            );
        ''',
        '''
            CREATE TABLE orders (
                order_id INT AUTO_INCREMENT PRIMARY KEY,
                quantity INT,
                total_price INT,
                delivery_date DATE,
                user_id INT,
                FOREIGN KEY (user_id) REFERENCES user(user_id)

            );
        ''',

        '''
                CREATE TABLE review (
                    review_id INT AUTO_INCREMENT PRIMARY KEY,
                    publish_timestamp TIMESTAMP,
                    title VARCHAR(200),
                    description VARCHAR(200),
                    rating INT,
                    user_id INT,
                    item_id INT,
                    FOREIGN KEY (user_id) REFERENCES user(user_id)

                );
        ''',
        '''
                CREATE TABLE comment (
                    comment_id INT PRIMARY KEY,
                    publish_timestamp TIMESTAMP,
                    content VARCHAR(200),
                    review_id INT,
                    user_id INT,
                    FOREIGN KEY (review_id) REFERENCES review(review_id),
                    FOREIGN KEY (user_id) REFERENCES user(user_id)

                );
        
          
        ''',
        '''
                CREATE TABLE orderItem (
                    order_id INT,
                    item_id INT,
                    FOREIGN KEY (order_id) REFERENCES orders(order_id),
                    FOREIGN KEY (item_id) REFERENCES item (item_id)
                );
    '''
    ]

    cursor = db.cursor()

    for table in create_sql_tables:
        cursor.execute(table)
        print(str(table))

    db.commit()
    cursor.close()
    return


def drop_tables(db):

    drop_sql_tables = [
        "SET FOREIGN_KEY_CHECKS = 0;",
        "Drop TABLE IF EXISTS orderItem;",
        "DROP TABLE IF EXISTS comment;",
        "DROP TABLE IF EXISTS review;",
        "DROP TABLE IF EXISTS orders;",
        "DROP TABLE IF EXISTS item;",
        "DROP TABLE IF EXISTS merchant;",
        "DROP TABLE IF EXISTS premium_account;",
        "DROP TABLE IF EXISTS normal_account;",
        "DROP TABLE IF EXISTS customer;",
        "DROP TABLE IF EXISTS user;",
        "SET FOREIGN_KEY_CHECKS = 0;"
    ]

    cursor = db.cursor()

    for table in drop_sql_tables:
        cursor.execute(table)
        print(str(table))

    db.commit()
    cursor.close()
    return


def insert_data(db):

    with open("data/adresse.txt") as file:
        adresse_list = file.read().split("\n")
    with open("data/bonus_points.txt") as file:
        bonus_points_list = file.read().split("\n")
    with open("data/category.txt") as file:
        category_list = file.read().split("\n")
    with open("data/content.txt") as file:
        content_list = file.read().split("\n")
    with open("data/date.txt") as file:
        date_list = file.read().split("\n")
    with open("data/email.txt") as file:
        email_list = file.read().split("\n")
    with open("data/item.txt") as file:
        item_list = file.read().split("\n")
    with open("data/merchant_name.txt") as file:
        merchant_name_list = file.read().split("\n")
    with open("data/phone_number.txt") as file:
        phone_number_list = file.read().split("\n")
    with open("data/price.txt") as file:
        price_list = file.read().split("\n")
    with open("data/publish_timestamp.txt") as file:
        publish_timestamp_list = file.read().split("\n")
    with open("data/quantity.txt") as file:
        quantity_list = file.read().split("\n")
    with open("data/review_desc.txt") as file:
        review_desc_list = file.read().split("\n")
    with open("data/review_title.txt") as file:
        review_title_list = file.read().split("\n")
    with open("data/username.txt") as file:
        username_list = file.read().split("\n")
    with open("data/website.txt") as file:
        website_list = file.read().split("\n")
    with open("data/passwords.txt") as file:
        passwords_list = file.read().split("\n")

    cursor = db.cursor()

    # user ( user_id,username ,email )

    for i in range(0, 100):
        random_password = passwords_list[i]
        random_username = username_list[i]
        random_email = email_list[i]

        sql = "INSERT INTO user (username,email,password) VALUES (%s, %s,%s)"

        val = (random_username, random_email, random_password)
        cursor.execute(sql, val)

    # customer (phone_number, delivery_address,bonus_points ,user_id)

    bonus_points_list = [int(points) for points in bonus_points_list]

    for i in range(0, 100):

        random_phone_number = phone_number_list[random.randrange(
            len(phone_number_list))]
        random_adresse = adresse_list[random.randrange(len(adresse_list))]
        random_bonus_points_list = bonus_points_list[random.randrange(
            len(bonus_points_list))]
        sql = "INSERT INTO customer (phone_number, delivery_address,bonus_points ) VALUES (%s, %s, %s)"
        val = (random_phone_number, random_adresse, random_bonus_points_list)
        cursor.execute(sql, val)

    # normal_account (delivery_fee, point_limit,user_id)

    for i in range(0, 100):
        delivery_fee = 5
        point_limit = 100

        sql = "INSERT INTO normal_account (delivery_fee, point_limit,user_id) VALUES (%s, %s, %s)"
        val = (delivery_fee, point_limit, i)
        cursor.execute(sql, val)

    # premium_account (invitation,discount,user_id)

    for i in range(0, 100):
        invitation = 'invitationlink'
        discount = 0.1
        sql = "INSERT INTO  premium_account(invitation,discount,user_id) VALUES (%s, %s, %s)"
        val = (invitation, discount, i)
        cursor.execute(sql, val)

    # merchant (merchant_name,website,user_id)

    for i in range(0, 100):
        random_merchant_name = merchant_name_list[random.randrange(
            len(merchant_name_list))]
        random_website = website_list[random.randrange(len(website_list))]

        sql = "INSERT INTO merchant (merchant_name,website) VALUES (%s, %s)"
        val = (random_merchant_name, random_website)
        cursor.execute(sql, val)

    # item (item_id ,description,price,category)

    price_list = [float(price) for price in price_list]

    for i in range(0, 100):
        random_item = item_list[i]
        random_price = price_list[random.randrange(len(price_list))]
        random_category = category_list[i]
        sql = "INSERT INTO item (item_id ,description,price,category) VALUES (%s,%s, %s, %s)"
        val = (i, random_item, random_price, random_category)
        cursor.execute(sql, val)

    # order (order_id,quantity,total_price,delivery_date,user_id)

    price_list = [float(price) for price in price_list]
    quantity_list = [int(quantity) for quantity in quantity_list]

    for i in range(0, 100):
        random_price = price_list[random.randrange(len(price_list))]
        date_idx = random.randrange(len(date_list))
        random_date = datetime.strptime(date_list[date_idx], '%d.%m.%Y').date()
        random_quantity = quantity_list[random.randrange(len(quantity_list))]

        sql = "INSERT INTO orders (quantity,total_price,delivery_date,user_id) VALUES ( %s, %s, %s,%s)"
        val = (random_quantity, random_price, random_date, i)
        cursor.execute(sql, val)

    # review (review_id,publish_timestamp ,title,description,rating,user_id)

    for i in range(0, 100):
        # random_publish_timestamp=
        random_review_desc = review_desc_list[random.randrange(
            len(review_desc_list))]
        random_review_title = review_desc_list[random.randrange(
            len(review_title_list))]
        random_publish_timestamp = publish_timestamp_list[random.randrange(
            len(publish_timestamp_list))]
        sql = "INSERT INTO review (publish_timestamp ,title,description,rating,user_id,item_id) VALUES (%s,%s, %s, %s,%s,%s)"
        val = (random_publish_timestamp, random_review_desc,
               random_review_title, random.randint(3, 5), random.randint(0, 100), random.randint(0, 49))
        cursor.execute(sql, val)

    # comment (comment_id,publish_timestamp,content,review_id,user_id)

    for i in range(0, 100):
        random_publish_timestamp = publish_timestamp_list[random.randrange(
            len(publish_timestamp_list))]
        random_content = content_list[random.randrange(len(content_list))]
        sql = "INSERT INTO comment (comment_id,publish_timestamp,content,review_id,user_id) VALUES (%s, %s, %s,%s,%s)"
        val = (i, random_publish_timestamp, random_content, i, i)
        cursor.execute(sql, val)

    # orderItem (order_id, item_id)

    for i in range(0, 100):

        sql = "INSERT INTO orderItem (item_id,order_id) VALUES (%s, %s)"
        val = (i, i)
        cursor.execute(sql, val)

    db.commit()
    cursor.close()
    return


def testing_insert_data():
    global is_migrated
    is_migrated = 0
    cursor = db.cursor()

    drop_tables(db)
    create_tables(db)
    insert_data(db)

    cursor.execute('SELECT * FROM user')
    table = cursor.fetchall()
    print("--- user BEGIN ---")
    for row in table:
        print(row)
    print("--- user END ---")

    cursor.execute('SELECT * FROM customer')
    table = cursor.fetchall()
    print("--- customer BEGIN ---")
    for row in table:
        print(row)
    print("--- customer END ---")

    cursor.execute('SELECT * FROM normal_account')
    table = cursor.fetchall()
    print("--- normal_account BEGIN ---")
    for row in table:
        print(row)
    print("--- normal_account END ---")

    cursor.execute('SELECT * FROM premium_account')
    table = cursor.fetchall()
    print("--- premium_account BEGIN ---")
    for row in table:
        print(row)
    print("--- premium_account END ---")

    cursor.execute('SELECT * FROM merchant')
    table = cursor.fetchall()
    print("--- merchant BEGIN ---")
    for row in table:
        print(row)
    print("--- merchant END ---")

    cursor.execute('SELECT * FROM item')
    table = cursor.fetchall()
    print("--- item  BEGIN ---")
    for row in table:
        print(row)
    print("--- item END ---")

    cursor.execute('SELECT * FROM orders')
    table = cursor.fetchall()
    print("--- order BEGIN ---")
    for row in table:
        print(row)
    print("--- order END ---")

    cursor.execute('SELECT * FROM review')
    table = cursor.fetchall()
    print("--- review BEGIN ---")
    for row in table:
        print(row)
    print("--- review END ---")

    cursor.execute('SELECT * FROM comment')
    table = cursor.fetchall()
    print("--- comment BEGIN ---")
    for row in table:
        print(row)
    print("--- comment END ---")

    cursor.execute('SELECT * FROM orderItem')
    table = cursor.fetchall()
    print("--- make BEGIN ---")
    for row in table:
        print(row)
    print("--- make END ---")

    cursor.close()


def migrate_data(db):
    global is_migrated
    is_migrated = 1
    drop_mongodb_columns()

    # create collections
    user_coll = mongo_db["user"]
    customer_coll = mongo_db["customer"]
    normal_account_coll = mongo_db["normal_account"]
    premium_account_coll = mongo_db["premium_account"]
    merchant_coll = mongo_db["merchant"]
    item_coll = mongo_db["item"]
    order_coll = mongo_db["orders"]
    review_coll = mongo_db["review"]
    comment_coll = mongo_db["comment"]
    orderItem_coll = mongo_db["orderItem"]
    cursor = db.cursor()

    # create index category 
    item_coll.create_index([("category:", -1)])

    # create index bonusPoints
    customer_coll.create_index([("bonus_points:", -1)])

    #create index username
    user_coll.create_index([("username:", -1)])


    # migrate user
    cursor.execute("SELECT * FROM user")
    for row in cursor:
        i = {"user_id": row[0],
             "username": row[1],
             "email": row[2],
             "password": row[3]
             }
        user_coll.insert_one(i)

    # migrate customer
    cursor.execute("SELECT * FROM customer")
    for row in cursor:
        i = {"phone_number": row[0],
             "delivery_address": row[1],
             "bonus_points": row[2],
             "user_id": row[3],

             }
        customer_coll.insert_one(i)

    # migrate normal_account
    cursor.execute("SELECT * FROM normal_account")
    for row in cursor:
        i = {"delivery_fee": float(row[0]),
             "point_limit": row[1],
             "user_id": row[2],
             }
        normal_account_coll.insert_one(i)

    # migrate premium_account
    cursor.execute("SELECT * FROM premium_account")
    for row in cursor:
        i = {"invitation": row[0],
             "discount": float(row[1]),
             "user_id": row[2],
             }
        premium_account_coll.insert_one(i)

    # migrate merchant
    cursor.execute("SELECT * FROM merchant")
    for row in cursor:
        i = {"merchant_name": row[0],
             "website": row[1],
             "user_id": row[2],
             }
        merchant_coll.insert_one(i)

    # migrate item
    cursor.execute("SELECT * FROM item")
    for row in cursor:
        # convert date
        i = {"item_id": row[0],
             "description": row[1],
             "price": float(row[2]),
             "category": row[3],

             }

        item_coll.insert_one(i)

    # migrate order
    cursor.execute(
        "SELECT orders.*, item.* FROM orders JOIN orderItem ON orderItem.order_id = orders.order_id JOIN item ON item.item_id= orderItem.item_id")

    orderItem_list = cursor.fetchall()

    for row in orderItem_list:
        i = {"order_id": row[0],
             "quantity": row[1],
             "total_price": float(row[2]),
             "delivery_date": datetime.combine(row[3], datetime.min.time()),
             "user_id": row[4],
             "item": [{"item_id": row[5], "description": row[6], "price": float(row[7]), "category": row[8]}]
             }
        order_coll.insert_one(i)

    # migrate review
    cursor.execute("SELECT * FROM review")
    for row in cursor:
        i = {"review_id": row[0],
             "publish_timestamp": row[1],
             "title": row[2],
             "description": row[3],
             "rating": row[4],
             "user_id": row[5],
             "item_id": row[6]}
        review_coll.insert_one(i)

    # migrate comment
    cursor.execute("SELECT * FROM comment")
    for row in cursor:
        i = {"comment_id": row[0],
             "publish_timestamp": row[1],
             "content": row[2],
             "review_id": row[3],
             "user_id": row[4]}

        comment_coll.insert_one(i)

    # migrate orderItem
    cursor.execute("SELECT * FROM orderItem")
    for row in cursor:
        i = {"user_id": row[0],
             "item_id": row[1],
             }

        orderItem_coll.coll.insert_one(i)

    cursor.close()
    return


def drop_mongodb_columns():
    for collection_name in mongo_db.list_collection_names():
        mongo_db[collection_name].drop()


@app.route('/')
def index():
    return render_template('login.html')


@app.route('/login', methods=['POST'])
def login():
    global username
    username = request.form['username']
    password = request.form['password']
    if is_migrated == 0:
        cur = db.cursor()
        cur.execute(
            "SELECT * FROM user WHERE username = %s AND password = %s", (username, password))
        user = cur.fetchone()
        print(user)
        cur.close()
        if user:
            return redirect(url_for('customer_page'))

        else:
            return render_template('success.html', message="Failed to login")
    else:
        collection = mongo_db.user
        result = collection.find_one(
            {'password': password, 'username': username})
        print(result)
        if result:
            return redirect(url_for('customer_page'))

        else:
            return render_template('success.html', message="Failed to login")


# sign up
@app.route('/use_case1', methods=['POST'])
def use_case1():

    username_signup = request.form['username']
    password_signup = request.form['password']
    if len(password_signup) < 6:
        return "weak password"
    email = request.form['email']
    user_type = request.form['user_type']

    if is_migrated == 0:
        cur = db.cursor()
        cur.execute('SELECT * FROM user where username=%s', (username_signup,))
        result = cur.fetchall()
        print(len(result))
        if len(result) > 0:
            return 'duplicate user name '
        else:
            cur.execute('SELECT * FROM user where email=%s', (email,))
            result_email = cur.fetchall()
            print(len(result_email))
            if len(result_email) != 0:
                return 'duplicate mail'
            else:
                cur.execute("INSERT INTO user (username,email, password) VALUES (%s, %s, %s)",
                            (username_signup, email, password_signup))

                if user_type == 'merchant':
                    website = request.form['website']
                    cur.execute(
                        "INSERT INTO merchant (merchant_name,website) VALUES (%s, %s)", (username_signup, website))

                elif user_type == 'customer':
                    phone_number = request.form['phone_number']
                    delivery_address = request.form['delivery_address']
                    bonus = 0
                    cur.execute("INSERT INTO customer (phone_number,delivery_address,bonus_points) VALUES (%s, %s,%s)", (
                        phone_number, delivery_address, bonus))
                cur.close()
    else:
        collection = mongo_db.user
        last_document = collection.find().sort('user_id', -1).limit(1)
        last_user_id = last_document[0]['user_id']
        result_1 = collection.find_one({'username': username_signup})
        if result_1:
            return 'duplicate user name'
        else:
            result_2 = collection.find_one({'email': email})
            if result_2:
                return 'duplicate email'
            else:
                new_user = {
                    'username': username_signup,
                    'email': email,
                    'password': password_signup,
                    'user_id': last_user_id+1
                }
                result = collection.insert_one(new_user)

                if user_type == 'merchant':
                    website = request.form['website']
                    new_merchant = {
                        'merchant_name': username_signup, 'website': website}
                    collection = mongo_db.merchant
                    result = collection.insert_one(new_merchant)

                elif user_type == 'customer':
                    phone_number = request.form['phone_number']
                    delivery_address = request.form['delivery_address']
                    bonus = 0
                    new_customer = {
                        'phone_number': phone_number,
                        'delivery_address': delivery_address,
                        'bonus_points': bonus}
                    collection = mongo_db.customer
                    result = collection.insert_one(new_customer)

    return render_template('success.html', message="Signed up successfully")


@app.route('/logout', methods=['GET'])
def logout():
    global username
    username = None
    return render_template('login.html', message="Logged out successfully")


@app.route('/initiate', methods=['POST'])
def initiate():
    testing_insert_data()
    return render_template('success.html', message="Initiation successful")


@app.route('/migrate', methods=['POST'])
def migrate():
    testing_insert_data()
    migrate_data(db)
    return render_template('success.html', message="Migration successful")


@app.route('/customer_page')
def customer_page():
    return render_template('customer_page.html', user_type='customer')


# add item to order
@app.route('/use_case2', methods=['POST'])
def use_case2():
    global is_migrated
    id = request.form['id']
    quantity = request.form['anzahl']
    current_date = date.today()
    formatted_date = current_date.strftime("%Y-%m-%d")
    if is_migrated == 0:
        cur = db.cursor(buffered=True)
        cur.execute('SELECT user_id FROM user where username=%s', (username,))
        user_id = cur.fetchone()[0]
        cur.execute("SELECT price FROM item where item_id=%s", (id,))
        price = cur.fetchall()[0][0]
        price = float(price)
        print(type(quantity), type(price))

        cur.execute("INSERT INTO orders (quantity,total_price,delivery_date,user_id) VALUES (%s, %s, %s,%s)",
                    (quantity, int(quantity)*price, formatted_date, user_id))
        cur.execute("select max(order_id) from orders")
        order_id_needed = cur.fetchall()
        order_id_needed = str(order_id_needed[0][0])
        cur.execute(
            "INSERT INTO orderItem (order_id,item_id) VALUES (%s, %s)", (order_id_needed, id))
        db.commit()
        totalprices = int(quantity)*price
    else:
        collection_user = mongo_db.user
        user = collection_user.find_one({'username': username}, {'user_id': 1})
        if user:
            user_id = user['user_id']
        collection_item = mongo_db.item
        item_col = collection_item.find_one(
            {'item_id': int(id)}, {'description': 1, 'price': 1})
        if item_col:
            item_desc = item_col['description']
            item_price = item_col['price']
            print(item_desc, item_price, quantity)
            totalprices = item_price*int(quantity)
        collection_order = mongo_db.orders
        last_document = collection_order.find().sort('order_id', -1).limit(1)
        last_order_id = last_document[0]['order_id']
        new_order = {
            'order_id': last_order_id+1,
            'item_id': id,
            'description': item_desc,
            'quantity': quantity,
            'user_id': int(user_id),
            "delivery_date": formatted_date,
            "total_price": float(totalprices)
        }
        result = collection_order.insert_one(new_order)
    return "delivery is on your way and the total price is " + str(totalprices)


@app.route('/products')
def products():
    global is_migrated
    if is_migrated == 0:
        cursor = db.cursor()
        cursor.execute('SELECT * FROM item')
        produkt = cursor.fetchall()
        return render_template('products.html', produkt=produkt)
    else:
        collections = mongo_db.item
        items = collections.find()
        produkt = [[item['item_id'], item['description'],
                    item['price'], item['category']] for item in items]
        return render_template('products.html', produkt=produkt)


@app.route('/orders')
def orders():
    global is_migrated
    if is_migrated == 0:
        cursor = db.cursor()
        cursor.execute(
            'SELECT user_id FROM user where username=%s', (username,))
        user_id = cursor.fetchone()[0]
        cursor.execute("""
            SELECT orders.order_id,orders.quantity,
            orders.delivery_date,
            orders.total_price, orderItem.item_id, item.description,orders.user_id
            FROM orders 
            JOIN orderItem  ON orderItem.order_id = orders.order_id
            JOIN item  ON orderItem.item_id = item.item_id
            where orders.user_id=%s;""", (user_id,))
        order = cursor.fetchall()
    else:
        collection = mongo_db.user
        user = collection.find_one({'username': username}, {'user_id': 1})
        if user:
            user_id = user['user_id']
            collection = mongo_db.orders
            order_table = collection.find({'user_id': user_id})
            order = [[item['order_id'], item['quantity'], item['delivery_date'], item['total_price'],
                      item['item_id'], item['description'], item['user_id']] for item in order_table]
    return render_template('orders.html', order=order)


# report1 (top item)
@app.route('/report1', methods=['GET', 'POST'])
def report1():
    if request.method == 'POST':
        selected_category = request.form.get('category')
        if selected_category == 'All':
            if is_migrated == 0:
                cursor = db.cursor()
                cursor.execute("""SELECT top_items.item_id, item.description, item.category, top_items.count AS item_count
                            FROM (
                                SELECT item_id, COUNT(item_id) AS count
                                FROM review
                                GROUP BY item_id
                                ORDER BY count DESC
                                LIMIT 10
                            ) AS top_items
                            JOIN item ON top_items.item_id = item.item_id
                            LIMIT 10;""")
                order = cursor.fetchall()
            else:
                review_collection = mongo_db.review
                pipeline = [
                    {'$group': {
                        '_id': '$item_id',
                        'count': {'$sum': 1}
                    }},
                    {'$lookup': {
                        'from': 'item',
                        'localField': '_id',
                        'foreignField': 'item_id',
                        'as': 'item'
                    }},
                    {'$unwind': '$item'},
                    {'$project': {
                        '_id': 0,
                        'item_id': '$_id',
                        'description': '$item.description',
                        'category': '$item.category',
                        'count': 1
                    }},
                    {'$sort': {'count': -1}},
                    {'$limit': 10}
                ]
                result = review_collection.aggregate(pipeline)
                order = [[item['item_id'], item['description'],
                          item['category'], item['count']] for item in result]
        else:
            if is_migrated == 0:
                cursor = db.cursor()
                cursor.execute("""SELECT top_items.item_id, item.description, item.category, top_items.count AS item_count
                            FROM (
                                SELECT item_id, COUNT(item_id) AS count
                                FROM review
                                GROUP BY item_id
                                ORDER BY count DESC
                            ) AS top_items
                            JOIN item ON top_items.item_id = item.item_id
                            WHERE item.category = %s
                            ORDER BY item_count DESC
                            LIMIT 10;""", (selected_category,))
                order = cursor.fetchall()
            else:
                review_collection = mongo_db.review
                pipeline = [
                    {'$group': {
                        '_id': '$item_id',
                        'count': {'$sum': 1}
                    }},
                    {'$lookup': {
                        'from': 'item',
                        'localField': '_id',
                        'foreignField': 'item_id',
                        'as': 'item'
                    }},
                    {'$unwind': '$item'},
                    {'$project': {
                        '_id': 0,
                        'item_id': '$_id',
                        'description': '$item.description',
                        'category': '$item.category',
                        'count': 1
                    }},
                    {'$match': {'category': selected_category}},
                    {'$sort': {'count': -1}},
                    {'$limit': 10}
                ]
                result = review_collection.aggregate(pipeline)
                order = [[item['item_id'], item['description'],
                          item['category'], item['count']] for item in result]
        return render_template('report1.html', employees=order)
    else:
        return render_template('report1.html', employees=[])


# report2 (top user)
@app.route('/report2', methods=['GET', 'POST'])
def report2():
    global user_id, username
    if is_migrated == 0:
        cursor = db.cursor()
        cursor.execute("""
            SELECT u.user_id, u.username, MAX(c.bonus_points), COUNT(DISTINCT r.review_id)
            FROM user u
            JOIN customer c ON c.user_id = u.user_id
            JOIN review r ON c.user_id = r.user_id
            WHERE c.bonus_points > 100
            GROUP BY u.user_id, u.username
            ORDER BY MAX(c.bonus_points) DESC
        """)
        order = cursor.fetchall()
    else:
        customer_collection = mongo_db.user
        pipeline = [
            {'$lookup': {
                'from': 'user',
                'localField': 'user_id',
                'foreignField': 'user_id',
                'as': 'user_info'
            }},
            {'$lookup': {
                'from': 'customer',
                'localField': 'user_id',
                'foreignField': 'user_id',
                'as': 'customer_info'
            }},
            {'$lookup': {
                'from': 'review',
                'localField': 'user_id',
                'foreignField': 'user_id',
                'as': 'reviews'
            }},
            {'$group': {
                '_id': '$user_id',
                'review_count': {'$sum': {'$size': '$reviews'}},
                'username': {'$first': {'$arrayElemAt': ['$user_info.username', 0]}},
                'bonus_points': {'$first': {'$arrayElemAt': ['$customer_info.bonus_points', 0]}}
            }},
            {'$match': {'bonus_points': {'$gt': 100}}},
            {'$sort': {'bonus_points': -1
                       }},
            {'$project': {
                'user_id': '$_id',
                'username': 1,
                'review_count': 1,
                'bonus_points': 1,
                '_id': 0
            }}
        ]
        result = customer_collection.aggregate(pipeline)
        order = [[item['user_id'], item['username'],
                  item['bonus_points'], item['review_count']] for item in result]
    return render_template('report2.html', users=order)


def get_next_auto_increment_value_review(collection_name):
    collection = mongo_db.orders
    # Replace 'counters' with the name of your sequence collection
    counters_collection = collection['order_od']
    counter = counters_collection.find_one_and_update(
        {'_id': collection_name},
        {'$inc': {'seq': 1}},
        return_document=pymongo.ReturnDocument.AFTER
    )

# add review


@app.route('/add_review', methods=['POST'])
def add_review():
    global user_id
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cur = db.cursor()
    id = request.form['id']  # Get the ID from the AJAX request
    review = request.form['review']
    if is_migrated == 0:
        cur.execute('SELECT user_id FROM user where username=%s', (username,))
        user_id = cur.fetchone()[0]
        cur.execute("""    
        SELECT orders.order_id,orders.quantity,orders.user_id,
        orders.delivery_date,
        orders.total_price, orderItem.item_id, item.description
        FROM orders 
        JOIN orderItem  ON orderItem.order_id = orders.order_id
        JOIN item  ON orderItem.item_id = item.item_id
        where item.item_id=%s and orders.user_id=%s;
        """, (id, user_id))
        result_review = cur.fetchone()
        if result_review:
            sql = "INSERT INTO review (publish_timestamp ,title,description,rating,item_id,user_id) VALUES (%s,%s, %s, %s,%s,%s)"
            val = (timestamp, review, review, 5, id, user_id)
            cur.execute(sql, val)
            cur.execute('SELECT * FROM review;')
            last_id = cur.fetchall()
            db.commit()
            return 'added successfully'
        else:
            return 'You did not buy this product yet'
    else:
        collection_user = mongo_db.user
        user = collection_user.find_one({'username': username}, {'user_id': 1})
        if user:
            print('yes user')
            user_id = user['user_id']
            collection = mongo_db.orders
            print(id, user_id)
            document = collection.find_one(
                {'item_id': id, 'user_id': int(user_id)})
            if document:
                print('yes item ordered')
                next_id = get_next_auto_increment_value_review('review')
                collection_review = mongo_db.review
                new_review = {
                    'review_id': next_id,
                    'publish_timestamp': timestamp,
                    'title': review,
                    'description': review,
                    'rating': 5,
                    'user_id': int(user_id),
                    "item_id": int(id)
                }
                result = collection_review.insert_one(new_review)
                print('okk')
                return 'added successfully'
            else:
                print('not ok')
                return 'You did not buy this product yet'


# view review

@app.route('/view_reviews', methods=['POST'])
def view_reviews():
    id = request.form['id']
    if is_migrated == 0:
        cur = db.cursor()
        cur.execute("""SELECT user.username, review.description
                   FROM user
                   JOIN review ON user.user_id = review.user_id
                   where review.item_id=%s""", (id,))
        result = cur.fetchall()
        result_str = ""
        for item in result:
            result_str += str(item) + "\n"
    else:
        review_collection = mongo_db.review
        pipeline = [{'$match': {'item_id': int(id)}},
                    {
            '$lookup': {
                    'from': 'user',
                    'localField': 'user_id',
                    'foreignField': 'user_id',
                    'as': 'user'
                    }
        },
            {
            '$unwind': '$user'
        },
            {
            '$project': {
                'username': '$user.username',
                'description': 1
            }
        }
        ]
        result = review_collection.aggregate(pipeline)
        result_str = ""
        for doc in result:
            result_str += "(" + str(doc['username']) + \
                ",    " + str(doc['description']) + ")\n"
    return result_str


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True, ssl_context='adhoc')
