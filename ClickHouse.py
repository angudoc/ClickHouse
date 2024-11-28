from clickhouse_driver import Client
from clickhouse_driver.errors import Error

# Подключение к ClickHouse
try:
    client = Client(host='localhost')
    print("Успешное подключение к ClickHouse")
except Error as e:
    print(f"Ошибка подключения к ClickHouse: {e}")
    exit()

# Создание таблицы
try:
    client.execute('''
        CREATE TABLE IF NOT EXISTS products (
            name String,
            price Float64
        ) ENGINE = MergeTree() 
        ORDER BY name
    ''')
    print("Таблица products создана или уже существует")
except Error as e:
    print(f"Ошибка при создании таблицы: {e}")
    exit()

# Пример данных
items = [
    {"name": "Product 1", "price": 10.0},
    {"name": "Product 2", "price": 20.0},
]

# Загрузка данных
try:
    data = [(item['name'], item['price']) for item in items]
    client.execute('INSERT INTO products (name, price) VALUES', data)
    print("Данные успешно загружены в таблицу products")
except Error as e:
    print(f"Ошибка при вставке данных: {e}")

# Пример запросов
try:
    result = client.execute('SELECT * FROM products WHERE price > 10')
    print("Данные с ценой больше 10:", result)
except Error as e:
    print(f"Ошибка при выполнении запроса: {e}")
