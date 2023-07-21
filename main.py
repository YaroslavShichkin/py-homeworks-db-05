import psycopg2

# Функция, удаляющая БД (таблицы)
def drop_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE phones;
            DROP TABLE customers;
        """)

# Функция, создающая структуру БД (таблицы)
def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS customers(
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(40) NOT NULL,
            last_name VARCHAR(40) NOT NULL,
            email VARCHAR(40) NOT NULL
        );   
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS phones(
            id SERIAL PRIMARY KEY,
            client_id INTEGER NOT NULL REFERENCES customers(id),
            phone VARCHAR(12)
        );
        """)
    conn.commit()

# Функция, позволяющая добавить нового клиента
def add_client(conn, id, first_name, last_name, email, phone=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO customers(id, first_name, last_name, email) VALUES(%s, %s, %s, %s
            );
            """, (id, first_name, last_name, email))
        cur.execute("""
           INSERT INTO phones(client_id, phone) VALUES(%s, %s);
            """, (id, phone))
    conn.commit()
    delete_none_phone(conn)

#Функция, позволяющая добавить телефон для существующего клиента
def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO phones(client_id, phone) VALUES(%s, %s);
            """, (client_id, phone))
    conn.commit()
    delete_none_phone(conn)

#Функция, стирающая пустые номера телефонов
def delete_none_phone(conn):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM phones WHERE phone is %s;
            """, (None,))
    conn.commit()

# Функция, позволяющая изменить данные о клиенте
def change_client(conn, client_id, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cur:
        if first_name is not None:
            cur.execute("""
            UPDATE customers SET first_name=%s WHERE id=%s;
            """, (first_name, client_id))
        if last_name is not None:
            cur.execute("""
            UPDATE customers SET last_name=%s WHERE id=%s;
            """, (last_name, client_id))
        if email is not None:
            cur.execute("""
            UPDATE customers SET email=%s WHERE id=%s;
            """, (email, client_id))
        if phone is not None:
            cur.execute("""
            UPDATE phones SET phone=%s WHERE id=%s;
            """, (phone, client_id))
    conn.commit()

# Функция, позволяющая удалить телефон для существующего клиента
def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM phones WHERE client_id=%s and phone=%s;
            """, (client_id, phone))
    conn.commit()

# Функция, позволяющая удалить существующего клиента.
def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
               DELETE FROM phones WHERE client_id=%s;
               DELETE FROM customers WHERE id=%s;
               """, (client_id, client_id))
    conn.commit()

# Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону.
def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cur:
        cur.execute("""SELECT * FROM customers c JOIN phones p ON c.id = p.client_id 
            WHERE first_name=%s OR 
            last_name=%s OR 
            email=%s OR 
            p.phone=%s;
            """, (first_name, last_name, email, phone))
        print(cur.fetchall())

# Функция, выводящая все данные таблицы
def fetchall_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
           SELECT * FROM customers;
           """)
        print(cur.fetchall())
    with conn.cursor() as cur:
        cur.execute("""
           SELECT * FROM phones;
           """)
        print(cur.fetchall())





with psycopg2.connect(database='customers', user='postgres', password='123456789') as conn:
    drop_db(conn)
    create_db(conn)
    # Добавление 6 клиентов:
    add_client(conn, 1, "Иван", "Иванов", "ivan@mail.ru")
    add_client(conn, 2, "Петр", "Петров", "piter@mail.ru", "89779886655")
    add_client(conn, 3, "Анна", "Василькова", "anvas@mail.ru")
    add_client(conn, 4, "Ирина", "Петрова", "irina@mail.ru")
    add_client(conn, 5, "Кирилл", "Вильи", "vilivili@mail.ru", "89087654321")
    add_client(conn, 6, "Софа", "Морозова", "moroz@mail.ru", "89998887766")
    fetchall_db(conn)

    # Добавление двух номеров 3 клиенту и дного номера 4:
    add_phone(conn, 3, "89123456789")
    add_phone(conn, 3, "84214124124")
    add_phone(conn, 4, "89990002525")
    fetchall_db(conn)

    # Изменение данных клиентов:
    change_client(conn, 1, first_name="Владимир")
    change_client(conn, 2, last_name="Дубров")
    change_client(conn, 3, email="kvakva@yandex.ru")
    change_client(conn, 4, phone="88881112233")
    change_client(conn, 5, "Стас", "Горбунов", "stasss@mail.ru", "80001119988")


    # Удаление конкретного номера телефона у 3 клиента и попытка удалить несуществующий номер:
    delete_phone(conn, 3, "89123456789")
    delete_phone(conn, 4, "9393")
    fetchall_db(conn)

    # Поиск клиентов по имени, фамилии, почте и номеру телефона, поиск несуществующего клиента:
    find_client(conn, first_name="Ирина")
    find_client(conn, last_name="Василькова")
    find_client(conn, email="piter@mail.ru")
    find_client(conn, phone="89998887766")
    find_client(conn, first_name="Миша")

    # Удаление 4 и 6 клиентов:
    delete_client(conn, 4)
    delete_client(conn, 6)
    fetchall_db(conn)

conn.close()