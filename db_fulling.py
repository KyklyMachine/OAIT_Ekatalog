import random

import pandas as pd
import psycopg2
from psycopg2 import Error
import constants


class Database:
    connection = None
    cursor = None
    db_name = ""
    db_username = ""
    db_password = ""
    db_host = ""
    db_port = ""

    def __init__(self, db_name, db_username, db_password, db_host, db_port):
        self.db_name = db_name
        self.db_username = db_username
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port

    def open_connection(self):
        try:
            self.connection = psycopg2.connect(dbname=self.db_name,
                                               user=self.db_username,
                                               password=self.db_password,
                                               host=self.db_host,
                                               port=self.db_port)
            self.cursor = self.connection.cursor()
            print("Открыто соединение с PostgreSQL")

        except(Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)

    def close_connection(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            print("Соединение с PostgreSQL закрыто")

    def write_users(self):
        try:
            users = pd.read_csv("user.csv")


            for i in range(users.shape[0]):
                user = users.iloc[i]
                sql_insert = "INSERT INTO \"user\" (" \
                             "mail," \
                             "username," \
                             "password" \
                             ") VALUES (%s,%s,%s);"

                self.cursor.execute(sql_insert, (user[0], user[1], user[2]))
                self.connection.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
            self.connection.rollback()

    def write_category(self):
        try:
            users = pd.read_csv("categories.csv")
            for i in range(users.shape[0]):
                user = users.iloc[i]
                sql_insert = "INSERT INTO \"category\" (" \
                             "category_id," \
                             "name," \
                             "description" \
                             ") VALUES (%s,%s,%s);"

                self.cursor.execute(sql_insert, (str(i), user[0], user[1]))
                self.connection.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
            self.connection.rollback()

    def write_brands(self):
        try:
            users = pd.read_csv("brand.csv")
            for i in range(users.shape[0]):
                user = users.iloc[i]
                sql_insert = "INSERT INTO \"brand\" (" \
                             "brand_id," \
                             "name," \
                             "information" \
                             ") VALUES (%s,%s,%s);"

                self.cursor.execute(sql_insert, (str(i), user[0], user[1]))
                self.connection.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
            self.connection.rollback()

    def write_characteristics(self):
        try:
            users = pd.read_csv("characteristic.csv")
            for i in range(users.shape[0]):
                user = users.iloc[i]
                sql_insert = "INSERT INTO \"characteristic\" (" \
                             "characteristic_id," \
                             "name," \
                             "description" \
                             ") VALUES (%s,%s,%s);"

                self.cursor.execute(sql_insert, (str(i), user[0], ""))
                self.connection.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
            self.connection.rollback()

    def write_product(self):
        try:
            users = pd.read_csv("product.csv")
            for i in range(users.shape[0]):
                user = users.iloc[i]
                sql_insert = "INSERT INTO \"product\" (" \
                             "product_id," \
                             "brand_id," \
                             "name," \
                             "date_added," \
                             "category_id" \
                             ") VALUES (%s,%s,%s,%s,%s);"

                self.cursor.execute(sql_insert, (str(i), str(user[1]), str(user[2]), str(user[3]), str(user[0])))
                self.connection.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
            self.connection.rollback()

    def write_product_characteristics(self):
        try:
            users = pd.read_csv("product_characteristic.csv")
            for i in range(users.shape[0]):
                user = users.iloc[i]
                sql_insert = "INSERT INTO \"product_characteristics\" (" \
                             "product_characteristics_id," \
                             "product_id," \
                             "value," \
                             "characteristic_id" \
                             ") VALUES (%s,%s,%s,%s);"

                self.cursor.execute(sql_insert, (str(i), str(user[0]), str(user[2]), str(user[1])))
                self.connection.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
            self.connection.rollback()

    def write_modification(self):
        try:
            users = pd.read_csv("modification.csv")
            for i in range(users.shape[0]):
                user = users.iloc[i]
                sql_insert = "INSERT INTO \"modification\" (" \
                             "modification_id," \
                             "product_id," \
                             "name" \
                             ") VALUES (%s,%s,%s);"

                self.cursor.execute(sql_insert, (str(i), str(user[0]), str(user[1])))
                self.connection.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
            self.connection.rollback()

    def write_mod_characteristic(self):
        try:
            users = pd.read_csv("mod_characteristic.csv")
            for i in range(users.shape[0]):
                user = users.iloc[i]
                sql_insert = "INSERT INTO \"mod_characteristic\" (" \
                             "mod_characteristic_id," \
                             "name," \
                             "description" \
                             ") VALUES (%s,%s,%s);"

                self.cursor.execute(sql_insert, (str(i), str(user[0]), str(user[1])))
                self.connection.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
            self.connection.rollback()

    def write_modification_mod_characteristic(self):
        try:
            users = pd.read_csv("modification_mod_characteristic.csv")
            for i in range(users.shape[0]):
                user = users.iloc[i]
                sql_insert = "INSERT INTO \"modification_mod_characteristic\" (" \
                             "modification_mod_characteristic_id," \
                             "mod_characteristic_id," \
                             "modification_id," \
                             "value" \
                             ") VALUES (%s,%s,%s,%s);"

                self.cursor.execute(sql_insert, (str(i), str(user[0]), str(user[1]), str(user[2])))
                self.connection.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
            self.connection.rollback()

    def write_shop(self):
        try:
            users = pd.read_csv("shop.csv")
            for i in range(users.shape[0]):
                user = users.iloc[i]
                sql_insert = "INSERT INTO \"shop\" (" \
                             "shop_id," \
                             "name," \
                             "link" \
                             ") VALUES (%s,%s,%s);"

                self.cursor.execute(sql_insert, (str(i), str(user[0]), str(user[1])))
                self.connection.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
            self.connection.rollback()

    def write_store_overviews(self):
        try:
            users = pd.read_csv("store_overviews.csv")
            for i in range(users.shape[0]):
                user = users.iloc[i]
                sql_insert = "INSERT INTO \"store_overviews\" (" \
                             "store_overviews_id," \
                             "mail," \
                             "score," \
                             "shop_id," \
                             "review_text," \
                             "date_added" \
                             ") VALUES (%s,%s,%s,%s,%s,%s);"

                if user[1] < 21:
                    self.cursor.execute(sql_insert, (str(i), str(user[0]), str(user[2]), str(user[1]), str(user[3]), str(user[4])))
                    self.connection.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
            self.connection.rollback()

    def write_favorite_products(self):
        try:
            users = pd.read_csv("favorite_prod.csv")
            for i in range(users.shape[0]):
                user = users.iloc[i]
                sql_insert = "INSERT INTO \"favorite_products\" (" \
                             "favorite_products_id," \
                             "product_id," \
                             "date_added," \
                             "mail" \
                             ") VALUES (%s,%s,%s,%s);"
                if user[1] < 3669:
                    self.cursor.execute(sql_insert, (str(i), str(user[1]), str(user[3]), str(user[2])))
                    self.connection.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
            self.connection.rollback()

    def write_browsing_history(self):
        try:
            users = pd.read_csv("browsing_history.csv")
            for i in range(users.shape[0]):
                user = users.iloc[i]
                sql_insert = "INSERT INTO \"browsing_history\" (" \
                             "browsing_history_id," \
                             "product_id," \
                             "date_added," \
                             "mail" \
                             ") VALUES (%s,%s,%s,%s);"
                if user[1] < 3669:
                    self.cursor.execute(sql_insert, (str(i), str(user[1]), str(user[3]), str(user[2])))
                    self.connection.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
            self.connection.rollback()

    def write_product_overviews(self):
        try:
            users = pd.read_csv("product_overviews.csv")
            for i in range(users.shape[0]):
                user = users.iloc[i]
                sql_insert = "INSERT INTO \"product_overviews\" (" \
                             "product_overviews_id," \
                             "product_id," \
                             "score," \
                             "mail," \
                             "review_text," \
                             "date_added" \
                             ") VALUES (%s,%s,%s,%s,%s,%s);"

                if user[1] < 3669:
                    self.cursor.execute(sql_insert, (str(i), str(user[1]), str(user[3]), str(user[2]), str(user[4]), str(user[5])))
                    self.connection.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
            self.connection.rollback()

    def write_offers(self):
        try:
            users = pd.read_csv("offer.csv")
            for i in range(users.shape[0]):
                user = users.iloc[i]
                sql_insert = "INSERT INTO \"offer\" (" \
                             "offer_id," \
                             "modification_id," \
                             "shop_id," \
                             "cost," \
                             "link" \
                             ") VALUES (%s,%s,%s,%s,%s);"

                self.cursor.execute(sql_insert, (str(i), str(user[0]), str(user[1]), str(user[2]), str(user[3])))
                self.connection.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL:", error)
            self.connection.rollback()


if __name__ == "__main__":
    db = Database("oait_ekatalog", "postgres", "postgres", "localhost", "5432")
    db.open_connection()
    

    #db.write_users()
    #db.write_category()
    #db.write_brands()
    #db.write_characteristics()
    #db.write_product()
    #db.write_product_characteristics()
    #db.write_modification()
    #db.write_mod_characteristic()
    #db.write_modification_mod_characteristic()
    #db.write_shop()
    #db.write_store_overviews()
    #db.write_favorite_products()
    #db.write_browsing_history()
    #db.write_product_overviews()
    db.write_offers()

    db.close_connection()