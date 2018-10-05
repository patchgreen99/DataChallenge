import sqlite3
import pandas as pd
import os

TABLES = \
[
('Customer Data', 'customers', ['customer_number', 'customer_name', 'country', 'region'],
 '''
CREATE TABLE IF NOT EXISTS customers (
 customer_number integer,
 customer_name VARCHAR,
 country VARCHAR,
 region VARCHAR,
 PRIMARY KEY (customer_number)
)
'''),
('Product data', 'product_prices', ['product_code', 'product', 'product_category_code', 'standard_price', 'variable_cost', 'gross_margin', 'year'],
 '''
CREATE TABLE IF NOT EXISTS product_prices (
 product_code integer,
 product VARCHAR,
 product_category_code integer,
 standard_price float,
 variable_cost float,
 gross_margin float,
 year year,
 PRIMARY KEY (year, product_code),
 FOREIGN KEY (product_category_code) REFERENCES product_categories (product_category_code) 
 ON DELETE CASCADE ON UPDATE NO ACTION
)
'''),
('Product data', 'product_categories', ['product_category', 'product_category_code'],
 '''
CREATE TABLE IF NOT EXISTS product_categories (
 product_category VARCHAR,
 product_category_code integer,
 PRIMARY KEY (product_category_code)
)
'''),
('Sales Data', 'sales', ['product', 'account', 'invoice_number', 'invoice_date', 'year', 'month', 'quantity', 'invoice_amount'],
'''
CREATE TABLE IF NOT EXISTS sales (
 product integer,
 account integer,
 invoice_number integer,
 invoice_date timestamp,
 year year,
 month month,
 quantity integer,
 invoice_amount float,
 PRIMARY KEY (year, month, invoice_date, invoice_number),
 FOREIGN KEY (year, product) REFERENCES product_prices (year, product_code) 
 ON DELETE CASCADE ON UPDATE NO ACTION,
 FOREIGN KEY (account) REFERENCES customers (customer_number) 
 ON DELETE CASCADE ON UPDATE NO ACTION
)
'''),
('Sales budgets', 'budgets', ['product_code', 'account', 'year', 'month', 'budget'],
 '''
CREATE TABLE IF NOT EXISTS budgets (
 product_code integer,
 account integer,
 year year,
 month month,
 budget float,
 PRIMARY KEY (year, product_code, account, month),
 FOREIGN KEY (year, product_code) REFERENCES product_prices (year, product_code) 
 ON DELETE CASCADE ON UPDATE NO ACTION,
 FOREIGN KEY (account) REFERENCES customers (customer_number) 
 ON DELETE CASCADE ON UPDATE NO ACTION
)
''')
]


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        conn.enable_load_extension(True)
        conn.load_extension('mod_spatialite.dylib')
        return conn
    except Error as e:
        print(e)

    return None


def create_table(c, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def write_table(conn, filename, tablename, includecolumns):
    """ write xlsx workbook into the table
    :param conn: database connection
    :param filename: xlsx filename in local directory
    :param tablename: table to insert the data into
    :param tablename: column names of that table
    :return:
    """

    # read workbook
    wb = pd.read_excel(filename.replace(' ', '_') + '.xlsx', sheetname=None)
    for sheet in wb:
        # rename columns and infer types
        renames = {c: c.replace(' ', '_').lower() for c in wb[sheet].columns}
        wb[sheet] = wb[sheet].rename(index=str, columns=renames).infer_objects()

        # dedupe on columns to include
        wb[sheet] = wb[sheet].filter(items=includecolumns).drop_duplicates()

        # insert to table with first sheet only
        wb[sheet].to_sql(tablename, conn, index=False, if_exists='append')
        break


def main():
    # delete previous database
    database = "Database.sql"
    os.remove(database)

    # create a database connection
    conn = create_connection(database)
    if conn is not None:
        c = conn.cursor()
        for (filename, tablename, includecolumns, sql) in TABLES:

            # create table
            create_table(c, sql)
            conn.commit()

            # write to table
            write_table(conn, filename, tablename, includecolumns)

        # Create a view for all the data
        c.execute("""
        CREATE VIEW V_Customer_Sales AS 
        SELECT * 
        FROM product_prices 
        JOIN product_categories USING (product_category_code) 
        JOIN sales ON sales.product = product_prices.product_code AND sales.year = product_prices.year 
        JOIN customers ON customers.customer_number = sales.account;
        """)

    else:
        print("Error! cannot create the database connection.")

    conn.close()


if __name__ == '__main__':
    main()