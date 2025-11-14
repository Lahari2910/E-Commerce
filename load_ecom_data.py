from pathlib import Path
import sqlite3

import pandas as pd


DATA_DIR = Path("data")
DB_PATH = Path("ecom.db")

TABLE_SCHEMAS = {
    "customers": """
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            phone TEXT NOT NULL,
            created_at TEXT NOT NULL,
            city TEXT NOT NULL,
            state TEXT NOT NULL
        )
    """,
    "products": """
        CREATE TABLE products (
            product_id TEXT PRIMARY KEY,
            product_name TEXT NOT NULL,
            category TEXT NOT NULL,
            price REAL NOT NULL,
            in_stock INTEGER NOT NULL CHECK (in_stock IN (0, 1)),
            added_at TEXT NOT NULL
        )
    """,
    "orders": """
        CREATE TABLE orders (
            order_id TEXT PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            order_date TEXT NOT NULL,
            total_amount REAL NOT NULL,
            payment_method TEXT NOT NULL,
            order_status TEXT NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
    """,
    "order_items": """
        CREATE TABLE order_items (
            item_id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            item_price REAL NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        )
    """,
    "reviews": """
        CREATE TABLE reviews (
            review_id TEXT PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            product_id TEXT NOT NULL,
            rating INTEGER NOT NULL,
            comment TEXT NOT NULL,
            review_date TEXT NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        )
    """,
}


def normalize_booleans(df: pd.DataFrame, column: str) -> pd.DataFrame:
    df = df.copy()
    df[column] = (
        df[column]
        .astype(str)
        .str.strip()
        .str.upper()
        .map({"TRUE": 1, "FALSE": 0})
        .fillna(0)
        .astype(int)
    )
    return df


def load_csv(name: str) -> pd.DataFrame:
    path = DATA_DIR / f"{name}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing CSV: {path}")
    df = pd.read_csv(path)
    if name == "products":
        df = normalize_booleans(df, "in_stock")
    return df


def main() -> None:
    DB_PATH.unlink(missing_ok=True)

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")

        for table, schema in TABLE_SCHEMAS.items():
            conn.execute(f"DROP TABLE IF EXISTS {table};")
            conn.execute(schema)

        # Load data with referential integrity order
        for table in ["customers", "products", "orders", "order_items", "reviews"]:
            df = load_csv(table)
            df.to_sql(table, conn, if_exists="append", index=False)

        cursor = conn.cursor()
        print("Row counts:")
        for table in TABLE_SCHEMAS:
            cursor.execute(f"SELECT COUNT(*) FROM {table};")
            count = cursor.fetchone()[0]
            print(f"- {table}: {count}")


if __name__ == "__main__":
    main()

