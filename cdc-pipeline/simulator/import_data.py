#!/usr/bin/env python3
"""
import_data.py
─────────────────────────────────────────────────────────
Nạp dữ liệu từ Instacart CSV files vào MySQL
Chạy 1 lần duy nhất trước khi bắt đầu CDC pipeline

Cách dùng:
    python simulator/import_data.py --data-dir ./data --limit 50000
"""

import argparse
import os
import sys
import time
import pandas as pd
import sqlalchemy
from sqlalchemy import text

# ─────────────────────────────────────────────────
# Cấu hình kết nối
# ─────────────────────────────────────────────────
DB_URL = "mysql+pymysql://root:root123@localhost:3306/instacart"

def get_engine():
    engine = sqlalchemy.create_engine(
        DB_URL,
        pool_pre_ping=True,
        echo=False,
        connect_args={"connect_timeout": 30}
    )
    return engine

def wait_for_mysql(engine, max_retries=30):
    """Chờ MySQL sẵn sàng nhận kết nối."""
    print("Chờ MySQL khởi động...")
    for i in range(max_retries):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("  => MySQL sẵn sàng!")
            return True
        except Exception:
            print(f"  ... thử lại {i+1}/{max_retries}")
            time.sleep(3)
    return False

def import_table(engine, csv_path, table_name, limit=None, dtype=None):
    """Import một file CSV vào MySQL table."""
    if not os.path.exists(csv_path):
        print(f"  [SKIP] Không tìm thấy file: {csv_path}")
        return 0

    print(f"\n  Đọc {csv_path}...")
    df = pd.read_csv(csv_path, nrows=limit, dtype=dtype)
    print(f"  => {len(df):,} dòng, {df.shape[1]} cột")
    print(f"  Columns: {list(df.columns)}")

    # Ghi vào database theo từng chunk
    chunk_size = 5000
    total_written = 0
    with engine.connect() as conn:
        # Xóa dữ liệu cũ
        conn.execute(text(f"DELETE FROM {table_name}"))
        conn.commit()

    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i + chunk_size]
        chunk.to_sql(table_name, engine, if_exists='append', index=False, method='multi')
        total_written += len(chunk)
        progress = (total_written / len(df)) * 100
        print(f"  [{progress:5.1f}%] {total_written:,}/{len(df):,} dòng", end='\r')

    print(f"  [{100:5.1f}%] {total_written:,}/{len(df):,} dòng - XONG")
    return total_written

def main():
    parser = argparse.ArgumentParser(description='Import Instacart data vào MySQL')
    parser.add_argument('--data-dir', default='./data',
                        help='Thư mục chứa CSV files (default: ./data)')
    parser.add_argument('--limit', type=int, default=50000,
                        help='Số dòng tối đa mỗi bảng (default: 50000)')
    args = parser.parse_args()

    print("=" * 55)
    print("  Import Instacart Data vào MySQL")
    print("=" * 55)
    print(f"  Data dir : {args.data_dir}")
    print(f"  Row limit: {args.limit:,} / bảng")
    print()

    engine = get_engine()
    if not wait_for_mysql(engine):
        print("LỖI: Không kết nối được MySQL. Kiểm tra docker-compose up")
        sys.exit(1)

    # Import orders
    import_table(
        engine,
        os.path.join(args.data_dir, 'orders.csv'),
        'orders',
        limit=args.limit,
        dtype={'order_id': int, 'user_id': int, 'order_number': int,
               'order_dow': int, 'order_hour_of_day': int}
    )

    # Import order_products (dùng file prior hoặc train)
    op_file = os.path.join(args.data_dir, 'order_products__prior.csv')
    if not os.path.exists(op_file):
        op_file = os.path.join(args.data_dir, 'order_products__train.csv')

    import_table(
        engine,
        op_file,
        'order_products',
        limit=args.limit * 3,  # Nhiều hơn vì multi-product per order
        dtype={'order_id': int, 'product_id': int,
               'add_to_cart_order': int, 'reordered': int}
    )

    # Import products
    import_table(
        engine,
        os.path.join(args.data_dir, 'products.csv'),
        'products'
    )

    # Kiểm tra kết quả
    print("\n" + "=" * 55)
    print("  Kiểm tra kết quả import")
    print("=" * 55)
    with engine.connect() as conn:
        for tbl in ['orders', 'order_products', 'products']:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {tbl}"))
            count = result.scalar()
            print(f"  {tbl:25s}: {count:>10,} dòng")

    print("\n  HOÀN TẤT! Sẵn sàng chạy CDC pipeline.")

if __name__ == '__main__':
    main()
