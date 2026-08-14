#!/usr/bin/env python3
"""
data_simulator.py
─────────────────────────────────────────────────────────
Giả lập luồng sự kiện INSERT / UPDATE / DELETE liên tục
lên MySQL để Debezium bắt và đẩy vào Kafka.

Cách dùng:
    python simulator/data_simulator.py
    python simulator/data_simulator.py --rate 2 --duration 300
"""

import argparse
import random
import signal
import sys
import time
from datetime import datetime

import mysql.connector
from mysql.connector import Error

# ─────────────────────────────────────────────────
# Cấu hình
# ─────────────────────────────────────────────────
DB_CONFIG = {
    'host':     'localhost',
    'port':     3306,
    'user':     'root',
    'password': 'root123',
    'database': 'instacart',
    'autocommit': False,
}

# Trọng số xác suất cho từng loại sự kiện
EVENT_WEIGHTS = {
    'insert': 50,   # 50%
    'update': 35,   # 35%
    'delete': 15,   # 15%
}

# ID range cho synthetic orders
SYNTHETIC_ID_START = 10_000_000

# ─────────────────────────────────────────────────
# Biến toàn cục
# ─────────────────────────────────────────────────
stats = {'insert': 0, 'update': 0, 'delete': 0, 'error': 0, 'total': 0}
running = True


def signal_handler(sig, frame):
    global running
    print("\n\nNhận tín hiệu dừng. Đang kết thúc...")
    running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def connect_with_retry(max_retries=30):
    """Kết nối MySQL với retry logic."""
    for attempt in range(max_retries):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            print(f"  => Kết nối MySQL thành công!")
            return conn
        except Error as e:
            print(f"  Thử kết nối {attempt+1}/{max_retries}: {e}")
            time.sleep(3)
    raise RuntimeError("Không thể kết nối MySQL sau nhiều lần thử")


def get_random_existing_order_id(cursor):
    """Lấy một order_id ngẫu nhiên đang tồn tại."""
    cursor.execute(
        "SELECT order_id FROM orders ORDER BY RAND() LIMIT 1"
    )
    row = cursor.fetchone()
    return row[0] if row else None


def do_insert(cursor):
    """Thêm một đơn hàng mới (synthetic)."""
    new_id = SYNTHETIC_ID_START + random.randint(0, 8_999_999)
    user_id = random.randint(1, 206_209)
    dow = random.randint(0, 6)
    hour = random.randint(7, 22)
    days_prior = round(random.uniform(1, 30), 1) if random.random() > 0.1 else None
    status = random.choice(['pending', 'processing', 'completed'])

    cursor.execute("""
        INSERT IGNORE INTO orders
            (order_id, user_id, eval_set, order_number, order_dow,
             order_hour_of_day, days_since_prior, status)
        VALUES (%s, %s, 'train', %s, %s, %s, %s, %s)
    """, (new_id, user_id, random.randint(1, 10), dow, hour, days_prior, status))

    if cursor.rowcount > 0:
        # Thêm vài sản phẩm vào đơn
        n_products = random.randint(1, 5)
        product_ids = random.sample(range(1, 10), min(n_products, 9))
        for pos, pid in enumerate(product_ids, 1):
            cursor.execute("""
                INSERT IGNORE INTO order_products
                    (order_id, product_id, add_to_cart_order, reordered)
                VALUES (%s, %s, %s, %s)
            """, (new_id, pid, pos, random.randint(0, 1)))
        return new_id
    return None


def do_update(cursor):
    """Cập nhật trạng thái hoặc giờ đặt hàng của một đơn."""
    order_id = get_random_existing_order_id(cursor)
    if not order_id:
        return None

    # Ngẫu nhiên chọn kiểu update
    update_type = random.choice(['status', 'hour', 'both'])

    if update_type == 'status':
        new_status = random.choice(['pending', 'processing', 'completed', 'cancelled'])
        cursor.execute(
            "UPDATE orders SET status = %s WHERE order_id = %s",
            (new_status, order_id)
        )
    elif update_type == 'hour':
        new_hour = random.randint(0, 23)
        cursor.execute(
            "UPDATE orders SET order_hour_of_day = %s WHERE order_id = %s",
            (new_hour, order_id)
        )
    else:
        new_status = random.choice(['processing', 'completed'])
        new_dow = random.randint(0, 6)
        cursor.execute(
            "UPDATE orders SET status = %s, order_dow = %s WHERE order_id = %s",
            (new_status, new_dow, order_id)
        )
    return order_id


def do_delete(cursor):
    """Xóa một đơn hàng synthetic (chỉ xóa đơn mình đã tạo)."""
    cursor.execute("""
        SELECT order_id FROM orders
        WHERE order_id >= %s
        ORDER BY RAND() LIMIT 1
    """, (SYNTHETIC_ID_START,))
    row = cursor.fetchone()
    if not row:
        return None

    order_id = row[0]
    # Xóa order_products trước (FK constraint)
    cursor.execute("DELETE FROM order_products WHERE order_id = %s", (order_id,))
    cursor.execute("DELETE FROM orders WHERE order_id = %s", (order_id,))
    return order_id


def print_stats_header():
    print(f"\n{'─'*65}")
    print(f"{'Thời gian':<12} {'Sự kiện':<10} {'ID':<12} {'INSERT':>7} {'UPDATE':>7} {'DELETE':>7} {'Tổng':>7}")
    print(f"{'─'*65}")


def print_event(event_type, record_id, symbol):
    colors = {'insert': '\033[92m', 'update': '\033[93m', 'delete': '\033[91m'}
    reset = '\033[0m'
    now = datetime.now().strftime('%H:%M:%S')
    color = colors.get(event_type, '')
    print(f"{now:<12} {color}{symbol} {event_type:<8}{reset} {str(record_id):<12} "
          f"{stats['insert']:>7,} {stats['update']:>7,} {stats['delete']:>7,} "
          f"{stats['total']:>7,}")


def main():
    parser = argparse.ArgumentParser(description='Giả lập CDC events')
    parser.add_argument('--rate', type=float, default=1.5,
                        help='Số sự kiện mỗi giây (default: 1.5)')
    parser.add_argument('--duration', type=int, default=0,
                        help='Thời gian chạy tính bằng giây (0=vô hạn)')
    args = parser.parse_args()

    interval = 1.0 / args.rate
    end_time = time.time() + args.duration if args.duration > 0 else float('inf')

    # Tạo danh sách events theo trọng số
    event_pool = []
    for event, weight in EVENT_WEIGHTS.items():
        event_pool.extend([event] * weight)

    print("=" * 65)
    print("  CDC Data Simulator — Instacart MySQL")
    print("=" * 65)
    print(f"  Tốc độ   : {args.rate} sự kiện/giây")
    print(f"  Thời gian: {'vô hạn' if args.duration == 0 else f'{args.duration}s'}")
    print(f"  Tỷ lệ    : INSERT {EVENT_WEIGHTS['insert']}% | "
          f"UPDATE {EVENT_WEIGHTS['update']}% | DELETE {EVENT_WEIGHTS['delete']}%")
    print()
    print("  Nhấn Ctrl+C để dừng")

    conn = connect_with_retry()
    cursor = conn.cursor()
    print_stats_header()

    try:
        while running and time.time() < end_time:
            loop_start = time.time()

            event_type = random.choice(event_pool)
            record_id = None

            try:
                if event_type == 'insert':
                    record_id = do_insert(cursor)
                    symbol = '+'
                elif event_type == 'update':
                    record_id = do_update(cursor)
                    symbol = '~'
                else:
                    record_id = do_delete(cursor)
                    symbol = '-'

                conn.commit()

                if record_id:
                    stats[event_type] += 1
                    stats['total'] += 1
                    print_event(event_type, record_id, symbol)

            except Error as e:
                conn.rollback()
                stats['error'] += 1
                if stats['error'] <= 5:
                    print(f"  [LỖI] {event_type}: {e}")
                if not conn.is_connected():
                    conn = connect_with_retry()
                    cursor = conn.cursor()

            # Giữ đúng tốc độ
            elapsed = time.time() - loop_start
            sleep_time = max(0, interval - elapsed)
            time.sleep(sleep_time)

    finally:
        cursor.close()
        conn.close()

    print(f"\n{'='*65}")
    print("  Kết quả cuối cùng:")
    print(f"  INSERT : {stats['insert']:,}")
    print(f"  UPDATE : {stats['update']:,}")
    print(f"  DELETE : {stats['delete']:,}")
    print(f"  Tổng   : {stats['total']:,}")
    print(f"  Lỗi    : {stats['error']:,}")
    print(f"{'='*65}")


if __name__ == '__main__':
    main()
