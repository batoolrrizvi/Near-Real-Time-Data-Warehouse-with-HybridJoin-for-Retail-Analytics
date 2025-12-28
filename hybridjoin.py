import threading
import queue
import time
import psycopg2
import psycopg2.extras
import csv
from collections import defaultdict, deque, namedtuple
from datetime import datetime

# params
HASH_SLOTS = 10000           # hS
DISK_PARTITION = 500         # vP
STREAM_BUFFER_SIZE = 5000
BATCH_SIZE = 1000
COMMIT_INTERVAL = 5000

DEFAULT_SUPPLIER_ID = 1
DEFAULT_STORE_ID = 1

#DB CONNECTION
host = input("Enter DB host: ")
dbname = input("Enter database name: ")
user = input("Enter username: ")
password = input("Enter password: ")

conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password)
conn.autocommit = False
cur = conn.cursor()

cur.execute("SET search_path TO walmartDW;")
conn.commit()

# ensure seq exists
cur.execute("CREATE SEQUENCE IF NOT EXISTS walmartDW.sales_id_seq START 1;")
conn.commit()

stream_buffer = queue.Queue(maxsize=STREAM_BUFFER_SIZE)

# StreamNode: key, tuple_data, arrival_id
StreamNode = namedtuple("StreamNode", ["key", "tuple_data", "arrival_id"])

# hash table: slot_id -> list of stream nodes
hash_table = defaultdict(list)

# hold arrival_id in FIFO process oldest tuples first.
arrival_queue = deque()

# arrival_id -> {"slot":slot, "active":True, "key":key}
arrival_meta = {}  

used_slots = 0
free_slots = HASH_SLOTS
lock = threading.Lock()
stream_finished_flag = False

# caches
product_cache = {}
customer_cache_set = set() 
date_cache = {}
pending_dates = {}

# sales id counter
sales_id_counter = 0
sales_id_lock = threading.Lock()
arrival_counter = 0
arrival_counter_lock = threading.Lock()

#UTILITIES
def ensure_default_supplier_and_store():
        cur.execute("SELECT supplier_id FROM Supplier WHERE supplier_id = %s", (DEFAULT_SUPPLIER_ID,))
        if cur.fetchone() is None:
            cur.execute("INSERT INTO Supplier (supplier_id, supplierName) VALUES (%s, %s) ON CONFLICT DO NOTHING", (DEFAULT_SUPPLIER_ID, "DEFAULT_SUPPLIER"))
        cur.execute("SELECT store_id FROM Store WHERE store_id = %s", (DEFAULT_STORE_ID,))
        if cur.fetchone() is None:
            cur.execute("INSERT INTO Store (store_id, storeName) VALUES (%s, %s) ON CONFLICT DO NOTHING", (DEFAULT_STORE_ID, "DEFAULT_STORE"))
        conn.commit()

ensure_default_supplier_and_store()

def initialize_sales_id_counter():
    global sales_id_counter
    cur.execute("SELECT last_value FROM walmartDW.sales_id_seq")
    sales_id_counter = cur.fetchone()[0]

initialize_sales_id_counter()

def load_product_master_to_cache():
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'walmartDW' AND table_name = 'product';
    """)
    cols = {r[0].lower() for r in cur.fetchall()}

    select_cols = ["product_id", "price"]
    if "supplier_id" in cols:
        select_cols.append("supplier_id")
    if "store_id" in cols:
        select_cols.append("store_id")

    query = "SELECT "+", ".join(select_cols)+" FROM walmartDW.Product;"
    cur.execute(query)
    rows = cur.fetchall()
    for r in rows:
            product_id = str(r[0])
            price = float(r[1]) if r[1] is not None else 0.0
            supplier_id = r[2] if ("supplier_id" in select_cols and len(r) > 2) else None
            store_id = r[3] if ("store_id" in select_cols and len(r) > 3) else None
            product_cache[product_id] = {
                "price": price,
                "supplier_id": supplier_id if supplier_id is not None else DEFAULT_SUPPLIER_ID,
                "store_id": store_id if store_id is not None else DEFAULT_STORE_ID
            }

load_product_master_to_cache()

print(f"[Loaded {len(product_cache)} products into cache]")

def load_customer_cache():
    #keep a set of customer ids to know if exist
    cur.execute("SELECT customer_id FROM Customer")
    for row in cur.fetchall():
        customer_cache_set.add(int(row[0]))

load_customer_cache()
print(f"[Loaded {len(customer_cache_set)} customers into cache]")

def load_date_cache():
    cur.execute("SELECT transaction_date, date_id FROM Date")
    for row in cur.fetchall():
        date_str = row[0].strftime("%Y-%m-%d")
        date_cache[date_str] = row[1]

load_date_cache()
print(f"[Loaded {len(date_cache)} dates into cache]")

def parse_date_str(date_str):
    try:
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(date_str, fmt).date()
            except:
                continue
        cur.execute("SELECT %s::date", (date_str,))
        return cur.fetchone()[0]
    except:
        return datetime.today().date()

def get_or_create_date_id(date_str):
    if date_str in date_cache:
        return date_cache[date_str]
    parsed = parse_date_str(date_str)
    date_key = parsed.strftime("%Y-%m-%d")
    if date_key in date_cache:
        return date_cache[date_key]
    if date_key not in pending_dates:
        pending_dates[date_key] = parsed
    if len(pending_dates) >= 100:
        flush_pending_dates()
    if date_key in date_cache:
        return date_cache[date_key]
    return insert_single_date(date_key, parsed)

def flush_pending_dates():
    if not pending_dates:
        return
    cur.execute("SELECT COALESCE(MAX(date_id), 0) FROM Date")
    next_id = cur.fetchone()[0] + 1
    batch_data = []
    for date_str, parsed in pending_dates.items():
        day_num = parsed.day
        month_num = parsed.month
        year = parsed.year
        dayofweek = parsed.strftime("%A")
        quarter = (parsed.month - 1) // 3 + 1
        is_weekend = parsed.weekday() >= 5
        batch_data.append((next_id, parsed, day_num, month_num, year, dayofweek, quarter, is_weekend))
        date_cache[date_str] = next_id
        next_id += 1

    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO Date (date_id, transaction_date, dayNum, monthNum, year, dayofweek, quarter_num, is_weekend)
        VALUES %s ON CONFLICT DO NOTHING
        """,
        batch_data, page_size=100
    )
    pending_dates.clear()

def insert_single_date(date_key, parsed):
    cur.execute("SELECT COALESCE(MAX(date_id), 0) + 1 FROM Date")
    new_id = cur.fetchone()[0]
    day_num = parsed.day
    month_num = parsed.month
    year = parsed.year
    dayofweek = parsed.strftime("%A")
    quarter = (parsed.month - 1) // 3 + 1
    is_weekend = parsed.weekday() >= 5
    cur.execute("""
        INSERT INTO Date (date_id, transaction_date, dayNum, monthNum, year, dayofweek, quarter_num, is_weekend)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
    """, (new_id, parsed, day_num, month_num, year, dayofweek, quarter, is_weekend))
    date_cache[date_key] = new_id
    return new_id

def next_sales_id():
    global sales_id_counter
    with sales_id_lock:
        sales_id_counter += 1
        return sales_id_counter

def next_arrival_id():
    global arrival_counter
    with arrival_counter_lock:
        arrival_counter += 1
        return arrival_counter

#STREAM LOADER
def stream_data_loader(csv_path):
    global stream_finished_flag
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        print("CSV columns:", reader.fieldnames)
        count = 0
        for row in reader:
            norm_row = {k.strip(): v.strip() if isinstance(v, str) else v for k, v in row.items()}
            stream_buffer.put(norm_row)
            count += 1
            if count % 10000 == 0:
                print(f"[ Loaded {count} rows into stream buffer... ]")
        stream_finished_flag = True
    print(f"[ Stream loader finished pushing {count} rows]")

# DISK PARTITION LOADER (R)
def load_disk_partition_near_key(key, partition_size=DISK_PARTITION):
    # select rows >= key ordered by customer_id limit partition_size
    cur.execute("""
        SELECT customer_id, gender, age_group, occupation, city_category, stay_in_current_city_years
        FROM Customer
        WHERE customer_id >= %s
        ORDER BY customer_id
        LIMIT %s
    """, (key, partition_size))

    rows = cur.fetchall()
    # if not enough rows after key, fetch preceding rows to make partition_size
    if len(rows) < partition_size:
        # compute how many preceding
        need = partition_size - len(rows)
        cur.execute("""
            SELECT customer_id, gender, age_group, occupation, city_category, stay_in_current_city_years
            FROM Customer
            WHERE customer_id < %s
            ORDER BY customer_id DESC
            LIMIT %s
        """, (key, need))
        prev_rows = cur.fetchall()
        #reverse to keep ascending order
        prev_rows.reverse()
        rows = prev_rows + rows
    # dict keyed by customer_id for quick probe
    part_map = {int(r[0]): {"gender": r[1], "age_group": r[2], "occupation": r[3], "city_category": r[4], "stay_in_current_city_years": r[5]} for r in rows}
    return part_map

# HYBRID JOIN
def hybrid_join():
    global used_slots, free_slots

    insert_buffer = []
    total_inserted = 0
    last_commit = 0

    while True:
        #take up to w (free_slots) tuples from stream_buffer into hash table
        pulled = 0
        while free_slots > 0 and not stream_buffer.empty():
            tuple_s = stream_buffer.get()
            # normalize key
            try:
                key_raw = tuple_s.get("Customer_ID") or tuple_s.get("customer_id")
                if not key_raw:
                    continue
                key = int(key_raw)
            except:
                continue

            # slot = hash(key) % HASH_SLOTS
            slot = hash(key) % HASH_SLOTS
            arrival_id = next_arrival_id()
            node = StreamNode(key=key, tuple_data=tuple_s, arrival_id=arrival_id)
            with lock:
                hash_table[slot].append(node)
                arrival_queue.append(arrival_id)
                arrival_meta[arrival_id] = {"slot": slot, "active": True, "key": key}
                used_slots += 1
                free_slots -= 1
                pulled += 1

        # exit condition
        if used_slots == 0 and stream_finished_flag:
            # flush any remaining inserts
            if insert_buffer:
                psycopg2.extras.execute_values(
                    cur,
                    """INSERT INTO walmartDW.Sales
                    (sales_id, order_id, customer_id, product_id, date_id, store_id, supplier_id, sales_amount, quantity) VALUES %s""",
                    insert_buffer,
                    page_size=500
                )
                flush_pending_dates()
                conn.commit()
                insert_buffer.clear()
            print(f"[HybridJoin completed! Total inserted: {total_inserted}]")
            break

        # if no tuples to process, wait briefly
        if used_slots == 0:
            time.sleep(0.01)
            continue

        # pick oldest active arrival_id
        oldest_arrival = None
        while arrival_queue:
            candidate = arrival_queue[0]
            meta = arrival_meta.get(candidate)
            if meta is None or not meta.get("active", False):
                # already removed, pop then continue
                arrival_queue.popleft()
                continue
            oldest_arrival = candidate
            break

        if oldest_arrival is None:
            time.sleep(0.005)
            continue

        meta = arrival_meta[oldest_arrival]
        oldest_key = meta["key"]

        #if the customer doesn't exist in R, drop stream tuples
        if oldest_key not in customer_cache_set:
            # Remove all stream nodes in the slot that have this key (no match possible)
            slot = meta["slot"]
            with lock:
                slot_list = hash_table.get(slot, [])
                # remove any nodes with this key
                new_slot_list = [n for n in slot_list if not (n.key == oldest_key)]
                freed = len(slot_list) - len(new_slot_list)
                if freed:
                    hash_table[slot] = new_slot_list
                    used_slots -= freed
                    free_slots += freed
                # mark all arrival_meta entries for that key inactive
                # iterate arrival_meta and mark matches inactive (could be many)
                for aid, am in list(arrival_meta.items()):
                    if am.get("key") == oldest_key and am.get("active"):
                        am["active"] = False
                # discard front queue entries that are inactive
                while arrival_queue and not arrival_meta.get(arrival_queue[0], {}).get("active", False):
                    arrival_queue.popleft()
            continue

        # load disk partition of R near oldest_key
        disk_partition = load_disk_partition_near_key(oldest_key, partition_size=DISK_PARTITION)

        # probe partition rows against the hash_table
        # for each customer_id in partition, check corresponding slot for matching stream tuples
        to_delete_arrival_ids = set()
        # iterate partition rows
        for cust_id, cust_attrs in disk_partition.items():
            slot = hash(cust_id) % HASH_SLOTS
            slot_nodes = list(hash_table.get(slot, []))  # copy to iterate
            if not slot_nodes:
                continue
            for node in slot_nodes:
                if node.key != cust_id:
                    continue
                # matched: enrich and produce an insert tuple
                stream_tuple = node.tuple_data
                # extract fields from stream tuple (robust key names)
                order_id_raw = stream_tuple.get("orderID") or stream_tuple.get("order_id")
                product_id_raw = stream_tuple.get("Product_ID") or stream_tuple.get("product_id")
                qty_raw = stream_tuple.get("quantity") or stream_tuple.get("Quantity")
                date_raw = stream_tuple.get("date") or stream_tuple.get("transaction_date")
                if not all([order_id_raw, product_id_raw, qty_raw, date_raw]):
                    # if missing fields, mark node inactive + slot ko free kardo
                    with lock:
                        try:
                            hash_table[slot].remove(node)
                        except:
                            pass
                        if arrival_meta.get(node.arrival_id, {}).get("active"):
                            arrival_meta[node.arrival_id]["active"] = False
                            used_slots -= 1
                            free_slots += 1
                    continue

                # product info from cache
                product_id = str(product_id_raw).strip()
                prod_info = product_cache.get(product_id)
                if prod_info is None:
                    prod_info = {"price": 0.0, "supplier_id": DEFAULT_SUPPLIER_ID, "store_id": DEFAULT_STORE_ID}
                    product_cache[product_id] = prod_info

                try:
                    qty = int(float(qty_raw))
                except:
                    qty = 0
                price = prod_info["price"]
                supplier_id = prod_info["supplier_id"]
                store_id = prod_info["store_id"]
                sales_amount = round(qty * float(price), 2)

                # date handling
                date_id = get_or_create_date_id(date_raw)

                sid = next_sales_id()

                insert_tuple = (
                    int(sid),
                    int(order_id_raw),
                    int(cust_id),
                    product_id,
                    int(date_id),
                    int(store_id),
                    int(supplier_id),
                    float(sales_amount),
                    int(qty)
                )
                insert_buffer.append(insert_tuple)
                total_inserted += 1
                print("inserted tuple:", insert_tuple)
                # mark node for deletion
                to_delete_arrival_ids.add(node.arrival_id)

                # remove node from slot immediately
                with lock:
                    try:
                        hash_table[slot].remove(node)
                    except:
                        pass
                    if arrival_meta.get(node.arrival_id, {}).get("active"):
                        arrival_meta[node.arrival_id]["active"] = False
                        used_slots -= 1
                        free_slots += 1

                # batch flush
                if len(insert_buffer) >= BATCH_SIZE:
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO walmartDW.Sales (sales_id, order_id, customer_id, product_id, date_id, store_id, supplier_id, sales_amount, quantity)
                        VALUES %s
                        """,
                        insert_buffer,
                        page_size=500
                    )
                    insert_buffer.clear()
                    if total_inserted - last_commit >= COMMIT_INTERVAL:
                        flush_pending_dates()
                        conn.commit()
                        last_commit = total_inserted
                        print(f"[Processed {total_inserted} records...]")

        # cleanup arrival_queue
        while arrival_queue and not arrival_meta.get(arrival_queue[0], {}).get("active", False):
            arrival_queue.popleft()

        # small sleep to avoid tight spinning
        time.sleep(0.0001)

    # Final flush
    flush_pending_dates()
    if insert_buffer:
        psycopg2.extras.execute_values(
            cur,
            """INSERT INTO walmartDW.Sales
            (sales_id, order_id, customer_id, product_id, date_id, store_id, supplier_id, sales_amount, quantity) VALUES %s""",
            insert_buffer,
            page_size=500
        )
    # set sequence to sales_id_counter
    cur.execute("SELECT %s::bigint", (sales_id_counter,))
    cur.execute("SELECT setval('walmartDW.sales_id_seq', %s)", (sales_id_counter,))
    conn.commit()
    print("[HybridJoin finished]")

# ---- [ main function!!] ----
def main():
    csv_path = "transactional_data.csv"
    print("[HybridJoin processing...]")
    start_time = time.time()
    t1 = threading.Thread(target=stream_data_loader, args=(csv_path,), daemon=True)
    t2 = threading.Thread(target=hybrid_join, daemon=True)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    elapsed = time.time() - start_time
    print(f"Execution time: {elapsed:.2f} seconds. Closing DB connection.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
