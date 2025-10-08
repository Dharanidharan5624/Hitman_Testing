
# import sys
# import mysql.connector
# from decimal import Decimal
# from collections import defaultdict
# import alpaca_trade_api as tradeapi

# # ===== Alpaca API credentials =====
# API_KEY = 'PKN1L7U3BZEVGUKGWJDZ'
# API_SECRET = 'rsH97z6DuBMBXbhoFtmILPlEmU8S94Wrln1WShH2'
# BASE_URL = 'https://paper-api.alpaca.markets'

# # Initialize Alpaca API
# api = tradeapi.REST(API_KEY, API_SECRET, BASE_URL, api_version='v2')

# # ===== Simple print logging =====
# def log(message):
#     print(message)

# # ===== Fetch transactions =====
# def fetch_transactions(created_by):
#     try:
#         conn = mysql.connector.connect(
#             host="localhost",
#             user="Hitman",
#             password="Hitman@123",
#             database="hitman_edge_dev"
#         )
#         cursor = conn.cursor()
#         cursor.execute("""
#             SELECT ticker, trade_type, quantity, price, created_by
#             FROM he_stock_transaction
#             WHERE created_by = %s;
#         """, (created_by,))
#         rows = cursor.fetchall()
#         cursor.close()
#         conn.close()
#         log(f"Fetched {len(rows)} transactions from DB for created_by={created_by}.")
#         return rows
#     except mysql.connector.Error as err:
#         log(f"⚠️ Database error: {err}")
#         return []

# # ===== Calculate average cost =====
# def calculate_avg_cost(transactions):
#     total_cost = defaultdict(float)
#     total_qty = defaultdict(float)
#     for ticker, trade_type, qty, price, created_by in transactions:
#         if trade_type.lower() == 'buy':
#             qty_f = float(qty)
#             price_f = float(price) if price is not None else 0.0
#             total_cost[(ticker, created_by)] += qty_f * price_f
#             total_qty[(ticker, created_by)] += qty_f
#     avg_cost = {}
#     for key in total_cost:
#         avg_cost[key] = total_cost[key] / total_qty[key] if total_qty[key] != 0 else 0.0
#     log(f"Average cost calculated for {len(avg_cost)} items.")
#     return avg_cost

# # ===== Place order on Alpaca =====
# def place_order(symbol, qty, side):
#     try:
#         order = api.submit_order(
#             symbol=symbol,
#             qty=qty,
#             side=side,
#             type='market',
#             time_in_force='gtc'
#         )
#         log(f"✅ Order submitted: {side.upper()} {qty} of {symbol} (Order ID: {order.id})")
#     except Exception as e:
#         log(f"❌ Order failed: {e}")
#         sys.exit(1)

# # ===== Main =====
# def main():
#     if len(sys.argv) != 5:
#         log("❌ Usage: new_aplaca.py <ticker> <buy_qty> <sell_qty> <created_by>")
#         sys.exit(1)

#     ticker = sys.argv[1].upper()
#     try:
#         buy_qty = int(sys.argv[2])
#         sell_qty = int(sys.argv[3])
#         created_by = int(sys.argv[4])  # Added created_by
#     except ValueError:
#         log("❌ Buy/Sell quantities and created_by must be integers")
#         sys.exit(1)

#     log(f"Processing order for ticker: {ticker} | Buy: {buy_qty} | Sell: {sell_qty} | Created_by: {created_by}")

#     if buy_qty > 0 and sell_qty > 0:
#         log("❌ Error: Enter either Buy Qty or Sell Qty, not both.")
#         sys.exit(1)
#     if buy_qty == 0 and sell_qty == 0:
#         log("❌ Error: Both Buy and Sell quantities are zero.")
#         sys.exit(1)

#     side = 'buy' if buy_qty > 0 else 'sell'
#     qty = buy_qty if buy_qty > 0 else sell_qty

#     log(f"▶️ Received from PowerBuilder: {ticker} {side.upper()} {qty}")

#     transactions = fetch_transactions(created_by)
#     if transactions:
#         avg_cost = calculate_avg_cost(transactions)
#         log("ℹ️ Average cost per user calculated.")

#     place_order(ticker, qty, side)

#     log("✅ Script completed successfully.")
#     sys.exit(0)

# if __name__ == "__main__":
#     main()

import sys
import mysql.connector
from collections import defaultdict
import yfinance as yf
import alpaca_trade_api as tradeapi

# ===== Alpaca API credentials =====
API_KEY = 'PKN1L7U3BZEVGUKGWJDZ'
API_SECRET = 'rsH97z6DuBMBXbhoFtmILPlEmU8S94Wrln1WShH2'
BASE_URL = 'https://paper-api.alpaca.markets'

# Initialize Alpaca API
api = tradeapi.REST(API_KEY, API_SECRET, BASE_URL, api_version='v2')

# ===== Logging helper =====
def log(message):
    print(message)

# ===== Fetch transactions for a user =====
def fetch_transactions(created_by):
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="Hitman",
            password="Hitman@123",
            database="hitman_edge_dev"
        )
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ticker, trade_type, quantity, price, created_by
            FROM he_stock_transaction
            WHERE created_by = %s;
        """, (created_by,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        log(f"Fetched {len(rows)} transactions for created_by={created_by}")
        return rows
    except mysql.connector.Error as e:
        log(f"Database error: {e}")
        return []

# ===== Calculate average cost =====
def calculate_avg_cost(transactions):
    total_cost = defaultdict(float)
    total_qty = defaultdict(float)
    for ticker, trade_type, qty, price, created_by in transactions:
        if trade_type.lower() == 'buy':
            qty_f = float(qty)
            price_f = float(price) if price else 0.0
            total_cost[(ticker, created_by)] += qty_f * price_f
            total_qty[(ticker, created_by)] += qty_f
    avg_cost = {}
    for key in total_cost:
        avg_cost[key] = total_cost[key] / total_qty[key] if total_qty[key] != 0 else 0.0
    return avg_cost

# ===== Place order on Alpaca =====
def place_order(symbol, qty, side):
    try:
        order = api.submit_order(
            symbol=symbol,
            qty=qty,
            side=side,
            type='market',
            time_in_force='gtc'
        )
        log(f"✅ Order placed: {side.upper()} {qty} of {symbol} (Order ID: {order.id})")
        return order
    except Exception as e:
        log(f"❌ Order failed: {e}")
        sys.exit(1)

# ===== Insert into he_stocks_ibkr =====
def insert_he_stocks_ibkr(ticker,  order, quantity, stock_price, avg_cost, buy_qty, sell_qty, created_by):
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="Hitman",
            password="Hitman@123",
            database="hitman_edge_dev"
        )
        cursor = conn.cursor()
        sql = """
            INSERT INTO he_stocks_ibkr
            (ticker,order_id, quantity, stock_price, avg_cost, buy_qty, sell_qty, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (ticker, order,quantity, stock_price, avg_cost, buy_qty, sell_qty, created_by))
        conn.commit()
        cursor.close()
        conn.close()
        log(f"✅ Record inserted into he_stocks_ibkr for {ticker} (qty={quantity})")
    except mysql.connector.Error as e:
        log(f"DB insert error: {e}")

# ===== Get stock price from Yahoo Finance =====
def get_stock_price(ticker):
    try:
        stock = yf.Ticker(ticker)
        price = stock.history(period="1d")['Close'][-1]
        return float(price)
    except Exception as e:
        log(f"⚠️ Failed to fetch stock price from Yahoo Finance: {e}")
        return 0.0

# ===== Main function =====
def main():
    if len(sys.argv) != 5:
        log("❌ Usage: new_aplaca.py <ticker> <buy_qty> <sell_qty> <created_by>")
        sys.exit(1)

    ticker = sys.argv[1].upper()
    try:
        buy_qty = int(sys.argv[2])
        sell_qty = int(sys.argv[3])
        created_by = int(sys.argv[4])  # Added created_by
    except ValueError:
        log("❌ Buy/Sell quantities and created_by must be integers")
        sys.exit(1)


    # ===== Validation =====
    if buy_qty == 0 and sell_qty == 0:
        log("❌ Both Buy and Sell quantities are zero.")
        sys.exit(1)

    # ===== Fetch previous transactions =====
    transactions = fetch_transactions(created_by)
    avg_cost_dict = calculate_avg_cost(transactions) if transactions else {}
    avg_cost = avg_cost_dict.get((ticker, created_by), 0.0)

    # ===== Get current stock price =====
    stock_price = get_stock_price(ticker)

    # ===== Determine total quantity for DB =====
    total_qty = buy_qty - sell_qty

    order_id = None
    # If both buy and sell orders placed, choose logic as per need; here saving first non-None
    if buy_qty > 0:
        order_id = place_order(ticker, buy_qty, 'buy').id   
    if sell_qty > 0 and order_id is None:
        order_id = place_order(ticker, sell_qty, 'sell').id

    # ===== Insert into DB =====
    insert_he_stocks_ibkr(
        ticker=ticker,
        order=order_id,
        quantity=total_qty,
        stock_price=stock_price,
        avg_cost=avg_cost,
        buy_qty=buy_qty,
        sell_qty=sell_qty,
        created_by=created_by
    )

    # ===== Place orders =====
    if buy_qty > 0:
        place_order(ticker, buy_qty, 'buy')
    if sell_qty > 0:
        place_order(ticker, sell_qty, 'sell')

    log("✅ Script completed successfully.")
    sys.exit(0)

if __name__ == "__main__":
    main()
