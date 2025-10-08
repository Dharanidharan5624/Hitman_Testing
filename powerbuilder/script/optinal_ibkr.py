# import yfinance as yf
# import pandas as pd
# from tabulate import tabulate
# import sys
# import mysql.connector

# # === DB Configuration ===
# DB_CONFIG = {
#     'host': 'localhost',
#     'user': 'Hitman',
#     'password': 'Hitman@123',
#     'database': 'hitman_edge_dev'
# }

# def main():
#     if len(sys.argv) != 3:
#         print("❌ Usage: python script.py <symbol> <created_by>")
#         sys.exit(1)

#     symbol = sys.argv[1].upper()
#     created_by = sys.argv[2]

#     print(f"Received symbol: {symbol}")
#     print(f"Requested by user ID: {created_by}")

#     # Fetch ticker and expiration dates with error handling
#     try:
#         ticker = yf.Ticker(symbol)
#         expirations = ticker.options
#         if not expirations:
#             print(f"❌ No option data found for {symbol}")
#             sys.exit(1)
#     except Exception as e:
#         print(f"❌ Failed to fetch option expirations for {symbol}: {e}")
#         sys.exit(1)

#     expiry = expirations[0]
#     print(f"\n🔍 Fetching options for: {symbol}, Expiry: {expiry}")

#     # Fetch options chain
#     try:
#         opt_chain = ticker.option_chain(expiry)
#     except Exception as e:
#         print(f"❌ Failed to fetch option chain for {symbol} at {expiry}: {e}")
#         sys.exit(1)

#     calls = opt_chain.calls
#     puts = opt_chain.puts

#     # Clean and rename calls data
#     calls_df = calls[['strike', 'lastPrice', 'bid']].copy()
#     calls_df.columns = ['Options Available', 'Premium Call', 'Sell Call']

#     # Clean and rename puts data
#     puts_df = puts[['strike', 'lastPrice', 'ask', 'bid']].copy()
#     puts_df.columns = ['Options Available', 'Premium Put', 'Buy Put', 'Sell Put']

#     # Merge calls and puts on strike price
#     df = pd.merge(calls_df, puts_df, on='Options Available')
#     if df.empty:
#         print(f"❌ No merged options data available for {symbol} at expiry {expiry}")
#         sys.exit(1)

#     numeric_cols = ['Premium Call', 'Sell Call', 'Premium Put', 'Buy Put', 'Sell Put']
#     for col in numeric_cols:
#         df[col] = df[col].round(2)

#     df['created_by'] = created_by

#     # Insert to MySQL
#     try:
#         conn = mysql.connector.connect(**DB_CONFIG)
#         cursor = conn.cursor()
#         insert_query = """
#             INSERT INTO he_options_ibkr (
#                 options_available,
#                 premium_call,
#                 sell_call,
#                 premium_put,
#                 buy_put,
#                 sell_put,
#                 created_by
#             ) VALUES (%s, %s, %s, %s, %s, %s, %s)
#         """
#         rows_inserted = 0
#         for _, row in df.iterrows():
#             cursor.execute(insert_query, (
#                 row['Options Available'],
#                 row['Premium Call'],
#                 row['Sell Call'],
#                 row['Premium Put'],
#                 row['Buy Put'],
#                 row['Sell Put'],
#                 row['created_by']
#             ))
#             rows_inserted += 1
#         conn.commit()
#         print(f"\n✅ Inserted {rows_inserted} rows into he_options_ibkr.")
#     except mysql.connector.Error as err:
#         print(f"❌ MySQL Error: {err}")
#         sys.exit(1)
#     finally:
#         if 'cursor' in locals() and cursor:
#             cursor.close()
#         if 'conn' in locals() and conn:
#             conn.close()

#     print("\n📊 Options Chain Preview:")
#     table = tabulate(df.head(10), headers='keys', tablefmt='pretty', showindex=False)
#     print(table)

# if __name__ == "__main__":
#     main()
import yfinance as yf
from datetime import datetime

symbol = "MSFT"
ticker = yf.Ticker(symbol)

# Fetch all expiration dates (strings 'YYYY-MM-DD')
option_dates = ticker.options

# Format as "Mon DD, YYYY (N days)" where N = days from today
def format_expiry(date_str):
    expiry_date = datetime.strptime(date_str, "%Y-%m-%d")
    today = datetime.today()
    delta_days = (expiry_date - today).days
    return f"{expiry_date.strftime('%b %d, %Y')} ({delta_days} days)"

# Generate formatted list for dropdown-like display
dropdown_dates = [format_expiry(d) for d in option_dates]

# Show formatted dates
for d in dropdown_dates:
    print(d)
