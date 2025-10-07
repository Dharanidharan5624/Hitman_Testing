# portfolio_master_table
import pandas as pd
from datetime import datetime
from collections import deque, defaultdict
from decimal import Decimal, InvalidOperation
import yfinance as yf
from tabulate import tabulate
import mysql.connector
import math
from HE_database_connect import get_connection 


def safe_round(val, digits=2):
    try:
        return round(float(val), digits)
    except (ValueError, TypeError, InvalidOperation):
        return 0

def clean_dataframe(df):
    for col in df.columns:
        df[col] = df[col].apply(lambda x: None if isinstance(x, float) and (math.isinf(x) or math.isnan(x)) else x)
    return df

def fetch_fifo_data():
    try:
        conn = get_connection()  
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ticker, date, trade_type, quantity, price, platform, created_by
            FROM he_stock_transaction;
        """)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        print("\nFetched Transactions:")
        print(tabulate(rows, headers=["Ticker", "Date", "Type", "Qty", "Price", "Platform", "Created By"], tablefmt="grid"))
        return rows

    except mysql.connector.Error as err:
        print(f"Database Error: {err}")
        return []

def safe_get(df, keys):
    for key in keys:
        if key in df.index:
            return df.loc[key].iloc[0]
    return None

def get_index_return(ticker):
    try:
        hist = yf.Ticker(ticker).history(period="1y")
        if hist.empty:
            return None
        start_price = hist['Close'].iloc[0]
        end_price = hist['Close'].iloc[-1]
        return round((end_price - start_price) / start_price * 100, 2)
    except:
        return None

# ---------- Index Returns ----------

sp500_return = get_index_return("^GSPC")
nasdaq_return = get_index_return("^IXIC")
russell1000_return = get_index_return("^RUI")

grouped = defaultdict(list)
platform_map = {}

# ---------- Fetch Transactions and Group ----------

for t in fetch_fifo_data():
    if len(t) != 7:
        print(f"Skipping invalid row (length != 7): {t}")
        continue

    ticker, date_obj, action, qty, price, platform, created_by = t

    ticker = ticker or "UNKNOWN"
    platform = platform or "UNKNOWN"
    action = (action or "unknown").strip().lower()

    try:
        qty = Decimal(qty) if qty is not None else Decimal('0')
    except (InvalidOperation, TypeError):
        print(f"⚠️ Invalid quantity for row: {t}")
        qty = Decimal('0')

    try:
        price = Decimal(price) if price is not None else Decimal('0')
    except (InvalidOperation, TypeError):
        print(f"⚠️ Invalid price for row: {t}")
        price = Decimal('0')

    if not date_obj:
        print(f"⚠️ Missing date for row: {t}, using today's date")
        date_str = datetime.today().strftime("%Y-%m-%d")
    else:
        date_str = date_obj.strftime("%Y-%m-%d")

    grouped[ticker].append((date_str, ticker, action, qty, price, platform, created_by))
    platform_map[ticker] = platform

# ---------- Process Each Ticker ----------

summary_list = []

for ticker, txns in grouped.items():
    print(f"\n📊 Processing: {ticker}")
    holdings = deque()
    cumulative_buy_cost = Decimal('0')
    total_qty = Decimal('0')
    realized_gain_loss = Decimal('0')
    first_buy_date = None

    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="260d")
        if hist.empty or 'Close' not in hist:
            print(f"❌ Skipping {ticker} — No valid historical data.")
            continue
    except Exception as e:
        print(f"❌ Skipping {ticker} — Error fetching history: {e}")
        continue

    ema_50 = safe_round(hist['Close'].ewm(span=50, adjust=False).mean().iloc[-1])
    ema_100 = safe_round(hist['Close'].ewm(span=100, adjust=False).mean().iloc[-1])
    ema_200 = safe_round(hist['Close'].ewm(span=200, adjust=False).mean().iloc[-1])

    try:
        info = stock.info
        current_price = Decimal(info.get('currentPrice', 0))
        category = info.get('sector', 'Unknown')
    except Exception as e:
        print(f"⚠️ Failed to fetch info for {ticker}: {e}")
        current_price = Decimal('0')
        category = "Unknown"
        info = {}

    for date_str, symbol, action, qty, price, platform, created_by in txns:
        try:
            date = datetime.strptime(date_str, "%Y-%m-%d")
        except Exception as e:
            print(f"Invalid date format: {date_str} in {symbol} — {e}")
            continue

        if action == 'buy':
            if not first_buy_date:
                first_buy_date = date
            holdings.append([qty, price, date])
            total_qty += qty
            cumulative_buy_cost += qty * price

        elif action == 'sell':
            sell_qty = qty
            while sell_qty > 0 and holdings:
                h_qty, h_price, h_date = holdings[0]
                used_qty = min(sell_qty, h_qty)
                profit = (price - h_price) * used_qty
                realized_gain_loss += profit
                cumulative_buy_cost -= used_qty * h_price
                total_qty -= used_qty
                if used_qty == h_qty:
                    holdings.popleft()
                else:
                    holdings[0][0] -= used_qty
                sell_qty -= used_qty

    avg_cost = cumulative_buy_cost / total_qty if total_qty else Decimal('0')
    total_cost = cumulative_buy_cost
    unrealized = (current_price - avg_cost) * total_qty if total_qty else Decimal('0')

    today = datetime.today()
    first_buy_age = (today - first_buy_date).days if first_buy_date else "-"
    average_age = (
        sum((today - h[2]).days * float(h[0]) for h in holdings) / float(total_qty)
        if total_qty > 0 else "-"
    )

    try:
        balance_sheet = stock.balance_sheet
        income_stmt = stock.financials
        cashflow_stmt = stock.cashflow

        net_income = safe_get(income_stmt, ["Net Income", "Net Income Applicable To Common Shares"])
        equity = safe_get(balance_sheet, ["Total Stockholder Equity", "Common Stock Equity"])
        total_revenue = safe_get(income_stmt, ["Total Revenue"])
        current_assets = safe_get(balance_sheet, ["Total Current Assets", "Current Assets"])
        current_liabilities = safe_get(balance_sheet, ["Total Current Liabilities", "Current Liabilities"])
        inventory = safe_get(balance_sheet, ["Inventory", "Total Inventory"]) or 0
        total_debt = safe_get(balance_sheet, ["Total Debt", "Short Long Term Debt Total"])

        op_cashflow = next((cashflow_stmt.loc[row].iloc[0] for row in cashflow_stmt.index if "operating" in row.lower()), None)
        capex = next((cashflow_stmt.loc[row].iloc[0] for row in cashflow_stmt.index if "capital expenditure" in row.lower()), 0)
        fcf = (op_cashflow + capex) if op_cashflow else None

    except Exception as e:
        print(f"⚠️ Financial data missing for {ticker}: {e}")
        net_income = equity = total_revenue = current_assets = current_liabilities = total_debt = None
        inventory = 0
        fcf = None

    eps = info.get("trailingEps") or info.get("forwardEps")
    growth_rate = info.get("earningsQuarterlyGrowth", 0.12)
    fwd_rev_growth = info.get("revenueGrowth")
    surprise_pct = info.get("earningsQuarterlyGrowth")
    market_cap = info.get("marketCap")

    pe_ratio = safe_round(current_price / Decimal(eps)) if current_price and eps else None
    peg_ratio = safe_round(pe_ratio / (growth_rate * 100)) if pe_ratio and growth_rate else None
    roe = safe_round((net_income / equity) * 100) if net_income and equity else None
    current_ratio = safe_round(current_assets / current_liabilities) if current_assets and current_liabilities else None
    quick_ratio = safe_round((current_assets - inventory) / current_liabilities) if current_assets and current_liabilities else None
    de_ratio = safe_round(total_debt / equity) if total_debt and equity else None
    net_profit_margin = safe_round((net_income / total_revenue) * 100) if net_income and total_revenue else None
    fcf_yield = safe_round((fcf / market_cap) * 100) if fcf and market_cap else None

    summary_list.append({
        "ticker": ticker,
        "Category": category,
        "quantity": float(total_qty),
        "avg_cost": safe_round(avg_cost),
        "total_cost": safe_round(total_cost),
        "current_price": safe_round(current_price),
        "unrealized_gain_loss": safe_round(unrealized),
        "realized_gain_loss": safe_round(realized_gain_loss),
        "first_buy_age": first_buy_age,
        "avg_age_days": round(average_age, 1) if isinstance(average_age, float) else average_age,
        "platform": platform_map[ticker],
        "industry_pe": safe_round(info.get('trailingPE')),
        "current_pe": safe_round(info.get('forwardPE')),
        "price_sales_ratio": safe_round(info.get('priceToSalesTrailing12Months')),
        "price_book_ratio": safe_round(info.get('priceToBook')),
        "50_day_ema": ema_50,
        "100_day_ema": ema_100,
        "200_day_ema": ema_200,
        "sp_500_ya": sp500_return,
        "nasdaq_ya": nasdaq_return,
        "russell_1000_ya": russell1000_return,
        "pe_ratio": pe_ratio,
        "peg_ratio": peg_ratio,
        "roe": roe,
        "net_profit_margin": net_profit_margin,
        "current_ratio": current_ratio,
        "debt_equity": de_ratio,
        "fcf_yield": fcf_yield,
        "revenue_growth": safe_round(fwd_rev_growth * 100) if isinstance(fwd_rev_growth, (float, int)) else None,
        "earnings_accuracy": safe_round(surprise_pct * 100) if isinstance(surprise_pct, (float, int)) else None,
        "created_by": created_by
    })

# ---------- Create DataFrame and Insert ----------

df = pd.DataFrame(summary_list)

if not df.empty:
    df['position_size'] = (df['total_cost'] / df['total_cost'].sum()).round(2)

    print("\n📈 Portfolio Summary:")
    print(tabulate(df, headers="keys", tablefmt="grid"))

    # Fill any missing columns
    required_columns = [
        "ticker", "Category", "quantity", "avg_cost", "position_size", "total_cost", "current_price",
        "unrealized_gain_loss", "realized_gain_loss", "first_buy_age", "avg_age_days", "platform",
        "industry_pe", "current_pe", "price_sales_ratio", "price_book_ratio",
        "50_day_ema", "100_day_ema", "200_day_ema",
        "sp_500_ya", "nasdaq_ya", "russell_1000_ya",
        "pe_ratio", "peg_ratio", "roe", "net_profit_margin", "current_ratio", "debt_equity", "fcf_yield",
        "revenue_growth", "earnings_accuracy", "created_by"
    ]
    for col in required_columns:
        if col not in df.columns:
            df[col] = None

    # Clean any NaNs/inf from float columns
    df = clean_dataframe(df)

    try:
        conn = get_connection()  
        cursor = conn.cursor()

        query = """
            INSERT INTO he_portfolio_master (
                ticker, Category, quantity, avg_cost, position_size, total_cost, current_price,
                unrealized_gain_loss, realized_gain_loss, first_buy_age, avg_age_days, platform,
                industry_pe, current_pe, price_sales_ratio, price_book_ratio,
                `50_day_ema`, `100_day_ema`, `200_day_ema`,
                sp_500_ya, nasdaq_ya, russell_1000_ya,
                pe_ratio, peg_ratio, roe, net_profit_margin, current_ratio, debt_equity, fcf_yield,
                revenue_growth, earnings_accuracy,
                created_by
            )
            VALUES (
                %(ticker)s, %(Category)s, %(quantity)s, %(avg_cost)s, %(position_size)s, %(total_cost)s, %(current_price)s,
                %(unrealized_gain_loss)s, %(realized_gain_loss)s, %(first_buy_age)s, %(avg_age_days)s, %(platform)s,
                %(industry_pe)s, %(current_pe)s, %(price_sales_ratio)s, %(price_book_ratio)s,
                %(50_day_ema)s, %(100_day_ema)s, %(200_day_ema)s,
                %(sp_500_ya)s, %(nasdaq_ya)s, %(russell_1000_ya)s,
                %(pe_ratio)s, %(peg_ratio)s, %(roe)s, %(net_profit_margin)s, %(current_ratio)s, %(debt_equity)s, %(fcf_yield)s,
                %(revenue_growth)s, %(earnings_accuracy)s,
                %(created_by)s
            )
        """

        cursor.executemany(query, df.to_dict(orient="records"))
        conn.commit()
        print("\n✅ Data inserted into `he_portfolio_master` successfully.")

    except mysql.connector.Error as err:
        print(f"\n❌ MySQL Insertion Error: {err}")

    finally:
        cursor.close()
        conn.close()
else:
    print("\n⚠️ No valid data available to insert.")



# scedule
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import subprocess
import os
import mysql.connector
from win10toast import ToastNotifier
import traceback
import sys
from HE_database_connect import get_connection  

toaster = ToastNotifier()

SCRIPT_FOLDER = os.path.dirname(os.path.abspath(__file__))

if len(sys.argv) != 6:
    print("Error: Expected 5 arguments")
    print("Usage: python scheduler.py <job_name> <start_time> <frequency> <schedule_type> <created_by>")
    sys.exit(1)

job_name = sys.argv[1]
start_time = sys.argv[2]
schedule_frequency = sys.argv[3].lower()
schedule_type = sys.argv[4].title()
created_by = int(sys.argv[5])

print(f"✅ Job Name: {job_name}")
print(f"✅ Start Time: {start_time}")
print(f"✅ Frequency: {schedule_frequency}")
print(f"✅ Schedule Type: {schedule_type}")
print(f"✅ Created By: {created_by}")

def show_notification(title, message):
    try:
        print(f"[NOTIFY] {title}: {message}")
        toaster.show_toast(title, message, duration=4)
    except Exception as e:
        print(f"[TOAST ERROR] {e}")

def get_next_id(table, column):
    conn = get_connection() 
    cursor = conn.cursor(buffered=True)
    cursor.execute(f"SELECT MAX({column}) FROM {table}")
    result = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return (result or 0) + 1

def insert_or_update_job(job_name, schedule_time, schedule_frequency, schedule_type, created_by):
    conn = get_connection() 
    cursor = conn.cursor(buffered=True)

    cursor.execute("SELECT job_number FROM he_job_master WHERE job_name = %s", (job_name,))
    result = cursor.fetchone()

    if result:
        cursor.execute("""
            UPDATE he_job_master
            SET start_time = %s, schedule_frequency = %s, schedule_type = %s, updated_at = NOW()
            WHERE job_name = %s
        """, (schedule_time, schedule_frequency, schedule_type, job_name))
    else:
        job_number = get_next_id("he_job_master", "job_number")
        cursor.execute("""
            INSERT INTO he_job_master (
                job_number, job_name, start_time, schedule_frequency, schedule_type,
                created_by, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
        """, (job_number, job_name, schedule_time, schedule_frequency, schedule_type, created_by))

    conn.commit()
    cursor.close()
    conn.close()


def get_next_run_number(job_number):
    conn = get_connection() 
    cursor = conn.cursor(buffered=True)
    cursor.execute("SELECT MAX(job_run_number) FROM he_job_execution WHERE job_number = %s", (job_number,))
    result = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return (result or 0) + 1

# === Log to Job Logs Table ===
def log_job(job_number, run_number, description, created_by):
    conn = get_connection() 
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO he_job_logs (job_number, job_run_number, job_log_description,
                                 created_by, created_at, updated_at)
        VALUES (%s, %s, %s, %s, NOW(), NOW())
    """, (job_number, run_number, description, created_by))
    conn.commit()
    cursor.close()
    conn.close()

# === Job Execution Logic ===
def run_scheduled_job(job_name, created_by=1):
    conn = get_connection()  # Use get_connection
    cursor = conn.cursor(buffered=True)

    cursor.execute("SELECT job_number FROM he_job_master WHERE job_name = %s", (job_name,))
    job_number_row = cursor.fetchone()
    if not job_number_row:
        print(f"[ERROR] Job '{job_name}' not found.")
        return

    job_number = job_number_row[0]
    job_run_number = get_next_run_number(job_number)
    start_time_now = datetime.now()

    try:
        cursor.execute("""
            INSERT INTO he_job_execution (
                job_number, job_run_number, execution_status, start_datetime,
                created_by, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
        """, (job_number, job_run_number, "RUNNING", start_time_now, created_by))
        conn.commit()

        log_job(job_number, job_run_number, f"{job_name} started at {start_time_now}", created_by)

        # ✅ Execute the target job script
        script_path = os.path.join(SCRIPT_FOLDER, f"{job_name}.py")
        print(f"[DEBUG] Executing: {script_path}")
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"Script not found: {script_path}")

        subprocess.run(["python", script_path], check=True)

        end_time = datetime.now()

        cursor.execute("""
            UPDATE he_job_execution
            SET execution_status = %s, end_datetime = %s, updated_at = NOW()
            WHERE job_number = %s AND job_run_number = %s
        """, ("SUCCESS", end_time, job_number, job_run_number))

        cursor.execute("""
            UPDATE he_job_master
            SET end_time = %s, updated_at = NOW()
            WHERE job_number = %s
        """, (end_time, job_number))

        conn.commit()
        log_job(job_number, job_run_number, f"{job_name} completed successfully at {end_time}", created_by)
        show_notification("✅ Job Success", f"{job_name} finished at {end_time.strftime('%H:%M:%S')}")

    except subprocess.CalledProcessError as e:
        end_time = datetime.now()
        cursor.execute("""
            UPDATE he_job_execution
            SET execution_status = %s, end_datetime = %s, updated_at = NOW()
            WHERE job_number = %s AND job_run_number = %s
        """, ("FAILED", end_time, job_number, job_run_number))

        cursor.execute("""
            UPDATE he_job_master
            SET end_time = %s, updated_at = NOW()
            WHERE job_number = %s
        """, (end_time, job_number))

        conn.commit()
        log_job(job_number, job_run_number, f"{job_name} failed: {e}", created_by)
        show_notification("❌ Job Failed", f"{job_name} failed at {end_time.strftime('%H:%M:%S')}")

    except Exception as e:
        print("[UNEXPECTED ERROR]")
        traceback.print_exc()
        log_job(job_number, job_run_number, f"Unexpected error: {str(e)}", created_by)

    finally:
        cursor.close()
        conn.close()

# === Schedule Setup ===
def schedule_job(job_name, schedule_time, schedule_frequency):
    try:
        time_obj = datetime.strptime(schedule_time, "%H:%M:%S")
    except ValueError:
        print("❌ Error: start_time format must be HH:MM:SS")
        return

    scheduler = BlockingScheduler()

    if schedule_frequency == 'daily':
        scheduler.add_job(lambda: run_scheduled_job(job_name, created_by), 'cron',
                          hour=time_obj.hour, minute=time_obj.minute, second=time_obj.second)
    elif schedule_frequency == 'weekly':
        scheduler.add_job(lambda: run_scheduled_job(job_name, created_by), 'cron',
                          day_of_week='mon', hour=time_obj.hour, minute=time_obj.minute, second=time_obj.second)
    elif schedule_frequency == 'monthly':
        scheduler.add_job(lambda: run_scheduled_job(job_name, created_by), 'cron',
                          day=1, hour=time_obj.hour, minute=time_obj.minute, second=time_obj.second)
    else:
        print("❌ Error: Frequency must be daily, weekly, or monthly")
        return

    show_notification("Scheduler Started", f"{job_name} will run {schedule_frequency} at {schedule_time}")
    print(f"[SCHEDULER] {job_name} scheduled {schedule_frequency} at {schedule_time}")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("[STOPPED] Scheduler stopped.")

# === Main Entry Point ===
def main():
    insert_or_update_job(job_name, start_time, schedule_frequency, schedule_type, created_by)
    schedule_job(job_name, start_time, schedule_frequency)

if __name__ == "__main__":
    main()


# upcomming_earnings_report

import requests
import time
import pandas as pd
import yfinance as yf
from tabulate import tabulate
import mysql.connector
from mysql.connector import Error
from email.message import EmailMessage
import smtplib
from datetime import datetime
from dateutil.relativedelta import relativedelta
from HE_database_connect import get_connection 


API_KEY = 'd0a8q79r01qnh1rh09v0d0a8q79r01qnh1rh09vg'
start_date = datetime.today()

end_date = start_date + relativedelta(months=1)

start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')

calendar_url = f'https://finnhub.io/api/v1/calendar/earnings?from={start_date}&to={end_date}&token={API_KEY}'
company_cache = {}

EXCLUDE_KEYWORDS = ["fund", "trust", "etf", "reit", "insurance", "life", "portfolio"]

sender_email = "dhineshapihitman@gmail.com"
receiver_email = "dharanidharan@shravtek.com"
subject = "Earnings Calendar Report"
app_password = "yiof ntnc xowc gpbp"

def convert_hour(hour_code):
    if not hour_code:
        return 'NULL'
    hour_code = hour_code.lower()
    return {
        'bmo': 'Before Market Open',
        'amc': 'After Market Close',
        'dmt': 'During Market Trading'
    }.get(hour_code, 'NULL')

def get_company_name(symbol):
    if symbol in company_cache:
        return company_cache[symbol]
    profile_url = f'https://finnhub.io/api/v1/stock/profile2?symbol={symbol}&token={API_KEY}'
    while True:
        try:
            response = requests.get(profile_url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                name = data.get('name', 'N/A')
                company_cache[symbol] = name
                return name
            elif response.status_code == 429:
                print(f"Rate limit hit for {symbol}. Retrying in 60 sec...")
                time.sleep(60)
                continue
            else:
                return 'N/A'
        except Exception as e:
            print(f"Exception fetching profile for {symbol}: {e}")
            return 'N/A'

def get_actual_eps(symbol, earnings_date):
    earnings_url = f'https://finnhub.io/api/v1/stock/earnings?symbol={symbol}&token={API_KEY}'
    while True:
        try:
            response = requests.get(earnings_url, timeout=5)
            if response.status_code == 200:
                for record in response.json():
                    if record.get("period", "").startswith(earnings_date):
                        return record.get("actual", None)
                return None
            elif response.status_code == 429:
                print(f"Rate limit hit on earnings for {symbol}. Retrying in 60 sec...")
                time.sleep(60)
                continue
            else:
                return None
        except Exception as e:
            print(f"Error fetching actual EPS for {symbol}: {e}")
            return None

def get_last_year_eps(symbol, current_date):
    earnings_url = f'https://finnhub.io/api/v1/stock/earnings?symbol={symbol}&token={API_KEY}'
    current_year = int(current_date[:4])
    while True:
        try:
            response = requests.get(earnings_url, timeout=5)
            if response.status_code == 200:
                for record in response.json():
                    if record.get("period", "").startswith(str(current_year - 1)):
                        return record.get("actual", None)
                return None
            elif response.status_code == 429:
                print(f"Rate limit hit on earnings for {symbol}. Retrying in 60 sec...")
                time.sleep(60)
                continue
            else:
                return None
        except Exception as e:
            print(f"Error fetching last year EPS for {symbol}: {e}")
            return None

def create_mysql_connection():
    try:
        connection = get_connection()  # Use get_connection instead of hardcoded credentials
        if connection.is_connected():
            print("✅ Connected to MySQL database")
            return connection
    except Error as e:
        print("❌ Error while connecting to MySQL:", e)
    return None

def format_market_cap(value):
    if value is None:
        return 'NULL'
    try:
        return f"${value / 1e9:.2f}B"
    except:
        return 'NULL'

def main():
    response = requests.get(calendar_url)
    if response.status_code != 200:
        print("Error fetching earnings calendar:", response.text)
        return

    earnings = response.json().get('earningsCalendar', [])
    if not earnings:
        print("No earnings data found.")
        return

    results = []
    total = len(earnings)

    for i, e in enumerate(earnings, 1):
        symbol = e.get('symbol', 'N/A')
        if symbol == 'N/A':
            continue

        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            market_cap = info.get("marketCap", 0)
            if market_cap is None or market_cap < 1_000_000_000:
                print(f"⏩ Skipping {symbol}: Market cap < $1B")
                continue
           
            formatted_cap = format_market_cap(market_cap)
        except Exception as ex:
            print(f"❌ Error fetching market cap for {symbol}: {ex}")
            continue

        earnings_date = e.get('date', None)
        eps_estimate = e.get('epsEstimate', None)
        time_str = convert_hour(e.get('hour', 'N/A'))
        company_name = get_company_name(symbol)

        if any(keyword in company_name.lower() for keyword in EXCLUDE_KEYWORDS):
            print(f"⏩ Skipping {symbol} - {company_name} (excluded by keyword)")
            continue

        actual_eps = get_actual_eps(symbol, earnings_date) if earnings_date else None
        last_year_eps = get_last_year_eps(symbol, earnings_date) if earnings_date else None

        try:
            hist = ticker.history(period='1mo')
            current_price = hist['Close'].iloc[-1] if not hist.empty else None
            volatility = hist['Close'].pct_change().std() * (252**0.5) if not hist.empty else None
        except Exception as ex:
            print(f"⚠️ Error fetching price/volatility for {symbol}: {ex}")
            current_price = None
            volatility = None

        results.append({
            "Company Name": company_name or "NULL",
            "Ticker Symbol": symbol or "NULL",
            "Earnings Date": earnings_date or "NULL",
            "Time": time_str or "NULL",
            "EPS Estimate": eps_estimate if eps_estimate is not None else "NULL",
            "Actual EPS": actual_eps if actual_eps is not None else "NULL",
            "Last Year EPS": last_year_eps if last_year_eps is not None else "NULL",
            "Market Cap": formatted_cap,
            "Current Price" : f"${current_price:.2f}" if current_price else "NULL",
            "Volatility": f"{volatility:.2%}" if isinstance(volatility, float) else "NULL"
        })

        print(f"✅ Processed {i}/{total} - {symbol} | Market Cap: {formatted_cap}")
        time.sleep(0.2)

    if not results:
        print("ℹ️ No records to insert or email.")
        return

    df = pd.DataFrame(results)
    print(tabulate(df, headers='keys', tablefmt='pretty', showindex=False))

    conn = create_mysql_connection()
    if conn:
        try:
            insert_sql = '''
            INSERT INTO he_upcoming_earning_report (
                company_name, ticker_symbol, earnings_date, time, eps_estimate,
                actual_eps, market_cap, current_price, volatility
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''
            cursor = conn.cursor()
            values = [
                (row["Company Name"], row["Ticker Symbol"], row["Earnings Date"], row["Time"],
                 row["EPS Estimate"], row["Actual EPS"], row["Market Cap"],
                 row["Current Price"], row["Volatility"])
                for row in results
            ]
            cursor.executemany(insert_sql, values)
            conn.commit()
            print(f"✅ {cursor.rowcount} records inserted into MySQL.")
        except Exception as e:
            print("❌ Insert failed (check schema):", e)
        finally:
            conn.close()

    # Email - HTML body
    html_body = """
    <html>
    <head>
      <style>
        table { border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; }
        th, td { border: 1px solid #ccc; padding: 8px; text-align: left; color: #000; }
        th { background-color: #f2f2f2; }
        h2 { color: #000; }
      </style>
    </head>
    <body>
      <h2>Earnings Calendar Report</h2>
      <table>
        <tr>
          <th>Company Name</th><th>Ticker</th><th>Date</th><th>Time</th>
          <th>Est. EPS</th><th>Actual EPS</th><th>Last Yr EPS</th>
          <th>Market Cap</th><th>Price</th><th>Volatility</th>
        </tr>
    """
    for row in results:
        html_body += f"""
        <tr>
          <td>{row['Company Name']}</td>
          <td>{row['Ticker Symbol']}</td>
          <td>{row['Earnings Date']}</td>
          <td>{row['Time']}</td>
          <td>{row['EPS Estimate']}</td>
          <td>{row['Actual EPS']}</td>
          <td>{row['Last Year EPS']}</td>
          <td>{row['Market Cap']}</td>
          <td>{row['Current Price']}</td>
          <td>{row['Volatility']}</td>
        </tr>
        """

    html_body += "</table></body></html>"

    msg = EmailMessage()
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg["Subject"] = subject
    msg.set_content("This is an HTML email. Please open in an HTML-compatible viewer.")
    msg.add_alternative(html_body, subtype='html')

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(sender_email, app_password)
            smtp.send_message(msg)
        print("✅ Email sent successfully!")
    except Exception as e:
        print("❌ Email sending failed:", e)

if __name__ == "__main__":
    main()