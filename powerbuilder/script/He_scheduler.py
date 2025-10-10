from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import subprocess
import os
import mysql.connector
from win10toast import ToastNotifier
import traceback
import sys
from HE_database_connect import get_connection
from HE_error_logs import log_error_to_db  # Import error logging function

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
        error_message = f"Toast notification error: {e}"
        print(f"[TOAST ERROR] {e}")
        log_error_to_db(error_message, type(e).__name__, "HE_scheduler.py", created_by)

def get_next_id(table, column):
    try:
        conn = get_connection()
        cursor = conn.cursor(buffered=True)
        cursor.execute(f"SELECT MAX({column}) FROM {table}")
        result = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return (result or 0) + 1
    except mysql.connector.Error as err:
        error_message = f"Database error in get_next_id for table {table}: {err}"
        print(error_message)
        log_error_to_db(error_message, type(err).__name__, "HE_scheduler.py", created_by)
        raise

def insert_or_update_job(job_name, schedule_time, schedule_frequency, schedule_type, created_by):
    try:
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
    except mysql.connector.Error as err:
        error_message = f"Database error in insert_or_update_job for job {job_name}: {err}"
        print(error_message)
        log_error_to_db(error_message, type(err).__name__, "HE_scheduler.py", created_by)
        raise

def get_next_run_number(job_number):
    try:
        conn = get_connection()
        cursor = conn.cursor(buffered=True)
        cursor.execute("SELECT MAX(job_run_number) FROM he_job_execution WHERE job_number = %s", (job_number,))
        result = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return (result or 0) + 1
    except mysql.connector.Error as err:
        error_message = f"Database error in get_next_run_number for job_number {job_number}: {err}"
        print(error_message)
        log_error_to_db(error_message, type(err).__name__, "HE_scheduler.py", created_by)
        raise

def log_job(job_number, run_number, description, created_by):
    try:
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
    except mysql.connector.Error as err:
        error_message = f"Database error in log_job for job_number {job_number}, run_number {run_number}: {err}"
        print(error_message)
        log_error_to_db(error_message, type(err).__name__, "HE_scheduler.py", created_by)
        raise

def run_scheduled_job(job_name, created_by=1):
    try:
        conn = get_connection()
        cursor = conn.cursor(buffered=True)

        cursor.execute("SELECT job_number FROM he_job_master WHERE job_name = %s", (job_name,))
        job_number_row = cursor.fetchone()
        if not job_number_row:
            error_message = f"Job '{job_name}' not found in he_job_master"
            print(f"[ERROR] {error_message}")
            log_error_to_db(error_message, "ValueError", "HE_scheduler.py", created_by)
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
            error_message = f"{job_name} failed: {e}"
            log_job(job_number, job_run_number, error_message, created_by)
            log_error_to_db(error_message, type(e).__name__, "HE_scheduler.py", created_by)
            show_notification("❌ Job Failed", f"{job_name} failed at {end_time.strftime('%H:%M:%S')}")

        except Exception as e:
            end_time = datetime.now()
            error_message = f"Unexpected error in {job_name}: {e}"
            print("[UNEXPECTED ERROR]")
            traceback.print_exc()
            log_job(job_number, job_run_number, error_message, created_by)
            log_error_to_db(error_message, type(e).__name__, "HE_scheduler.py", created_by)

        finally:
            cursor.close()
            conn.close()

    except mysql.connector.Error as err:
        error_message = f"Database error in run_scheduled_job for job {job_name}: {err}"
        print(error_message)
        log_error_to_db(error_message, type(err).__name__, "HE_scheduler.py", created_by)

def schedule_job(job_name, schedule_time, schedule_frequency):
    try:
        time_obj = datetime.strptime(schedule_time, "%H:%M:%S")
    except ValueError as e:
        error_message = f"Invalid start_time format for {job_name}: {e}"
        print(f"❌ Error: start_time format must be HH:MM:SS")
        log_error_to_db(error_message, type(e).__name__, "HE_scheduler.py", created_by)
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
        error_message = f"Invalid frequency for {job_name}: {schedule_frequency}"
        print("❌ Error: Frequency must be daily, weekly, or monthly")
        log_error_to_db(error_message, "ValueError", "HE_scheduler.py", created_by)
        return

    show_notification("Scheduler Started", f"{job_name} will run {schedule_frequency} at {schedule_time}")
    print(f"[SCHEDULER] {job_name} scheduled {schedule_frequency} at {schedule_time}")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("[STOPPED] Scheduler stopped.")

def main():
    insert_or_update_job(job_name, start_time, schedule_frequency, schedule_type, created_by)
    schedule_job(job_name, start_time, schedule_frequency)

if __name__ == "__main__":
    main()