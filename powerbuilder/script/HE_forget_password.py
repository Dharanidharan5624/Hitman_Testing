import smtplib
import random
import mysql.connector
from mysql.connector import Error
import sys
from HE_database_connect import get_connection
from HE_error_logs import log_error_to_db


EMAIL_SENDER = 'dhineshapihitman@gmail.com'
EMAIL_PASSWORD = "yiof ntnc xowc gpbp"
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587

OTP_LENGTH = 6


def generate_otp(length=OTP_LENGTH):
    return ''.join(random.choices('0123456789', k=length))

def send_email_otp(recipient_email, otp_code, created_by):
    subject = "Your One-Time Password (OTP)"
    body = f"""
Your OTP code is: {otp_code}
It will expire in 5 minutes.
"""
    email_text = f"Subject: {subject}\n\n{body}"

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, recipient_email, email_text)
        print(f"✅ OTP sent to {recipient_email}")
    except Exception as e:
        error_message = f"Failed to send email to {recipient_email}: {e}"
        print(f"❌ {error_message}")
        log_error_to_db(error_message, type(e).__name__, "otp_generator.py", created_by)
        exit(1)

def store_or_update_otp(email, otp_code, created_by):
    connection = None
    try:
        connection = get_connection()
        if connection.is_connected():
            cursor = connection.cursor()

            cursor.execute("SELECT id FROM he_otp_data WHERE email = %s", (email,))
            row = cursor.fetchone()

            if row:
                otp_id = row[0]
                cursor.execute("""
                    UPDATE he_otp_data
                    SET otp = %s, updated_by = %s, updated_at = NOW()
                    WHERE id = %s
                """, (otp_code, created_by, otp_id))
            else:
                cursor.execute("""
                    INSERT INTO he_otp_data (email, otp, created_by, created_at)
                    VALUES (%s, %s, %s, NOW())
                """, (email, otp_code, created_by))

            connection.commit()
            cursor.close()
            print("✅ OTP stored/updated in DB")

    except Error as e:
        error_message = f"MySQL Error for email {email}: {e}"
        print(f"❌ {error_message}")
        log_error_to_db(error_message, type(e).__name__, "otp_generator.py", created_by)
        exit(1)
    finally:
        if connection is not None and connection.is_connected():
            connection.close()

def main():
    if len(sys.argv) < 3:
        print("Usage: python otp_generator.py recipient_email created_by_user_id")
        exit(1)

    recipient_email = sys.argv[1]
    try:
        created_by = int(sys.argv[2])  # Convert created_by to int
    except ValueError as e:
        error_message = f"Invalid created_by_user_id {sys.argv[2]}: {e}"
        print(f"❌ {error_message}")
        log_error_to_db(error_message, type(e).__name__, "otp_generator.py", 1)
        exit(1)

    otp = generate_otp()
    send_email_otp(recipient_email, otp, created_by)
    store_or_update_otp(recipient_email, otp, created_by)

if __name__ == "__main__":
    main()