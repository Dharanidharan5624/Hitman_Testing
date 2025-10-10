
import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_welcome_email(user_email, user_name):
    sender_email ="kgaba.dataservices@gmail.com"
    password = "qouj rgbd nizh zljo" # App Password (Use your actual app password here)

    subject = "Welcome to Our Application!"
    body = f"""
Hi {user_name},

Welcome to the Hitman community! We're excited to have you join us.
This email confirms your registration and gives you access to everything Hitman has to offer.

If you have any questions or need assistance, please reply to kgaba.dataservices@gmail.com 

Thanks,
Hitman Team
"""

    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = user_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(sender_email, password)
        server.sendmail(sender_email, user_email, msg.as_string())
        print(f"✅ Welcome email sent to {user_email}")
    except Exception as e:
        print("❌ Error:", e)
    finally:
        server.quit()

if __name__ == "__main__":
    if len(sys.argv) >= 3:
        email = sys.argv[1]
        user_name = " ".join(sys.argv[2:])  # Handles full names with spaces
        send_welcome_email(email, user_name)
        print("Welcome email function executed.")
        print(f"Email: {email}")
        print(f"User Name: {user_name}")
    else:
        print("Usage: python send_mail.py <email> <user_name>")
