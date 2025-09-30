import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_welcome_email(user_email, user_name):
    sender_email = "dhineshapihitman@gmail.com"
    password ="yiof ntnc xowc gpbp"  # Gmail App Password

    # === Email Setup ===
    subject = "Welcome Hitman !"
    body = f"""
    Hi {user_name},

    🎉 Welcome to Hitman application! 
    We're excited to have you on board.

    If you have any questions, feel free to reach out to our support team.

    Best regards,  
    The Team
    """

    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = user_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    try:
        # Connect to Gmail SMTP
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(sender_email, password)

        # Send mail
        server.sendmail(sender_email, user_email, msg.as_string())
        print(f"✅ Welcome email sent to {user_email}")

    except Exception as e:
        print("❌ Error:", e)

    finally:
        server.quit()


# === Example: When a new user registers ===
new_user_email = "dhineshapihitman@shravtek.com"
new_user_name = "dharanidharan"

send_welcome_email(new_user_email, new_user_name)


# import sys
# import smtplib
# from email.mime.text import MIMEText
# from email.mime.multipart import MIMEMultipart

# def send_welcome_email(user_email, user_name):
#     sender_email = "dhineshapihitman@gmail.com"
#     password = "yiof ntnc xowc gpbp"   # 🔑 Gmail App Password (not real password)

#     subject = "Welcome to Our Application!"
#     body = f"""
#     Hi {user_name},

#     🎉 Welcome to Hitman!
#     We're excited to have you on board.

#     If you have any questions, feel free to reach out to our support team.

#     Regards,
#     The Team
#     """

#     msg = MIMEMultipart()
#     msg["From"] = sender_email
#     msg["To"] = user_email
#     msg["Subject"] = subject
#     msg.attach(MIMEText(body, "plain"))

#     try:
#         server = smtplib.SMTP("smtp.gmail.com", 587)
#         server.starttls()
#         server.login(sender_email, password)
#         server.sendmail(sender_email, user_email, msg.as_string())
#         print(f"✅ Welcome email sent to {user_email}")
#     except Exception as e:
#         print("❌ Error:", e)
#     finally:
#         server.quit()

# if __name__ == "__main__":
#     if len(sys.argv) >= 3:
#         email = sys.argv[1]
#         name = sys.argv[2]
#         send_welcome_email(email, name)
#     else:
#         print("Usage: python send_mail.py <email> <name>")
