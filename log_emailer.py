import os
import glob
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def get_most_recent_status(log_file):
    with open(log_file, 'r') as file:
        lines = file.readlines()
    # Assuming the status is on the last line of the log file
    most_recent_status = lines[-1].strip() if lines else "No status found in the log"
    return most_recent_status

def search_logs_and_report(folder_path):
    log_files = glob.glob(os.path.join(folder_path, '*.log'))
    if not log_files:
        print("No log files found in the specified folder.")
        return

    report = ""
    for log_file in log_files:
        most_recent_status = get_most_recent_status(log_file)
        report += f"Log file: {log_file}\n"
        report += f"Most recent status: {most_recent_status}\n"
        report += "-" * 30 + "\n"

    return report

def send_email(email_from, email_to, password, subject, body):
    msg = MIMEMultipart()
    msg['From'] = email_from
    msg['To'] = email_to
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    # Use Gmail's SMTP server
    server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
    server.login(email_from, password)
    server.sendmail(email_from, email_to, msg.as_string())
    server.quit()
    print("Done Sending Email!")

if __name__ == "__main__":
    folder_path = "/home/azureuser/"  # Replace with the path to your log folder
    email_from = "kinetixopensprocessing@gmail.com"  # Replace with your Gmail email address
    email_to = "awhelan@kinetixhr.com"  # Replace with the recipient's email address
    email_password = "ttljtrsnsqlhmnrz"  # Replace with your Gmail app password
    email_subject = "Log Report for today!"

    log_report = search_logs_and_report(folder_path)

    # Send the report via email
    send_email(email_from, email_to, email_password, email_subject, log_report)