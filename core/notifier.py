import smtplib
from email.message import EmailMessage
from typing import List

class DagNotifier:

    def __init__(self, username: str, password: str):

        self.username = username
        self.password = password

    def email_notification(

            self,
            subject: str,
            content: str,
            to_address: List[str]

    ):

        for r in to_address:

            msg = EmailMessage()

            msg['Subject'] = subject ; msg['From'] = self.username ; msg['To'] = r

            msg.set_content(content)

            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
                smtp.login(
                    self.username, self.password
                )
                smtp.send_message(msg)