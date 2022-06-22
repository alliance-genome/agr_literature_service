import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def send_email(subject, recipients, msg, sender_email, reply_to):

    try:
        message = MIMEMultipart("alternative")
        message["Subject"] = subject
        message["From"] = sender_email
        message.add_header('reply-to', reply_to)
        html_message = MIMEText(msg, "html")
        message.attach(html_message)

        server = smtplib.SMTP("localhost", 25)
        any_recipients_error = server.sendmail(sender_email, recipients, message.as_string())
        server.quit()

        if(len(any_recipients_error) > 0):
            error_message = ''
            for key in any_recipients_error:
                error_message = error_message + ' ' + key + ' ' + str(any_recipients_error[key]) + ' ;' + '\n'

            error_message = "Email sending unsuccessful for this recipients " + error_message
            return ("error", error_message)

        return ("success", "Email was successfully sent.")

    except smtplib.SMTPHeloError as e:
        return ("error", "The server didn't reply properly to the hello greeting. " + str(e))
    except smtplib.SMTPRecipientsRefused as e:
        return ("error", "The server rejected ALL recipients (no mail was sent). " + str(e))
    except smtplib.SMTPSenderRefused as e:
        return ("error", "The server didn't accept the sender's email. " + str(e))
    except smtplib.SMTPDataError as e:
        return ("error", "The server replied with an unexpected error. " + str(e))
    except Exception as e:
        return ("error", "Error occured while sending email. " + str(e))
