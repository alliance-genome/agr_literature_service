import smtplib
from unittest.mock import MagicMock, patch

from agr_literature_service.lit_processing.utils.email_utils import send_email


def _args():
    return dict(
        subject="Subject",
        recipients="to@example.org",
        msg="<p>hello</p>",
        sender_email="from@example.org",
        sender_password="secret",
        reply_to="reply@example.org",
    )


class TestSendEmail:
    def test_success(self):
        server = MagicMock()
        server.send_message.return_value = {}  # no per-recipient errors
        with patch.object(smtplib, "SMTP_SSL", return_value=server) as mock_ssl:
            status, message = send_email(**_args())

        mock_ssl.assert_called_once_with("smtp.gmail.com", 465)
        server.login.assert_called_once_with("from@example.org", "secret")
        server.send_message.assert_called_once()
        server.quit.assert_called_once()
        assert status == "success"
        assert message == "Email was successfully sent."

    def test_partial_recipient_failure(self):
        server = MagicMock()
        server.send_message.return_value = {"bad@example.org": (550, b"no such user")}
        with patch.object(smtplib, "SMTP_SSL", return_value=server):
            status, message = send_email(**_args())

        assert status == "error"
        assert "bad@example.org" in message
        assert "Email sending unsuccessful" in message

    def test_recipients_refused_maps_to_error(self):
        server = MagicMock()
        server.send_message.side_effect = smtplib.SMTPRecipientsRefused({})
        with patch.object(smtplib, "SMTP_SSL", return_value=server):
            status, message = send_email(**_args())

        assert status == "error"
        assert "rejected ALL recipients" in message

    def test_generic_exception_maps_to_error(self):
        with patch.object(smtplib, "SMTP_SSL", side_effect=RuntimeError("boom")):
            status, message = send_email(**_args())

        assert status == "error"
        assert "Error occured while sending email" in message
        assert "boom" in message
