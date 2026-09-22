import json
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pytest

import app.fetch_sneeze_data as fsd

def test_lines_to_rows_defaults_location_when_omitted():
    df = fsd._lines_to_rows(["01/06/2024,10:15:00"])
    assert len(df) == 1
    assert df.iloc[0]["Latitude"] == round(fsd.LAT_DEFAULT, 4)
    assert df.iloc[0]["Longitude"] == round(fsd.LONG_DEFAULT, 4)


def test_lines_to_rows_parses_explicit_coordinates_and_strips_tz_suffix():
    df = fsd._lines_to_rows(["01/06/2024,10:15:00 BST,51.5,-0.1"])
    assert df.iloc[0]["Latitude"] == 51.5
    assert df.iloc[0]["Longitude"] == -0.1


def test_lines_to_rows_skips_blank_and_unparseable_lines():
    df = fsd._lines_to_rows(["", "garbage,line", "01/06/2024,10:15:00"])
    assert len(df) == 1


# ---------------------------------------------------------------------------
# _fetch_sneeze_email_bodies — fake imaplib, no real network/Gmail
# ---------------------------------------------------------------------------

def _multipart_email_bytes(body_texts):
    msg = MIMEMultipart()
    for text in body_texts:
        msg.attach(MIMEText(text, "plain"))
    return msg.as_bytes()


class FakeIMAP:
    select_should_fail = False

    def __init__(self, *args, **kwargs):
        self.closed = False
        self.logged_out = False

    def login(self, username, password):
        pass

    def select(self, mailbox):
        if self.select_should_fail:
            raise fsd.imaplib.IMAP4.error("boom")

    def search(self, charset, criteria):
        return "OK", [b"1"]

    def fetch(self, msg_id, parts):
        raw = _multipart_email_bytes(["01/06/2024,10:00:00", "-- \nsignature block"])
        return "OK", [(b"1 (RFC822 {123}", raw)]

    def close(self):
        self.closed = True

    def logout(self):
        self.logged_out = True


# Bug 1: a multipart email with more than one text/plain part (e.g. a
# signature block) was appended to email_bodies once per part instead of
# once per email, doubling every sneeze record from that email.
def test_fetch_sneeze_email_bodies_does_not_duplicate_multipart_email(monkeypatch):
    monkeypatch.setattr(fsd.imaplib, "IMAP4_SSL", FakeIMAP)
    bodies = fsd._fetch_sneeze_email_bodies("user@example.com", "pw")
    assert len(bodies) == 1


# Bug 4: mail.close() is illegal in IMAP AUTHENTICATED state. If select()
# raises before the mailbox is selected, calling close() in the finally
# block raised its own error and masked the original exception.
def test_fetch_sneeze_email_bodies_does_not_mask_original_exception_on_select_failure(
    monkeypatch,
):
    instances = []

    class FailingSelectIMAP(FakeIMAP):
        select_should_fail = True

    def factory(*args, **kwargs):
        inst = FailingSelectIMAP(*args, **kwargs)
        instances.append(inst)
        return inst

    monkeypatch.setattr(fsd.imaplib, "IMAP4_SSL", factory)

    with pytest.raises(fsd.imaplib.IMAP4.error, match="boom"):
        fsd._fetch_sneeze_email_bodies("user@example.com", "pw")

    assert instances[0].closed is False
    assert instances[0].logged_out is True


# ---------------------------------------------------------------------------
# _get_gmail_credentials_from_secrets — fake secretsmanager client
# ---------------------------------------------------------------------------

class FakeSecretsClient:
    def __init__(self, secret_dict):
        self.secret_dict = secret_dict

    def get_secret_value(self, SecretId):
        return {"SecretString": json.dumps(self.secret_dict)}


# Bug 7: if the Secrets Manager JSON used different key names, .get() calls
# silently returned (None, None) with no error, surfacing only as a
# confusing later IMAP auth failure.
def test_get_gmail_credentials_raises_when_keys_missing(monkeypatch):
    monkeypatch.setenv("GMAIL_SECRET_ARN", "arn:aws:secretsmanager:eu-west-2:123:secret:x")
    monkeypatch.setattr(
        fsd.boto3,
        "client",
        lambda *a, **kw: FakeSecretsClient({"email": "a@b.com", "password": "x"}),
    )
    with pytest.raises(RuntimeError):
        fsd._get_gmail_credentials_from_secrets()


def test_get_gmail_credentials_returns_values_when_present(monkeypatch):
    monkeypatch.setenv("GMAIL_SECRET_ARN", "arn:aws:secretsmanager:eu-west-2:123:secret:x")
    monkeypatch.setattr(
        fsd.boto3,
        "client",
        lambda *a, **kw: FakeSecretsClient({"username": "a@b.com", "app_password": "x"}),
    )
    username, password = fsd._get_gmail_credentials_from_secrets()
    assert username == "a@b.com"
    assert password == "x"
