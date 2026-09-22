import os

os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_SECURITY_TOKEN", "testing")
os.environ.setdefault("AWS_SESSION_TOKEN", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "eu-west-2")

# s3io.py creates its boto3 S3 client at import time (module-level `s3 =
# boto3.client("s3")`). moto only intercepts calls made through a client
# that was itself created while a mock_aws() context was active, so we
# force that first import to happen under a throwaway mock context here.
# Individual tests then open their own mock_aws() context per test, which
# this already-mock-bound client correctly routes through.
#
# s3io ends up imported under two different module names depending on
# whether the importer uses the package-style path (tests, e.g. `import
# app.s3io`) or the bare path app/*.py itself uses internally (e.g.
# fetch_sneeze_data.py's `from s3io import _read_sneeze_data`, which only
# resolves because "app" is also on pythonpath) — each creates its own
# separate boto3 client, so both need to be warmed here.
from moto import mock_aws as _mock_aws

with _mock_aws():
    import s3io  # noqa: F401
    import app.s3io  # noqa: F401  
