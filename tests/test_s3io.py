import boto3
import pandas as pd
import pytest
from moto import mock_aws

import app.s3io as s3io

BUCKET = "test-sneeze-bucket"


@pytest.fixture
def s3_bucket():
    with mock_aws():
        client = boto3.client("s3", region_name="eu-west-2")
        client.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield client


def test_read_sneeze_data_returns_empty_df_when_no_object(s3_bucket):
    df = s3io._read_sneeze_data(BUCKET)
    assert df.empty
    assert list(df.columns) == s3io.SNEEZE_COLUMNS


def test_append_sneeze_data_writes_and_appends(s3_bucket):
    first = pd.DataFrame([{col: 1 for col in s3io.SNEEZE_COLUMNS}])
    s3io.append_sneeze_data(BUCKET, first)

    second = pd.DataFrame([{col: 2 for col in s3io.SNEEZE_COLUMNS}])
    s3io.append_sneeze_data(BUCKET, second)

    stored = s3io._read_sneeze_data(BUCKET)
    assert len(stored) == 2


def test_dedupe_sneeze_data_removes_duplicates(s3_bucket):
    rows = [{col: 1 for col in s3io.SNEEZE_COLUMNS} for _ in range(3)]
    df = pd.DataFrame(rows)
    df["Date"] = "2024-01-01"
    df["Time"] = "10:00:00"
    s3io.append_sneeze_data(BUCKET, df)

    s3io.dedupe_sneeze_data(BUCKET)

    stored = s3io._read_sneeze_data(BUCKET)
    assert len(stored) == 1


# Bug 10: _ensure_bucket silently swallowed a missing bucket and was never
# wired into the write path, so a typo'd bucket name would surface later as
# a confusing raw S3 error instead of a clear one.
def test_append_sneeze_data_raises_clearly_when_bucket_missing():
    with mock_aws():
        with pytest.raises(RuntimeError, match="not found"):
            s3io.append_sneeze_data(
                "nonexistent-bucket", pd.DataFrame(columns=s3io.SNEEZE_COLUMNS)
            )
