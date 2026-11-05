# Sneeze Pipeline

An AWS ECS pipeline that:
- Reads sneezing data from Gmail via IMAP
- Enriches with weather and air-quality data
- Stores daily updates in an S3 bucket (stored in M&C Saatchi Fluency's AWS account)

## Development

Requires [uv](https://docs.astral.sh/uv/) (`winget install astral-sh.uv`).

```bash
uv sync                      # install dependencies into .venv
uv run pytest                # run the tests
uv run python app/main.py    # run the pipeline locally (needs AWS credentials)
```

Dependencies live in `pyproject.toml` and are pinned in `uv.lock`. Use
`uv add <package>` (or `uv add --dev <package>` for test-only tools) rather
than pip, and commit both files.
