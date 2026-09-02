# Install the native ADBC drivers declared in dbc.toml/dbc.lock (currently just
# `bigquery`, used for Arrow BATCH ingestion -- see docs.adbc-drivers.org/drivers/bigquery/).
dbc:
    uvx dbc sync

test *args:
    uv run pytest {{args}}
