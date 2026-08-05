# src/verdanta/extract/adls.py
from pathlib import Path
from urllib.parse import urlparse

from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient


def upload(local: Path, target_url: str) -> None:
    # abfss://<container>@<account>.dfs.core.windows.net/<path>
    parsed = urlparse(target_url)
    container, account_host = parsed.netloc.split("@")
    file_path = parsed.path.lstrip("/")

    client = DataLakeServiceClient(
        account_url=f"https://{account_host}",
        credential=DefaultAzureCredential(),
    )
    fs = client.get_file_system_client(container)
    with local.open("rb") as fh:
        fs.get_file_client(file_path).upload_data(fh, overwrite=True)