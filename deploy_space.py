from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from huggingface_hub import HfApi


def main() -> None:
    load_dotenv()
    token = os.environ.get("HF_TOKEN", "").strip()
    if not token:
        raise RuntimeError("HF_TOKEN is missing. Add it to .env or environment variables.")

    space_id = os.environ.get("HF_SPACE_ID", "hitanshjain1812/SqlBenchmarking").strip()
    repo_root = Path(__file__).resolve().parent

    api = HfApi(token=token)
    api.create_repo(repo_id=space_id, repo_type="space", space_sdk="docker", exist_ok=True)

    api.upload_folder(
        repo_id=space_id,
        repo_type="space",
        folder_path=str(repo_root),
        ignore_patterns=[".git/*", "__pycache__/*", ".env", ".venv/*", ".pytest_cache/*"],
    )

    print(f"Uploaded successfully to https://huggingface.co/spaces/{space_id}")


if __name__ == "__main__":
    main()
