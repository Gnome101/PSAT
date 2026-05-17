from pathlib import Path


def test_backend_image_includes_runtime_data() -> None:
    dockerfile = Path("Dockerfile").read_text(encoding="utf-8")

    assert "COPY data/ data/" in dockerfile
