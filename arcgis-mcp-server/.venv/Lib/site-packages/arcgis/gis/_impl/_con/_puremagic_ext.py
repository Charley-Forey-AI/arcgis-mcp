from __future__ import annotations
import requests
from io import BytesIO
import puremagic


def find_puremagic_ext(path):
    """
    Validate the file extension returned by puremagic.
    Try to go through and see if either jpeg, png, or gif.
    Accepts raw bytes or a file path/URL.
    """
    # If input is a string, treat as path or URL and fetch bytes
    if isinstance(path, str):
        if path.startswith("http://") or path.startswith("https://"):
            with requests.Session() as session:
                data = session.get(path).content
        else:
            with open(path, "rb") as f:
                data = f.read()
    else:
        # Assume already bytes
        data = path

    b = BytesIO(data)
    stream = puremagic.magic_stream(b)

    for s in stream:
        if s.extension in [".jpeg", ".png", ".gif", "avif"]:
            return s.extension.replace(".", "")
    return None
