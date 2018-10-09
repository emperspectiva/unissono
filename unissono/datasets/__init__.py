import os
import logging
import urllib.request
import zipfile
from urllib.parse import urlparse
from pathlib import Path
from tqdm import tqdm

DATASETS_DIR = Path.home() / ".unissono/data/"

logger = logging.getLogger(__name__)


def download(url, dest):
    """ Download given url to dest.
    """
    dest.mkdir(parents=True, exist_ok=True)
    fname = os.path.basename(urlparse(url).path)
    fname_abs = dest / fname

    if fname_abs.exists():
        logger.info("File already exists: %s" % (fname_abs))
        return fname_abs

    with fname_abs.open("wb") as o:
        with urllib.request.urlopen(url) as f:
            content_length = int(f.info()["Content-length"])
            with tqdm(total=content_length) as pbar:
                while True:
                    buf = f.read(16*1024)
                    if not buf:
                        break
                    o.write(buf)
                    pbar.update(len(buf))

    return fname_abs


def extract_zip(fname, dest):
    zip = zipfile.ZipFile(fname, "r")
    zip.extractall(dest)
    zip.close()
