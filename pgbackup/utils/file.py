import gzip
import os
import tempfile
from pathlib import Path
from shutil import copyfileobj
from tempfile import SpooledTemporaryFile
from typing import IO, AnyStr, Optional, Tuple, Union

import gnupg

from pgbackup.exceptions import DecryptionError, EncryptionError
from pgbackup.settings import settings

BYTES = (
    ("PiB", 1125899906842624.0),
    ("TiB", 1099511627776.0),
    ("GiB", 1073741824.0),
    ("MiB", 1048576.0),
    ("KiB", 1024.0),
    ("B", 1.0),
)


def setup_tmp_dir(func):
    """decorator to create and clean temporary directory"""

    def inner(*args, **kwargs):
        settings.TMP_DIR.mkdir(parents=True, exist_ok=True)
        func(*args, **kwargs)
        settings.TMP_DIR.rmdir()

    return inner


def create_spooled_temporary_file(
    filepath: Optional[Path] = None, fileobj: Optional[Union[IO, gzip.GzipFile]] = None
) -> SpooledTemporaryFile:
    """
    Create a spooled temporary file. if ``filepath`` or ``fileobj`` is
    defined its content will be copied into temporary file.
    """
    spooled_file = tempfile.SpooledTemporaryFile(
        max_size=settings.TMP_FILE_MAX_SIZE, dir=settings.TMP_DIR
    )
    if filepath:
        fileobj = open(filepath, "r+b")
    if fileobj is not None:
        fileobj.seek(0)
        copyfileobj(fileobj, spooled_file, settings.TMP_FILE_READ_SIZE)
    return spooled_file


def bytes_to_str(byte_value: Union[int, float], decimals: int = 1) -> str:
    """
    Convert bytes to a human readable string.
    """
    for unit, byte in BYTES:
        if byte_value >= byte:
            if decimals == 0:
                return "%s %s" % (int(round(byte_value / byte, 0)), unit)
            return "%s %s" % (round(byte_value / byte, decimals), unit)
    return "%s B" % byte_value


def handle_size(filehandle: IO) -> str:
    """
    Get file's size to a human readable string.
    """
    filehandle.seek(0, 2)
    return bytes_to_str(filehandle.tell())


def write_local_file(outputfile: SpooledTemporaryFile, filepath: Path) -> Path:
    """Write file to the desired path."""
    outputfile.seek(0)
    with open(filepath, "wb") as fd:
        copyfileobj(outputfile, fd)
    return filepath


@setup_tmp_dir
def compress_file(
    inputfile: SpooledTemporaryFile, filename: str
) -> Tuple[SpooledTemporaryFile, str]:
    """
    Compress input file using gzip and change its name.
    """
    outputfile = create_spooled_temporary_file()
    new_filename = f"{filename}.gz"
    zipfile = gzip.GzipFile(filename=filename, fileobj=outputfile, mode="wb")
    try:
        inputfile.seek(0)
        copyfileobj(inputfile, zipfile, settings.TMP_FILE_READ_SIZE)
    finally:
        zipfile.close()
    return outputfile, new_filename


@setup_tmp_dir
def uncompress_file(inputfile: IO, filename: str) -> Tuple[SpooledTemporaryFile, str]:
    """
    Uncompress this file using gzip and change its name.
    """
    zipfile = gzip.GzipFile(fileobj=inputfile, mode="rb")
    try:
        outputfile = create_spooled_temporary_file(fileobj=zipfile)
    finally:
        zipfile.close()
    new_basename = filename.replace(".gz", "")
    return outputfile, new_basename


@setup_tmp_dir
def encrypt_file(inputfile: Union[AnyStr, IO]) -> Tuple[SpooledTemporaryFile, str]:
    """
    Encrypt input file using GPG and and .gpg extension to its name.
    """

    filename = f"{inputfile.name}.gpg"
    filepath: Path = settings.TMP_DIR.joinpath(filename)
    try:
        inputfile.seek(0)
        g = gnupg.GPG()
        result = g.encrypt_file(
            inputfile,
            output=filepath,
            recipients=settings.gpg_recipient,
            always_trust=settings.gpg_always_trust,
        )
        inputfile.close()
        if not result:
            raise EncryptionError(f"Encryption failed; status: result.status")
        return create_spooled_temporary_file(filepath), filename
    finally:
        if filepath.exists():
            filepath.unlink()


@setup_tmp_dir
def unencrypt_file(
    inputfile: Union[AnyStr, IO], passphrase: Optional[str] = None
) -> Tuple[SpooledTemporaryFile, str]:
    """
    Unencrypt input file using GPG and remove .gpg extension to its name.
    """
    new_basename = inputfile.name.replace(".gpg", "")
    temp_filename = settings.TMP_DIR.joinpath(new_basename)
    try:
        inputfile.seek(0)
        g = gnupg.GPG()
        result = g.decrypt_file(
            file=inputfile, passphrase=passphrase, output=temp_filename
        )
        if not result:
            raise DecryptionError("Decryption failed; status: %s" % result.status)
        outputfile = create_spooled_temporary_file(temp_filename)
    finally:
        if temp_filename.exists():
            os.remove(temp_filename)

    return outputfile, new_basename
