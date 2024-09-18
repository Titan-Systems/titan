"Adapted from https://github.com/snowflakedb/snowflake-cli/blob/main/src/snowflake/cli/app/snow_connector.py"

# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import errno
import logging
import os
import shutil
import tempfile

from contextlib import contextmanager, redirect_stdout, redirect_stderr
from pathlib import Path
from typing import Optional, Union, Dict

import snowflake.connector

from click.exceptions import ClickException
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import DatabaseError, ForbiddenError

log = logging.getLogger()

DEFAULT_SIZE_LIMIT_MB = 128
UNLIMITED = -1
ENCRYPTED_PKCS8_PK_HEADER = b"-----BEGIN ENCRYPTED PRIVATE KEY-----"
UNENCRYPTED_PKCS8_PK_HEADER = b"-----BEGIN PRIVATE KEY-----"


class InvalidConnectionConfiguration(ClickException):
    def format_message(self):
        return f"Invalid connection configuration. {self.message}"


class SnowflakeConnectionError(ClickException):
    def __init__(self, snowflake_err: Exception):
        super().__init__(f"Could not connect to Snowflake. Reason: {snowflake_err}")


class DirectoryIsNotEmptyError(ClickException):
    def __init__(self, path: Path):
        super().__init__(f"Directory '{path}' is not empty")


class FileTooLargeError(ClickException):
    def __init__(self, path: Path, size_limit_in_kb: int):
        super().__init__(f"File {path} is too large (size limit: {size_limit_in_kb} KB)")


def connect(
    mfa_passcode: Optional[str] = None,
    enable_diag: Optional[bool] = False,
    diag_log_path: Optional[str] = None,
    diag_allowlist_path: Optional[str] = None,
    **kwargs,
) -> SnowflakeConnection:

    using_session_token = "session_token" in kwargs and kwargs["session_token"] is not None
    using_master_token = "master_token" in kwargs and kwargs["master_token"] is not None
    _raise_errors_related_to_session_token(using_session_token, using_master_token)

    connection_parameters = get_env_vars()

    # Apply kwargs to connection details
    for key, value in kwargs.items():
        # Command line override case
        if value:
            connection_parameters[key] = value
            continue

        # Generic environment variable case, apply only if value not passed via flag or connection variable
        generic_env_value = os.environ.get(f"SNOWFLAKE_{key}".upper())
        if key not in connection_parameters and generic_env_value:
            connection_parameters[key] = generic_env_value
            continue

    # Clean up connection params
    connection_parameters = {k: v for k, v in connection_parameters.items() if v is not None}

    connection_parameters = _update_connection_details_with_private_key(connection_parameters)

    if mfa_passcode:
        connection_parameters["passcode"] = mfa_passcode

    if connection_parameters.get("authenticator") == "username_password_mfa":
        connection_parameters["client_request_mfa_token"] = True

    if enable_diag:
        connection_parameters["enable_connection_diag"] = enable_diag
        if diag_log_path:
            connection_parameters["connection_diag_log_path"] = diag_log_path
        if diag_allowlist_path:
            connection_parameters["connection_diag_allowlist_path"] = diag_allowlist_path

    # Make sure the connection is not closed if it was shared to the SnowCLI, instead of being created in the SnowCLI
    _avoid_closing_the_connection_if_it_was_shared(using_session_token, using_master_token, connection_parameters)

    _update_connection_application_name(connection_parameters)

    try:
        # Whatever output is generated when creating connection,
        # we don't want it in our output. This is particularly important
        # for cases when external browser and json format are used.
        # Redirecting both stdout and stderr for offline usage.
        with redirect_stdout(None), redirect_stderr(None):
            return snowflake.connector.connect(
                **connection_parameters,
            )
    except ForbiddenError as err:
        raise SnowflakeConnectionError(err)
    except DatabaseError as err:
        raise InvalidConnectionConfiguration(err.msg or "")


class SecurePath:
    def __init__(self, path: Union[Path, str]):
        self._path = Path(path)

    def __repr__(self):
        return f'SecurePath("{self._path}")'

    def __truediv__(self, key):
        return SecurePath(self._path / key)

    @property
    def path(self) -> Path:
        """
        Returns itself in pathlib.Path format
        """
        return self._path

    @property
    def parent(self):
        """
        The logical parent of the path. For details, check pathlib.Path.parent
        """
        return SecurePath(self._path.parent)

    def absolute(self):
        """
        Make the path absolute, without normalization or resolving symlinks.
        """
        return SecurePath(self._path.absolute())

    def iterdir(self):
        """
        When the path points to a directory, yield path objects of the directory contents.
        Otherwise, NotADirectoryError is raised.
        If the location does not exist, FileNotFoundError is raised.

        For details, check pathlib.Path.iterdir()
        """
        self.assert_exists()
        self.assert_is_directory()
        return (SecurePath(p) for p in self._path.iterdir())

    def exists(self) -> bool:
        """
        Return True if the path points to an existing file or directory.
        """
        return self._path.exists()

    def is_dir(self) -> bool:
        """
        Return True if the path points to a directory (or a symbolic link pointing to a directory),
        False if it points to another kind of file.
        """
        return self._path.is_dir()

    def is_file(self) -> bool:
        """
        Return True if the path points to a regular file (or a symbolic link pointing to a regular file),
        False if it points to another kind of file.
        """
        return self._path.is_file()

    @property
    def name(self) -> str:
        """A string representing the final path component."""
        return self._path.name

    def chmod(self, permissions_mask: int) -> None:
        """
        Change the file mode and permissions, like os.chmod().
        """
        log.info("Update permissions of file %s to %s", self._path, oct(permissions_mask))
        self._path.chmod(permissions_mask)

    def restrict_permissions(self) -> None:
        """
        Restrict file/directory permissions to owner-only.
        """
        import stat

        owner_permissions = (
            # https://docs.python.org/3/library/stat
            stat.S_IRUSR  # readable by owner
            | stat.S_IWUSR  # writeable by owner
            | stat.S_IXUSR  # executable by owner
        )
        self.chmod(self._path.stat().st_mode & owner_permissions)

    def touch(self, permissions_mask: int = 0o600, exist_ok: bool = True) -> None:
        """
        Create a file at this given path. For details, check pathlib.Path.touch()
        """
        if not self.exists():
            log.info("Creating file %s", str(self._path))
        self._path.touch(mode=permissions_mask, exist_ok=exist_ok)

    def mkdir(
        self,
        permissions_mask: int = 0o700,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        """
        Create a directory at this given path. For details, check pathlib.Path.mkdir()
        """
        if parents and not self.parent.exists():
            self.parent.mkdir(permissions_mask=permissions_mask, exist_ok=exist_ok, parents=True)
        if not self.exists():
            log.info("Creating directory %s", str(self._path))
        self._path.mkdir(mode=permissions_mask, exist_ok=exist_ok)

    def read_text(self, file_size_limit_mb: int, *args, **kwargs) -> str:
        """
        Return the decoded contents of the file as a string.
        Raises an error of the file exceeds the specified size limit.
        For details, check pathlib.Path.read_text()
        """
        self._assert_exists_and_is_file()
        self._assert_file_size_limit(file_size_limit_mb)
        log.info("Reading file %s", self._path)
        return self._path.read_text(*args, **kwargs)

    def write_text(self, *args, **kwargs):
        """
        Open the file pointed to in text mode, write data to it, and close the file.
        """
        if not self.exists():
            self.touch()
        log.info("Writing to file %s", self._path)
        self.path.write_text(*args, **kwargs)

    @contextmanager
    def open(  # noqa: A003
        self,
        mode="r",
        read_file_limit_mb: Optional[int] = None,
        **open_kwargs,
    ):
        """
        Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        If the file is opened for reading, [read_file_limit_kb] parameter must be provided.
        Raises error if the read file exceeds the specified size limit.
        """
        opened_for_reading = "r" in mode
        if opened_for_reading:
            assert read_file_limit_mb is not None, "For reading mode ('r') read_file_limit_mb must be provided"
            self._assert_exists_and_is_file()
            self._assert_file_size_limit(read_file_limit_mb)

        if self.exists():
            self.assert_is_file()
        else:
            self.touch()  # makes sure permissions of freshly-created file are strict

        log.info("Opening file %s in mode '%s'", self._path, mode)
        with self._path.open(mode=mode, **open_kwargs) as fd:
            yield fd
        log.info("Closing file %s", self._path)

    def move(self, destination: Union[Path, str]) -> "SecurePath":
        """Recursively move a file or directory (src) to another location and return the destination.

        If dst is an existing directory or a symlink to a directory, then src is moved inside that directory.
        The destination path in that directory must not already exist.
        """
        destination = Path(destination)
        if destination.is_dir():
            destination = destination / self._path.name
        if destination.exists():
            _raise_file_exists_error(destination)
        log.info("Moving %s to %s", str(self._path), destination.resolve())
        return SecurePath(shutil.move(str(self._path), destination))

    def copy(self, destination: Union[Path, str], dirs_exist_ok: bool = False) -> "SecurePath":
        """
        Copy the file/directory into the destination.
        If source is a directory, its whole content is copied recursively.
        Permissions of the copy are limited only to the owner.

        If destination is an existing directory, the copy will be created inside it,
        unless dirs_exist_ok is true and the destination has the same name as this path.

        Otherwise, the copied file/base directory will be renamed to match destination.
        If dirs_exist_ok is false (the default) and dst already exists,
        a FileExistsError is raised. If dirs_exist_ok is true,
        the copying operation will continue if it encounters existing directories,
        and files within the destination tree will be overwritten by corresponding
        files from the src tree.
        """
        self.assert_exists()

        destination = Path(destination)
        if destination.exists():
            if destination.is_dir() and (destination.name != self._path.name or self.path.is_file()):
                destination = destination / self._path.name

            if destination.exists():
                if not all([destination.is_dir(), self._path.is_dir(), dirs_exist_ok]):
                    raise FileExistsError(errno.EEXIST, os.strerror(errno.EEXIST), self._path.resolve())

        def _recursive_check_for_conflicts(src: Path, dst: Path):
            if dst.exists() and not dirs_exist_ok:
                _raise_file_exists_error(dst)
            if dst.is_file() and not src.is_file():
                _raise_not_a_directory_error(dst)
            if dst.is_dir() and not src.is_dir():
                _raise_is_a_directory_error(dst)
            if src.is_dir():
                for child in src.iterdir():
                    _recursive_check_for_conflicts(child, dst / child.name)

        def _recursive_copy(src: SecurePath, dst: SecurePath):
            if src.is_file():
                log.info("Copying file %s into %s", src.path, dst.path)
                if dst.exists():
                    dst.unlink()
                shutil.copyfile(src.path, dst.path)
                dst.restrict_permissions()
            if src.is_dir():
                dst.mkdir(exist_ok=True)
                for child in src.iterdir():
                    _recursive_copy(child, dst / child.name)

        _recursive_check_for_conflicts(self._path, destination)
        _recursive_copy(self, self.__class__(destination))

        return SecurePath(destination)

    def unlink(self, missing_ok=False):
        """
        Remove this file or symbolic link.
        If the path points to a directory, use SecurePath.rmdir() instead.

        Check pathlib.Path.unlink() for details.
        """
        if not self.exists():
            if not missing_ok:
                self.assert_exists()
            return

        self.assert_is_file()
        log.info("Removing file %s", self._path)
        self._path.unlink()

    def rmdir(self, recursive=False, missing_ok=False):
        """
        Remove this directory.
        If the path points to a file, use SecurePath.unlink() instead.

        If path points to a file, NotADirectoryError will be raised.
        If directory does not exist, FileNotFoundError will be raised unless [missing_ok] is True.
        If the directory is not empty, DirectoryNotEmpty will be raised unless [recursive] is True.
        """
        if not self.exists():
            if not missing_ok:
                self.assert_exists()
            return

        self.assert_is_directory()

        if not recursive and any(self._path.iterdir()):
            raise DirectoryIsNotEmptyError(self._path.resolve())

        log.info("Removing directory %s", self._path)
        shutil.rmtree(str(self._path))

    @classmethod
    @contextmanager
    def temporary_directory(cls):
        """
        Creates a temporary directory in the most secure manner possible.
        The directory is readable, writable, and searchable only by the creating user ID.
        Yields SecurePath pointing to the absolute location of created directory.

        Works similarly to tempfile.TemporaryDirectory
        """
        with tempfile.TemporaryDirectory(prefix="snowflake-cli") as tmpdir:
            log.info("Created temporary directory %s", tmpdir)
            yield SecurePath(tmpdir)
            log.info("Removing temporary directory %s", tmpdir)

    def _assert_exists_and_is_file(self) -> None:
        self.assert_exists()
        self.assert_is_file()

    def assert_exists(self) -> None:
        if not self.exists():
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), self._path.resolve())

    def assert_is_file(self) -> None:
        if not self._path.is_file():
            _raise_is_a_directory_error(self._path.resolve())

    def assert_is_directory(self) -> None:
        if not self._path.is_dir():
            _raise_not_a_directory_error(self._path.resolve())

    def _assert_file_size_limit(self, size_limit_in_mb):
        if size_limit_in_mb != UNLIMITED and self._path.stat().st_size > size_limit_in_mb * 1024 * 1024:
            raise FileTooLargeError(self._path.resolve(), size_limit_in_mb)


def _raise_file_exists_error(path: Path):
    raise FileExistsError(errno.EEXIST, os.strerror(errno.EEXIST), path)


def _raise_is_a_directory_error(path: Path):
    raise IsADirectoryError(errno.EISDIR, os.strerror(errno.EISDIR), path)


def _raise_not_a_directory_error(path: Path):
    raise NotADirectoryError(errno.ENOTDIR, os.strerror(errno.ENOTDIR), path)


def _avoid_closing_the_connection_if_it_was_shared(
    using_session_token: bool, using_master_token: bool, connection_parameters: Dict
):
    if using_session_token and using_master_token:
        connection_parameters["server_session_keep_alive"] = True


def _raise_errors_related_to_session_token(using_session_token: bool, using_master_token: bool):
    if using_session_token and not using_master_token:
        raise ClickException("When using a session token, you must provide the corresponding master token")
    if using_master_token and not using_session_token:
        raise ClickException("When using a master token, you must provide the corresponding session token")


def _update_connection_application_name(connection_parameters: Dict):
    connection_application_params = {
        "application_name": "titan-core",
    }
    connection_parameters.update(connection_application_params)


def _update_connection_details_with_private_key(connection_parameters: Dict):
    if "private_key_path" in connection_parameters:
        if connection_parameters.get("authenticator") == "SNOWFLAKE_JWT":
            private_key = _load_pem_to_der(connection_parameters["private_key_path"])
            connection_parameters["private_key"] = private_key
            del connection_parameters["private_key_path"]
        else:
            raise ClickException("Private Key authentication requires authenticator set to SNOWFLAKE_JWT")
    return connection_parameters


def _load_pem_to_der(private_key_path: str) -> bytes:
    """
    Given a private key file path (in PEM format), decode key data into DER
    format
    """
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives.serialization import (
        Encoding,
        NoEncryption,
        PrivateFormat,
        load_pem_private_key,
    )

    with SecurePath(private_key_path).open("rb", read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB) as f:
        private_key_pem = f.read()

    private_key_passphrase = os.getenv("PRIVATE_KEY_PASSPHRASE", None)
    if private_key_pem.startswith(ENCRYPTED_PKCS8_PK_HEADER) and private_key_passphrase is None:
        raise ClickException(
            "Encrypted private key, you must provide the"
            "passphrase in the environment variable PRIVATE_KEY_PASSPHRASE"
        )

    if not private_key_pem.startswith(ENCRYPTED_PKCS8_PK_HEADER) and not private_key_pem.startswith(
        UNENCRYPTED_PKCS8_PK_HEADER
    ):
        raise ClickException("Private key provided is not in PKCS#8 format. Please use correct format.")

    if private_key_pem.startswith(UNENCRYPTED_PKCS8_PK_HEADER):
        private_key_passphrase = None

    private_key = load_pem_private_key(
        private_key_pem,
        (str.encode(private_key_passphrase) if private_key_passphrase is not None else private_key_passphrase),
        default_backend(),
    )

    return private_key.private_bytes(
        encoding=Encoding.DER,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )


def get_env_vars() -> dict:
    connection_params = {}
    for var in [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_MFA_PASSCODE",
        "SNOWFLAKE_AUTHENTICATOR",
        "SNOWFLAKE_PRIVATE_KEY_PATH",
        "SNOWFLAKE_PRIVATE_KEY_FILE_PWD",
    ]:
        value = os.getenv(var, None)
        if value:
            connection_params[var[10:].lower()] = value
    return connection_params
