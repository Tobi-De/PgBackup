class ConnectorError(Exception):
    """Base connector error"""


class DumpError(ConnectorError):
    """Error on dump"""


class RestoreError(ConnectorError):
    """Error on restore"""


class CommandConnectorError(ConnectorError):
    """Failing command"""


class EncryptionError(Exception):
    pass


class DecryptionError(Exception):
    pass
