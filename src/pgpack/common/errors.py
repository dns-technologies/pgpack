class PGPackError(Exception):
    """Base PGPack error."""


class PGPackHeaderError(PGPackError, ValueError):
    """Error header signature."""


class PGPackMetadataCrcError(PGPackError, ValueError):
    """Error metadata crc32."""


class PGPackModeError(PGPackError, ValueError):
    """Error fileobject mode."""


class PGPackNotDefineError(PGPackError, ValueError):
    """Fileobject not define."""


class PGPackTypeError(PGPackError, TypeError):
    """PGPack type error."""
