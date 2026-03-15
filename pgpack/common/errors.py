class PGPackError(Exception):
    """Base PGPack error."""


class PGPackHeaderError(PGPackError, ValueError):
    """Error header signature."""


class PGPackMetadataCrcError(PGPackError, ValueError):
    """Error metadata crc32."""


class PGPackModeError(PGPackError, ValueError):
    """Error fileobject mode."""
