from setuptools import (
    Extension,
    setup,
)
from Cython.Build import cythonize

extensions = [
    Extension(
        "pgpack.pgcopylib.core.functions",
        ["src/pgpack/pgcopylib/core/functions.pyx"],
    ),
    Extension(
        "pgpack.pgcopylib.core.types.arrays",
        ["src/pgpack/pgcopylib/core/types/arrays.pyx"],
    ),
    Extension(
        "pgpack.pgcopylib.core.types.dates",
        ["src/pgpack/pgcopylib/core/types/dates.pyx"],
    ),
    Extension(
        "pgpack.pgcopylib.core.types.digits",
        ["src/pgpack/pgcopylib/core/types/digits.pyx"],
    ),
    Extension(
        "pgpack.pgcopylib.core.types.geometrics",
        ["src/pgpack/pgcopylib/core/types/geometrics.pyx"],
    ),
    Extension(
        "pgpack.pgcopylib.core.types.ipaddrs",
        ["src/pgpack/pgcopylib/core/types/ipaddrs.pyx"],
    ),
    Extension(
        "pgpack.pgcopylib.core.types.jsons",
        ["src/pgpack/pgcopylib/core/types/jsons.pyx"],
    ),
    Extension(
        "pgpack.pgcopylib.core.types.strings",
        ["src/pgpack/pgcopylib/core/types/strings.pyx"],
    ),
    Extension(
        "pgpack.pgcopylib.core.types.uuids",
        ["src/pgpack/pgcopylib/core/types/uuids.pyx"],
    ),
]

with open(file="README.md", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="pgpack",
    version="0.3.3.dev7",
    author="0xMihalich",
    author_email="bayanmobile87@gmail.com",
    description=(
        "PGCopy dump packed into GZIP, LZ4, SNAPPY, ZSTD or "
        "uncompressed with meta data information packed into zlib."
    ),
    url="https://dns-technologies.github.io/dbhose_airflow/base_modules/pgpack/index.html",
    long_description=long_description,
    long_description_content_type="text/markdown",
    package_dir={"": "src"},
    ext_modules=cythonize(extensions, language_level="3"),
    packages=[
        "pgpack",
        "pgpack.common",
        "pgpack.pgcopylib",
        "pgpack.pgcopylib.core",
        "pgpack.pgcopylib.core.types",
    ],
    package_data={
        "pgcopylib": [
            "**/*.py",
            "**/*.pyd",
            "**/*.pyi",
            "*.py",
            "*.pyd",
            "*.md",
            "*.txt",
        ]
    },
    exclude_package_data={
        "pgpack": ["*.c", "*.pxd", "*.pyx"],
        "pgpack.pgcopylib": ["**/*.c", "**/*.pxd", "**/*.pyx"],
    },
    include_package_data=True,
    setup_requires=["Cython>=0.29.33"],
    zip_safe=False,
)
