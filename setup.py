import shutil
from setuptools import (
    find_packages,
    setup,
)


shutil.rmtree("build", ignore_errors=True)
shutil.rmtree("pgpack.egg-info", ignore_errors=True)

with open(file="README.md", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="pgpack",
    version="0.3.3.dev2",
    packages=find_packages(),
    author="0xMihalich",
    author_email="bayanmobile87@gmail.com",
    description=(
        "PGCopy dump packed into LZ4, ZSTD or uncompressed "
        "with meta data information packed into zlib."
    ),
    url="https://dns-technologies.github.io/dbhose_airflow/base_modules/pgpack/index.html",
    long_description=long_description,
    long_description_content_type="text/markdown",
    zip_safe=False,
)
