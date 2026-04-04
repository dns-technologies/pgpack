# Version History

## 0.3.3.dev5

* Developer release (not public to pip)
* Swap PGPackWriter metadata and fileobj initialization parameters

## 0.3.3.dev4

* Developer release (not public to pip)
* Refactor project code
* Move pgcopylib into pgpack. Now it is a one library
* Delete depends pgcopylib
* Add depends python-dateutil>=2.8.0
* Decomposite PGPackWriter.from_bytes() method
* Improve docstrings
* Update README.md

## 0.3.3.dev3

* Developer release (not public to pip)
* Refactor project code
* Change repository structure
* Update pyproject.toml

## 0.3.3.dev2

* Developer release (not public to pip)
* Add new errors
* Add imports from pgcopylib into pgpack.`__init__`.py
* Add pytest coverage
* Add comprehensive tests for all 32 PostgreSQL data types including arrays and edge cases
* Add support for empty DataFrame (auto-detect types as TEXT)
* Add error handling for empty files in PGPackReader
* Change S3File signature to b"S3\x00Stream\x00Object"
* Decompose project
* Fix detect_oid to return default TEXT type for empty or None values
* Fix PGPackReader.to_rows() to handle empty data without pgcopy object
* Fix PGPackWriter metadata generation for pandas and polars DataFrames
* Improve all compression methods support (GZIP, LZ4, SNAPPY, ZSTD, NONE)
* Improve S3 mode tail handling and validation
* Improve tell() method to return correct position after reading
* Improve close() method to properly close underlying file objects
* Add test_all_types_roundtrip covering all PostgreSQL data types
* Add test_unseekable_stream for socket-like streams
* Add test_s3_mode_tail_presence for S3 format validation
* Update `__repr__` methods for better debugging output
* Update project structure and dependencies
* Update depends pgcopylib==0.2.4.dev1
* Update depends light-compressor==0.1.1.dev1

## 0.3.3.dev1

* Developer release (not public to pip)
* Update depends light_compressor==0.1.0.dev3
* Update string representation for s3file object
* Fix docstring
* Refactor PGPackWriter.from_bytes() method
* Add PGPackReader.schema_overrides attribute for polars.DataFrame/LazyFrame
* Fix PGPackReader.to_polars() for dumps with nested objects
* Update README.md

## 0.3.3.dev0

* Developer release (not public to pip)
* Add compression_level parameter for PGPackWriter
* Add s3_file parameter for PGPackReader and PGPackWriter
* Add polars.LazyFrame support
* Add Gzip and Snappy compression support
* Speed-up PGPackWriter.from_pandas() method
* Refactor errors
* Update depends pgcopylib==0.2.4.dev0
* Update depends light-compressor==0.1.0.dev2
* Update README.md

## 0.3.2.3

* Update depends pgcopylib==0.2.3.3
* Update depends light-compressor==0.0.2.2
* Change url link

## 0.3.2.2

* Update depends pgcopylib==0.2.3.2
* Fix install on unix systems

## 0.3.2.1

* Update depends pgcopylib==0.2.3.1

## 0.3.2.0

* Update depends pgcopylib==0.2.3.0
* Fix unpack requires a buffer of 4 bytes for unix systems

## 0.3.1.7

* Update depends pgcopylib==0.2.2.8

## 0.3.1.6

* Update depends pgcopylib==0.2.2.7
* Update depends light-compressor==0.0.2.1
* Back compile depends to latest setuptools

## 0.3.1.5

* Update depends pgcopylib==0.2.2.6
* Update depends light-compressor==0.0.2.0
* Downgrade compile depends to setuptools<70

## 0.3.1.4

* Update depends pgcopylib==0.2.2.5

## 0.3.1.3

* Update depends pgcopylib==0.2.2.4

## 0.3.1.2

* Update depends pgcopylib==0.2.2.3
* Fix write_timestamp error Can't subtract offset-naive and offset-aware datetimes

## 0.3.1.1

* Update depends pgcopylib==0.2.2.2
* Update depends light-compressor==0.0.1.9

## 0.3.1.0

* Update depends pgcopylib==0.2.2.0
* Fixed conversion to pandas for null values ​​in int columns
* Fixed an issue with converting to polars for large numeric values

## 0.3.0.9

* Update depends pgcopylib==0.2.1.9
* Update depends light_compressor==0.0.1.8
* Change str and repr column view
* Add auto upload to pip

## 0.3.0.8

* Update depends pgcopylib==0.2.1.8
* Add automake universal wheel

## 0.3.0.7

* Update depends pgcopylib==0.2.1.7
* Update depends light_compressor==0.0.1.7
* Update depends setuptools==80.9.0

## 0.3.0.6

* Delete polars_schema its interferes with correct operation to_polars() method
* Fix pandas_astype

## 0.3.0.5

* Change PGPackReader & PGPackWriter tell() method
* Add numpy data types to AssociatePyType dictionary

## 0.3.0.4

* Update requirements.txt depends pgcopylib==0.2.1.6
* Update requirements.txt depends light_compressor==0.0.1.6
* Fix PGPackWriter from_rows() function
* Add PGPackWriter.__init_copy() function

## 0.3.0.3

* Add MANIFEST.in
* Add tell() & close() methods to PGPackReader & PGPackWriter
* Update requirements.txt depends pgcopylib==0.2.1.5
* Update requirements.txt depends light_compressor==0.0.1.5

## 0.3.0.2

* Add metadata_reader import

## 0.3.0.1

* Small refactor PGPackWriter 
* Update requirements.txt depends pgcopylib==0.2.1.4
* Add MIT License

## 0.3.0.0

* Delete python internal compressed libraryes
* Change requirements.txt
* Change methods & class attributes
* Change write strategy
* Change default write compression to ZSTD
* Update README.md
* Redistribute project directories
* Refactor PGPackReader & PGPackWriter classes
* Compressed core change to light-compressor
* Fix detect_oid function for pandas.Timestamp type
* Fix pandas.DataFrame string dtype from object to string[python]

## 0.2.0.1

* Update requirements.txt depends pgcopylib==0.2.1.2
* Add pandas.DataFrame & polars.DataFrame cast dtypes for PGPackReader

## 0.2.0.0

* Update requirements.txt depends pgcopylib==0.2.0.1
* Change methods to new pgcopylib library
* Add attribute metadata with uncompress metadata bytes

## 0.1.3.1

* Add array nested into metadata
* Add property method to CompressionMethod
* Update README.md

## 0.1.3

* Add PGParam class
* Add values length and numeric precision/scale
* Add pgparam attibute into PGPackReader and PGPackWriter
* Fix ZSTD unpacked length where write with to_python() method
* Update requeriments.txt

## 0.1.2

* Rename project to pgpack
* Rename classes from PGCrypt* to PGPack*
* Change header to b"PGPACK\n\x00"
* Add size parameter into to_python, to_pandas, to_polars and to_bytes methods
* Update requirements.txt
* Fix nan values from pandas.DataFrame

## 0.1.1

* Add CHANGELOG.md
* Update README.md
* Improve ZstdDecompressionReader.seek() method

## 0.1.0

* Add methods from_python(),  from_pandas(),  from_polars() to PGPackWriter
* Add detect_oid function for generate oids from python types
* Add metadata_from_frame function
* Rename dtypes to pgtypes
* Change PGDataType to PGOid in pgtypes
* New __str__ and __repr__ output in PGPackReader and PGPackWriter

## 0.0.4

* Add support CopyByffer object as buffer

## 0.0.3

* Remove columns count from __str__ method

## 0.0.2

* Fix ZstdDecompressionReader.readall()
* Add docstring into __init__.py
* Improve docs
* Publish library to Pip

## 0.0.1

First version of the library pgcrypt
