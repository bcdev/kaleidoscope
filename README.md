# Kaleidoscope

Monte Carlo simulation of errors for ESA SCOPE.

## Installing and testing

Read [INSTALL.md](INSTALL.md) for detailed instructions on installation
and testing.

## Operations Manual

### Operational principles

The processor is coded in Python and requires an environment described
in [INSTALL.md](INSTALL.md). The [kaleidoscope](kaleidoscope) directory
includes the `kaleidoscope` Python package, which includes the Kaleidoscope
processor. The processor is invoked from the command line. Typing

    kaleidoscope --help

will print a detailed usage message to the screen

    usage: kaleidoscope [-h] [--chunk-size-lat CHUNK_SIZE_LAT]
                        [--chunk-size-lon CHUNK_SIZE_LON]
                        [--engine-reader {h5netcdf,netcdf4,zarr}]
                        [--engine-writer {h5netcdf,netcdf4,zarr}]
                        [--log-level {debug,info,warning,error,off}]
                        [--mode {multithreading,synchronous}]
                        [--workers {1,2,3,4,5,6,7,8}] [--progress]
                        [--no-progress] [--stack-traces] [--no-stack-traces]
                        [--test] [--no-test] [--tmpdir TMPDIR] [-v]
                        source_file target_file
    
    This scientific processor simulates measurement errors.
    
    positional arguments:
      source_file           the file path of the source dataset.
      target_file           the file path of the target dataset.
    
    options:
      -h, --help            show this help message and exit
      --chunk-size-lat CHUNK_SIZE_LAT
                            specify the chunk size along the latitudinal
                            dimension for reading and computing data arrays. A
                            value of `-1` refers to full latitudinal chunk size
                            and a value of `0` refers to the chunk size used in
                            the source file. (default: None)
      --chunk-size-lon CHUNK_SIZE_LON
                            specify the chunk size along the longitudinal
                            dimension for reading and computing data arrays. A
                            value of `-1` refers to full longitudinal chunk size
                            and a value of `0` refers to the chunk size used in
                            the source file. (default: None)
      --engine-reader {h5netcdf,netcdf4,zarr}
                            specify the engine used to read the source product
                            file. (default: None)
      --engine-writer {h5netcdf,netcdf4,zarr}
                            specify the engine used to write the target product
                            file. (default: None)
      --log-level {debug,info,warning,error,off}
                            specify the log level. (default: None)
      --mode {multithreading,synchronous}
                            specify the operating mode. In multithreading mode a
                            multithreading scheduler is used. In synchronous
                            mode a single-thread scheduler is used. (default:
                            None)
      --workers {1,2,3,4,5,6,7,8}
                            specify the number of workers used in multithreading
                            mode. If not set, the number of workers is
                            determined by the system. (default: None)
      --progress            enable progress bar display. (default: False)
      --no-progress         disable progress bar display. (default: True)
      --stack-traces        enable Python stack traces. (default: False)
      --no-stack-traces     disable Python stack traces. (default: True)
      --test                enable test mode. (default: False)
      --no-test             disable test mode. (default: True)
      --tmpdir TMPDIR       specify the path to the temporary directory.
                            (default: None)
      -v, --version         show program's version number and exit
    
    Copyright (c) Brockmann Consult GmbH, 2025. License: MIT

### Normal operations

TBD.

### Error conditions

The processor terminates on the first occurrence of an error. The exit code
of the processor is `0` if the processor completed without errors, and nonzero
otherwise. Warning and error messages are sent to the standard error stream. 

### Recovery operations

There are no recovery operations.
