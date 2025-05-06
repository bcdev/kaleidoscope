# ![Kaleidoscope](/assets/img/kaleidoscope.png)

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

    usage: kaleidoscope [-h]
                        --source-type {esa-cci-oc,esa-scope-exchange,ghrsst,glorys}
                        --selector SELECTOR
                        [--engine-reader {h5netcdf,netcdf4,zarr}]
                        [--engine-writer {h5netcdf,netcdf4,zarr}]
                        [--log-level {debug,info,warning,error,off}]
                        [--mode {multithreading,synchronous}]
                        [--workers {1,2,3,4,5,6,7,8}] [--progress]
                        [--no-progress] [--stack-traces] [--no-stack-traces]
                        [--tmpdir TMPDIR] [-v]
                        source_file target_file
    
    This scientific processor simulates measurement errors.
    
    positional arguments:
      source_file           the file path of the source dataset.
      target_file           the file path of the target dataset.
    
    options:
      -h, --help            show this help message and exit
      --source-type {esa-cci-oc,esa-scope-exchange,ghrsst,glorys}
                            the source type. (default: None)
      --selector SELECTOR   the Monte Carlo stream selector. An integral number
                            which must not be negative. (default: None)
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
      --tmpdir TMPDIR       specify the path to the temporary directory.
                            (default: None)
      -v, --version         show program's version number and exit
    
    Copyright (c) Brockmann Consult GmbH, 2025. License: MIT

### Normal operations

To invoke the processor from the terminal, for instance, type 

    kaleidoscope --source-type ghrsst --selector 17 in.nc out.nc

which normally will log information to the terminal, e.g.,

    2025-04-30T09:42:11.928000Z <node> kaleidoscope 2025.1.0 [76069] [I] starting running processor
    2025-04-30T09:42:11.928000Z <node> kaleidoscope 2025.1.0 [76069] [I] config: engine_reader = None
    2025-04-30T09:42:11.928000Z <node> kaleidoscope 2025.1.0 [76069] [I] config: engine_writer = None
    2025-04-30T09:42:11.928000Z <node> kaleidoscope 2025.1.0 [76069] [I] config: log_level = info
    2025-04-30T09:42:11.928000Z <node> kaleidoscope 2025.1.0 [76069] [I] config: mode = multithreading
    2025-04-30T09:42:11.928000Z <node> kaleidoscope 2025.1.0 [76069] [I] config: processor_name = kaleidoscope
    2025-04-30T09:42:11.928000Z <node> kaleidoscope 2025.1.0 [76069] [I] config: processor_version = 2025.1.0
    2025-04-30T09:42:11.928000Z <node> kaleidoscope 2025.1.0 [76069] [I] config: source_type = ghrsst
    2025-04-30T09:42:11.928000Z <node> kaleidoscope 2025.1.0 [76069] [I] config: progress = False
    2025-04-30T09:42:11.928000Z <node> kaleidoscope 2025.1.0 [76069] [I] config: selector = 17
    2025-04-30T09:42:11.928000Z <node> kaleidoscope 2025.1.0 [76069] [I] config: source_file = in.nc
    2025-04-30T09:42:11.929000Z <node> kaleidoscope 2025.1.0 [76069] [I] config: stack_traces = False
    2025-04-30T09:42:11.929000Z <node> kaleidoscope 2025.1.0 [76069] [I] config: target_file = out.nc
    2025-04-30T09:42:11.929000Z <node> kaleidoscope 2025.1.0 [76069] [I] config: tmpdir = .
    2025-04-30T09:42:11.929000Z <node> kaleidoscope 2025.1.0 [76069] [I] config: workers = 2
    2025-04-30T09:42:12.149000Z <node> kaleidoscope 2025.1.0 [76069] [I] starting creating processing graph
    2025-04-30T09:42:12.150000Z <node> kaleidoscope 2025.1.0 [76069] [I] starting graph for variable: analysed_sst
    2025-04-30T09:42:14.765000Z <node> kaleidoscope 2025.1.0 [76069] [I] finished graph for variable: analysed_sst
    2025-04-30T09:42:14.765000Z <node> kaleidoscope 2025.1.0 [76069] [I] finished creating processing graph
    2025-04-30T09:42:14.765000Z <node> kaleidoscope 2025.1.0 [76069] [I] starting writing target dataset: out.nc
    2025-04-30T09:42:20.637000Z <node> kaleidoscope 2025.1.0 [76069] [I] finished writing target dataset
    2025-04-30T09:42:20.638000Z <node> kaleidoscope 2025.1.0 [76069] [I] starting closing datasets
    2025-04-30T09:42:20.638000Z <node> kaleidoscope 2025.1.0 [76069] [I] finished closing datasets
    2025-04-30T09:42:20.638000Z <node> kaleidoscope 2025.1.0 [76069] [I] finished running processor
    2025-04-30T09:42:20.639000Z <node> kaleidoscope 2025.1.0 [76069] [I] elapsed time (seconds):    8.710

and eventually produce a forecast output dataset. Normally, the processor
will terminate with an exit code of `0`. 

### Error conditions

The processor terminates on the first occurrence of an error. The exit code
of the processor is `0` if the processor completed without errors, and nonzero
otherwise. Warning and error messages are sent to the standard error stream. 

### Recovery operations

There are no recovery operations.
