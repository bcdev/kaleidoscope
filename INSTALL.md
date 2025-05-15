# Operational Environment

## Hardware configuration

The Water Quality Forecast Processor requires a Linux environment which can execute Python
code. The table below specifies the environment characteristics.

| Environment characteristics  | Environment specification | Comment                     |
|:-----------------------------|:--------------------------|:----------------------------|
| Operating system             | Unix-like                 |                             |
| CPU architecture             | Any                       |                             |
| Number of CPU cores          | 2-4                       |                             |
| Number of logical processors | 4-8                       |                             |
| RAM                          | 8 GB                      |                             |
| Disk space                   | 100 GB                    |                             |
| Dependencies                 | Python 3.10               | Managed by `conda` or `pip` |

## Software configuration

Running the Kaleidoscope processor requires Python 3.10 or higher (preferably a Miniconda
distribution) installed  on  the system. How to install the processor is
described further below. The installation process automatically installs
dependencies as needed.

## Operational constraints

No constraints are applicable.

## External dependencies

Python package dependencies are specified in the [pyproject.toml](environment.yml) file.

# Installation

Installing the software and its dependencies is a manual process, which is
described hereafter. 

## Install the software

To install the Python packages and their dependencies into your Python environment
type

    python -m pip install .

Repeat the installation after each update of the software library. The installation
command automatically installs all missing dependencies into your environment, too.
If your environment satisfies all dependencies already, you may perform the
installation without dependencies, i.e,

    python -m pip install --no-deps .

In a development environment, you may instead type, once,

    python -m pip install --no-deps --editable .

which registers the current directory into your environment in a way which does
not require repeated installation.

## Verification

To verify the software installation, open a terminal window and type

    kaleidoscope --help

which will print usage messages. To execute unit level tests `cd` into 
the `kaleidoscope` directory and type

    pytest

The `pytest` output will be printed to the console and will read like, e.g.:

    ============================ test session starts =============================
    platform ...
    rootdir: Developer/github/kaleidoscope
    configfile: pyproject.toml
    testpaths: test
    plugins: ...
    collected 14 items                                                           

    test/kaleidoscope/main/test_processor.py .....                         [ 35%]
    test/kaleidoscope/test_generators.py .                                 [ 42%]
    test/kaleidoscope/test_reader.py ....                                  [ 71%]
    test/kaleidoscope/test_writer.py ....                                  [100%]
    ======================= 14 passed in 152.59s (0:02:32) =======================
