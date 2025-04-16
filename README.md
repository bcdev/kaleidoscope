# Kaleidoscope

Monte Carlo simulation of errors for ESA SCOPE.

## Installing and testing

Read [INSTALL.md](INSTALL.md) for detailed instructions on installation
and testing.

## Operations Manual

### Operational principles

The processor is coded in Python and requires an environment described
in [INSTALL.md](INSTALL.md). The [kaleidoscope](kaleidoscope) directory includes the `kaleidoscope`
Python package, which includes the Kaleidoscope processor.
The processor is invoked from the command line. Typing

    kaleidoscope --help

will print a detailed usage message to the screen.

### Normal operations

TBD.

### Error conditions

The processor terminates on the first occurrence of an error. The exit code
of the processor is `0` if the processor completed without errors, and nonzero
otherwise. Warning and error messages are sent to the standard error stream. 

### Recovery operations

There are no recovery operations.
