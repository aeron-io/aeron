# C Media Driver

Here you will find the source for the C Media Driver for Aeron. The build process builds
a binary for the driver and places it in the following location

    ${CMAKE_CURRENT_BINARY_DIR}/binaries/aeronmd

## Dependencies

The driver binary requires the following dependencies.

- Aeron C Driver Library, source of which is included here and built and placed in `${CMAKE_CURRENT_BINARY_DIR}/lib`
- Linux Dependencies:
    - C Library (for the system built on)
    - `-lpthread` - pthread Library
    - `-ldl` - DL Library
    - `-lbsd` - BSD Library (optional - will use /dev/urandom directly instead of arc4random if not available)
    - `-luuid` - UUID Library (optional - will use pure random Receiver ID if not available)
    - `-lm` - Math Library
- Windows Dependencies:
	- Windows Version >= Vista 
	- MSVC >= v141 (Visual Studio 2017)
	
**Note:** The C Media Driver is supported in production on Linux. Windows build can be used for local development.

## Configuration

All configuration for the C media driver is done, currently, via environment variables. The variables are directly related to the Java properties
for the Java media driver. The environment variables simply have `_` in the place of `.`. For example, setting the environment variable `AERON_TERM_BUFFER_LENGTH` is equivalent
to setting `aeron.term.buffer.length` in the Java media driver.

## Operation

The driver can be started simply by executing `aeronmd`.

    $ aeronmd

The driver can be stopped gracefully via `Control-C` or `SIGINT` just like the Java media driver.

## Embedding the Driver

The C media driver may be embedded quite easily. An example is the driver main itself, 
[aeronmd.c](https://github.com/aeron-io/aeron/blob/master/aeron-driver/src/main/c/aeronmd.c), and the API is documented in 
[aeronmd.h](https://github.com/aeron-io/aeron/blob/master/aeron-driver/src/main/c/aeronmd.h).

## Driver Logging

The C media driver uses DL interception for logging. To use this logging, add the
Aeron C Driver Agent Library to `LD_PRELOAD` or `DYLD_INSERT_LIBRARIES` (for Mac.
Also requires flat namespace) and set the environment variable, `AERON_EVENT_LOG`
to a numeric mask for the events of interest. The following
[script](https://github.com/aeron-io/aeron/blob/master/aeron-samples/scripts/logging-c-media-driver)
may be used for convenience.
