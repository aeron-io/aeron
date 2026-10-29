# C API

Here you will find the source for the C API for Aeron. The build process builds
a library for the client and places it in the following location

    ${CMAKE_CURRENT_BINARY_DIR}/lib/libaeron.so or libaeron.dylib

## Dependencies

The C API library requires the following dependencies.

- Aeron C API Library, source of which is included here and built and placed in `${CMAKE_CURRENT_BINARY_DIR}/lib`
- Linux Dependencies:
    - C Library (for the system built on)
    - `-lpthread` - pthread Library
    - `-ldl` - DL Library
    - `-lm` - Math Library
- Windows Dependencies:
	- Windows Version >= Vista 
	- MSVC >= v141 (Visual Studio 2017)

**Note:** The C API is supported in production on Linux. Windows build can be used for local development.

## Documentation and Samples

The C API has a single header that contains the API documentation 
[here](https://github.com/aeron-io/aeron/blob/master/aeron-client/src/main/c/aeronc.h)

The System Tests for the API and the C driver can be found 
[here](https://github.com/aeron-io/aeron/blob/master/aeron-driver/src/test/c/aeron_c_system_test.cpp).
	
Samples of usage of the C API are in development.
	
## Configuration

Configuration for the C API can be done programmatically and/or via environment variables. The variables are directly related to the Java properties
for the Java API. The environment variables simply have `_` in the place of `.`. For example, setting the environment variable `AERON_DIR` is equivalent
to setting `aeron.dir` in the Java API.
