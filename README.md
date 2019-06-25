# RSDB: Really Simple Database

Author: *huangjiahua*

## About

* Inspired by the database library in *[APUE](http://www.apuebook.com)* chapter 20.
* Implemented by C++

## Version

* V 0.1.0

## TODO

1. Add iterator support.
2. Refactor the codes to make them more "C++", because currently it use many codes directly from *[APUE](http://www.apuebook.com)* , which were written in C.
3. Make it thread-safe.
4. Implement a server using socket to provide service and provide client API.

## How to build?

```
cd path/to/the/project
mkdir build && cd build
cmake .. # required cmake version > 3.10
make
```

## How to use?

* After you build the project, you will get a shared library file (librsdb.so in Linux, librsdb.dylib in Mac OS X) and a static library file (librsdb.a).
* Include the header in the include/rsdb and link the library you've just built, and you can start using RSDB.
* Alternatively, if you use cmake, you can just drag the project in to your cmake project and use `add_subdirectory` to add rsdb to your project. And you may do things like the following.

```cmake
add_executable(my_binary ${MY_FILES})
target_link_libraries(my_binary rsdb ${OTHER_LIBS})
```