cmake_minimum_required(VERSION 3.14)
project(rsdb)

set(CMAKE_CXX_STANDARD 14)

include_directories(./include)

add_executable(rsdb main.cpp src/db_impl.cpp src/db.cpp src/util.cpp)