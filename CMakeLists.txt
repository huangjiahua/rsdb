cmake_minimum_required(VERSION 3.10)
project(rsdb)

set(CMAKE_CXX_STANDARD 14)

include_directories(./include)

set(LIB_NAME rsdb)
set(STATIC_LIB_NAME rsdb_static)

set(SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(DB_SRC ${SRC_DIR}/src/db.cpp ${SRC_DIR}/src/db_impl.cpp ${SRC_DIR}/src/hash.cpp ${SRC_DIR}/src/util.cpp)
set(DB_SRC ${DB_SRC} ${SRC_DIR}/src/hash.cpp ${SRC_DIR}/src/iterator.cpp ${SRC_DIR}/src/iterator_impl.cpp)

add_library(${LIB_NAME} SHARED ${DB_SRC})

add_library(${STATIC_LIB_NAME} STATIC ${DB_SRC})
set_target_properties(${STATIC_LIB_NAME} PROPERTIES OUTPUT_NAME ${LIB_NAME})

if (ENABLE_TEST)
    add_subdirectory(test)
endif ()