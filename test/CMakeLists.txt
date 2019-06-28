include_directories(../src/)
add_executable(hash_test hash_test.cpp ${DB_SRC})
target_link_libraries(hash_test gtest gtest_main pthread)

add_executable(basic_test basic_test.cpp)
target_link_libraries(basic_test ${LIB_NAME})

add_executable(iterator_test iterator_test.cpp)
target_link_libraries(iterator_test ${LIB_NAME})

add_executable(multithread_basic_test multithread_basic_test.cpp)
target_link_libraries(multithread_basic_test ${LIB_NAME} pthread gtest gtest_main)

add_executable(multithread_iterator_test multithread_iterator_test.cpp)
target_link_libraries(multithread_iterator_test ${LIB_NAME} pthread gtest gtest_main)
