project(kv_store LANGUAGES CXX VERSION 0.1.0 DESCRIPTION "Key Value Store")

link_directories(${CMAKE_BINARY_DIR})

add_executable(kv_store kv_store.cpp)
set_target_properties(kv_store PROPERTIES CXX_VISIBILITY_PRESET default)
target_include_directories(kv_store PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/
    ${FLATBUFFERS_INCLUDE_DIRS})
target_link_libraries(kv_store fabrix sqlite3)
add_dependencies(kv_store fbs_schemas)

add_executable(kv_store_test kv_store_test.cpp)
set_target_properties(kv_store_test PROPERTIES CXX_VISIBILITY_PRESET default)
target_include_directories(kv_store_test PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/
    ${FLATBUFFERS_INCLUDE_DIRS})
target_link_libraries(kv_store_test fabrix sqlite3 gtest_main pthread gmock gtest rt)
add_dependencies(kv_store_test fbs_schemas)