cmake_minimum_required(VERSION 3.25)
project(aztl VERSION 0.1.0 LANGUAGES CXX)

option(AZTL_BUILD_TESTS "Build correctness tests" ON)
option(AZTL_BUILD_BENCHMARKS "Build performance benchmarks" OFF)

add_library(aztl INTERFACE)
add_library(aztl::aztl ALIAS aztl)
target_compile_features(aztl INTERFACE cxx_std_23)
target_include_directories(aztl INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

if(AZTL_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

if(AZTL_BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()
