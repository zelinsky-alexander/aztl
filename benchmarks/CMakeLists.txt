add_executable(aztl_benchmarks benchmark_main.cpp)
target_link_libraries(aztl_benchmarks PRIVATE aztl::aztl)
target_compile_features(aztl_benchmarks PRIVATE cxx_std_23)

if(MSVC)
    target_compile_options(aztl_benchmarks PRIVATE /O2 /W4)
else()
    target_compile_options(aztl_benchmarks PRIVATE -O3 -DNDEBUG -Wall -Wextra -Wpedantic)
endif()
