add_executable(aztl_tests
    test_main.cpp
)
target_link_libraries(aztl_tests PRIVATE aztl::aztl)
target_compile_features(aztl_tests PRIVATE cxx_std_23)

if(MSVC)
    target_compile_options(aztl_tests PRIVATE /W4 /permissive-)
else()
    target_compile_options(aztl_tests PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wshadow)
endif()

add_test(NAME aztl_tests COMMAND aztl_tests)
