cmake_minimum_required(VERSION 3.20)
project(OpenYXDB VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# --- Library sources ---

set(LIB_SOURCES
    src/FieldBase.cpp
    src/FieldBlob.cpp
    src/FieldBool.cpp
    src/FieldDateTime.cpp
    src/FieldFixedDecimal.cpp
    src/FieldSchema.cpp
    src/FieldType.cpp
    src/GlotKernel.cpp
    src/Hash128.cpp
    src/LzfWrapper.cpp
    src/MiniXmlParser.cpp
    src/Open_AlteryxYXDB.cpp
    src/Record.cpp
    src/RecordCopier.cpp
    src/RecordInfo.cpp
    src/SCType.cpp
    src/SpookyV2.cpp
    src/SRC_stringHelper_OpenAlteryx.cpp
    src/StringToDouble.cpp
    src/UnicodeCaseFoldingTable.cpp
    src/UnicodeCompareNoCase.cpp
    src/UTF16Traits.cpp
)

set(C_SOURCES
    src/lzf_c.c
    src/lzf_d.c
    src/sqlite3.c
)

set_source_files_properties(${C_SOURCES} PROPERTIES LANGUAGE C)
set_source_files_properties(${C_SOURCES} PROPERTIES COMPILE_OPTIONS "-w")

# Vendored C++ files - suppress warnings
set_source_files_properties(src/SpookyV2.cpp PROPERTIES COMPILE_OPTIONS "-w")

add_library(openyxdb SHARED ${LIB_SOURCES} ${C_SOURCES})

target_compile_definitions(openyxdb PRIVATE
    SRCLIB_REPLACEMENT
    OPEN_ALTERYX_EXPORTS
    UNICODE
    NOMINMAX
)

target_include_directories(openyxdb
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
)

if(MSVC)
    target_compile_options(openyxdb PRIVATE
        /W4 /WX
        /wd4127 /wd4245 /wd4251 /wd4275 /wd4267 /wd4244
        -EHa -J -MP
    )
    target_compile_options(openyxdb PRIVATE
        $<$<CONFIG:Debug>:/Od /RTC1>
        $<$<CONFIG:Release>:/Ob2 /Oi /Zi>
    )
else()
    target_compile_options(openyxdb PRIVATE
        -Wall -Wextra -pedantic
        -Wno-unknown-pragmas
        -Wno-parentheses
        -funsigned-char
        $<$<CONFIG:Debug>:-O0 -g>
        $<$<CONFIG:Release>:-O2>
    )
    target_link_libraries(openyxdb PRIVATE pthread dl)
endif()

target_precompile_headers(openyxdb PRIVATE
    "$<$<COMPILE_LANGUAGE:CXX>:${CMAKE_SOURCE_DIR}/include/stdafx.h>"
)

# --- Tests ---

option(OPENYXDB_BUILD_TESTS "Build tests" ON)

if(OPENYXDB_BUILD_TESTS)
    enable_testing()

    # Legacy example test
    add_executable(openyxdb_example test/main.cpp)
    target_include_directories(openyxdb_example PRIVATE ${CMAKE_SOURCE_DIR}/include)
    target_compile_definitions(openyxdb_example PRIVATE UNICODE NOMINMAX)
    target_link_libraries(openyxdb_example PRIVATE openyxdb)

    add_test(NAME example_roundtrip COMMAND openyxdb_example)

    # Catch2 tests
    find_package(Catch2 3 QUIET)
    if(Catch2_FOUND)
        file(GLOB TEST_SOURCES test/test_*.cpp)
        if(TEST_SOURCES)
            add_executable(openyxdb_tests ${TEST_SOURCES})
            target_include_directories(openyxdb_tests PRIVATE ${CMAKE_SOURCE_DIR}/include)
            target_compile_definitions(openyxdb_tests PRIVATE UNICODE NOMINMAX)
            target_link_libraries(openyxdb_tests PRIVATE openyxdb Catch2::Catch2WithMain)

            include(Catch)
            catch_discover_tests(openyxdb_tests)
        endif()
    endif()
endif()

# --- Python bindings ---

option(OPENYXDB_BUILD_PYTHON "Build Python bindings" ON)

if(OPENYXDB_BUILD_PYTHON)
    find_package(Python COMPONENTS Interpreter Development.Module QUIET)
    if(Python_FOUND)
        execute_process(
            COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
            OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE NB_DIR)
        list(APPEND CMAKE_PREFIX_PATH "${NB_DIR}")
        find_package(nanobind CONFIG QUIET)
        if(nanobind_FOUND)
            nanobind_add_module(_openyxdb python/_openyxdb.cpp)
            target_link_libraries(_openyxdb PRIVATE openyxdb)
            target_include_directories(_openyxdb PRIVATE ${CMAKE_SOURCE_DIR}/include)
            target_compile_definitions(_openyxdb PRIVATE SRCLIB_REPLACEMENT UNICODE NOMINMAX)

            install(TARGETS _openyxdb LIBRARY DESTINATION .)
        endif()
    endif()
endif()
