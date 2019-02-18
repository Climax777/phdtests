cmake_minimum_required(VERSION 3.8)
project(dbphd)

# Set build type to RELEASE by default:
if(NOT CMAKE_BUILD_TYPE)
  message(STATUS "No build type provided, default to RELEASE.")
  set(CMAKE_BUILD_TYPE "RELEASE")
endif()

set( CMAKE_EXPORT_COMPILE_COMMANDS ON )
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG")

find_package (PkgConfig REQUIRED)
# Options can be turned on and off by providing cmake with a -DOPTIONNAME=ON (or OFF) argument.
# For example, to turn off benchmarks, build a shared library, and use sanitize flags in a DEBUG build:
#    cmake .. -DCMAKE_BUILD_TYPE=DEBUG -DDBPHD_BENCH=OFF -DBENCH_STATIC=OFF -DDBPHD_SANITIZE=ON
option(DBPHD_TEST      "Build tests" ON)
option(DBPHD_BENCH     "Build benchmarks" ON)
option(DBPHD_STATIC    "Builds a static library instead of a shared one" OFF)
option(DBPHD_SANITIZE  "Adds sanitive flags" OFF)

if(DBPHD_STATIC)
	set(DBPHD_LIB_NAME dbphd_static)
	set(DBPHD_LIB_TYPE STATIC)
else()
	set(DBPHD_LIB_NAME dbphd)
	set(DBPHD_LIB_TYPE SHARED)
endif()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

find_package(Threads REQUIRED)
find_package(Boost)
find_package(Math)

find_package(libmysqlcppconn)
pkg_check_modules(PQXX libpqxx)
pkg_check_modules(PQ libpq)
pkg_check_modules(MONGOCXX libmongocxx)

include_directories(. include ${Boost_INCLUDE_DIRS} ${PQXX_INCLUDE_DIRS})

set(CMAKE_CXX_STANDARD 17) # cmake 3.1 supports '98', '11' or '14'. cmake 3.8 or later support '17'.
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(DBPHD_SANITIZE)
  set(CMAKE_CXX_FLAGS "-fsanitize=address -fsanitize=undefined ${CMAKE_CXX_FLAGS}")
endif()

add_subdirectory(src)

if(DBPHD_TEST)
  enable_testing()
  add_subdirectory(test)
endif()

if(DBPHD_BENCH)
  add_subdirectory(bench)
endif()

if( EXISTS "${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json" )
  execute_process( COMMAND ${CMAKE_COMMAND} -E copy_if_different
    ${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json
    ${CMAKE_CURRENT_SOURCE_DIR}/compile_commands.json
  )
endif()

message(STATUS "")
message(STATUS "BUILD SUMMARY")
message(STATUS "  CMAKE_GENERATOR      : ${CMAKE_GENERATOR}")
message(STATUS "  Compiler ID          : ${CMAKE_CXX_COMPILER_ID}")
message(STATUS "  Build type           : ${CMAKE_BUILD_TYPE}")
message(STATUS "  Library name         : ${DBPHD_LIB_NAME}")
message(STATUS "  Library type         : ${DBPHD_LIB_TYPE}")
message(STATUS "  Build tests          : ${DBPHD_TEST}")
message(STATUS "  Build benchmarks     : ${DBPHD_BENCH}")
message(STATUS "  Sanitize flags       : ${DBPHD_SANITIZE}")
message(STATUS "  Boost include dirs   : ${Boost_INCLUDE_DIRS}")
message(STATUS "")
