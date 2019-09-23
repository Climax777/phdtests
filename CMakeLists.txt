cmake_minimum_required(VERSION 3.8)
project(dbphd)

# Set build type to RELEASE by default:
if(NOT CMAKE_BUILD_TYPE)
	message(STATUS "No build type provided, default to RELEASE.")
	set(CMAKE_BUILD_TYPE "RELEASE")
endif()

if(UNIX AND NOT APPLE)
	set(LINUX TRUE)
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON )
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG")
set(CMAKE_SKIP_BUILD_RPATH FALSE)
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
#set (CMAKE_INSTALL_RPATH "$CMAKE_INSTALL_PREFIX/lib")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

## the RPATH to be used when installing, but only if it's not a system directory
#LIST(FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES "${CMAKE_INSTALL_PREFIX}/lib" isSystemDir)
#IF("${isSystemDir}" STREQUAL "-1")
#SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
#ENDIF("${isSystemDir}" STREQUAL "-1")

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

if((CMAKE_SIZEOF_VOID_P EQUAL 8) OR (CMAKE_CXX_COMPILER_ID MATCHES "SunPro"))
	set(IS64BIT 1)
	message("Generating 64bit code")
else()
	message("Generating 32bit code")
endif()

set(WITH_CONCPP $ENV{WITH_CONCPP} CACHE PATH
	"MySQL Connector/C++ 8.0 install location"
	)

set(CONCPP_INCLUDE_DIR $ENV{CONCPP_INCLUDE_DIR} CACHE PATH
	"Location of Connector/C++ 8.0 headers"
	)

set(CONCPP_LIB_DIR $ENV{CONCPP_LIB_DIR} CACHE PATH
	"Location of Connector/C++ 8.0 libraries"
	)

# Set include and library paths if not given
if(NOT WITH_CONCPP AND LINUX)
	set(WITH_CONCPP "/usr")
endif()

if(WITH_CONCPP)
	if(NOT CONCPP_INCLUDE_DIR)
		if(LINUX)
			set(CONCPP_INCLUDE_DIR "${WITH_CONCPP}/include/mysql-cppconn-8")
		else()
			set(CONCPP_INCLUDE_DIR "${WITH_CONCPP}/include")
		endif()
	endif()
	if(NOT CONCPP_LIB_DIR)
		if(IS64BIT AND NOT LINUX)
			set(CONCPP_LIB_DIR "${WITH_CONCPP}/lib64")
		else()
			set(CONCPP_LIB_DIR "${WITH_CONCPP}/lib")
		endif()
	endif()
endif()

if(NOT CONCPP_INCLUDE_DIR OR NOT CONCPP_LIB_DIR)
	message(FATAL_ERROR
		"This project requires MySQL Connector/C++ 8.0, please specify install location"
		" using WITH_CONCPP setting or set header/library paths with CONCPP_INCLUDE_DIR"
		" and CONCPP_LIB_DIR settings."
		)
endif()

if(NOT EXISTS "${CONCPP_INCLUDE_DIR}/mysqlx/xdevapi.h")
	message(FATAL_ERROR
		"Could not find MySQL Connector/C++ 8.0 headers at specified"
		" location: ${CONCPP_INCLUDE_DIR}"
		)
endif()

set(CONCPP_LIBS
	mysqlcppconn8
	ssl
	crypto
	)

#set(CONCPP_JDBC_LIBS
#optimized mysqlcppconn
#)


find_package(Threads REQUIRED)
find_package(Boost)
find_package(Math)

pkg_check_modules(PQXX libpqxx)
pkg_check_modules(PQ libpq)
pkg_check_modules(MONGOCXX libmongocxx)
pkg_check_modules(BSONCXX libbsoncxx)

include_directories(. include ${Boost_INCLUDE_DIRS} ${PQXX_INCLUDE_DIRS} ${CONCPP_INCLUDE_DIR} ${MONGOCXX_INCLUDE_DIRS})

set(CMAKE_CXX_STANDARD 17) # cmake 3.1 supports '" 98', '11' or '14'. cmake 3.8 or later support '17'.
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
message(STATUS "  PostgreSQL include dirs   : ${PQXX_INCLUDE_DIRS}")
message(STATUS "  PostgreSQL libs   : ${PQXX_LDFLAGS}" )
message(STATUS "  Mysql include dirs   : ${CONCPP_INCLUDE_DIR}")
message(STATUS "  Mysql libs   : ${CONCPP_LIB_DIR} ${CONCPP_LIBS}" )
message(STATUS "  MongoDB include dirs   : ${MONGOCXX_INCLUDE_DIRS}")
message(STATUS "  MongoDB libs   : ${MONGOCXX_LDFLAGS}" )
message(STATUS "")
