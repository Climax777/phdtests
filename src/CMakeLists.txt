# Source files (except the main for the executable):
set(dbphd_src
	#TODO add others too
	dbphd.cpp
	mongodb/mongodb.cpp
	mysqldb/mysqldb.cpp
	postgresql/postgresql.cpp
    tpc/tpchelpers.cpp
)
message(STATUS "BSONCXX: ${BSONCXX_INCLUDE_DIRS}")
# Compile the library
add_library(${DBPHD_LIB_NAME} ${DBPHD_LIB_TYPE} ${dbphd_src})
target_include_directories(${DBPHD_LIB_NAME} PUBLIC include ${DBPHD_LIB_NAME} ${CMAKE_THREAD_LIBS_INIT} ${PQXX_INCLUDE_DIRS} ${CONCPP_INCLUDE_DIR} ${MONGOCXX_INCLUDE_DIRS} ${BSONCXX_INCLUDE_DIRS})
target_link_directories(${DBPHD_LIB_NAME} PUBLIC ${CONCPP_LIB_DIR})
target_link_libraries(${DBPHD_LIB_NAME} PUBLIC ${CMAKE_THREAD_LIBS_INIT} ${PQXX_LDFLAGS} ${PQ_LDFLAGS} ${CONCPP_LIBS} ${MONGOCXX_LDFLAGS} fmt::fmt)

# Compile the executable
add_executable(dbphd_exe main.cpp)

# setup include paths
target_include_directories(dbphd_exe PUBLIC include ${DBPHD_LIB_NAME} ${CMAKE_THREAD_LIBS_INIT} ${PQXX_INCLUDE_DIRS} ${CONCPP_INCLUDE_DIR} ${MONGOCXX_INCLUDE_DIRS})

# Link the executable to the dbphd library (and thread/math libs).
target_link_directories(dbphd_exe PUBLIC ${CONCPP_LIB_DIR})
target_link_libraries(dbphd_exe PUBLIC ${DBPHD_LIB_NAME} ${CMAKE_THREAD_LIBS_INIT} ${PQXX_LDFLAGS} ${PQ_LDFLAGS} ${CONCPP_LIBS} fmt::fmt)

# How and what to install
install(TARGETS ${DBPHD_LIB_NAME} LIBRARY DESTINATION lib ARCHIVE DESTINATION lib)
install(TARGETS dbphd_exe RUNTIME DESTINATION bin)
install(DIRECTORY ../include/dbphd DESTINATION include)
