# Source files (except the main for the executable):
set(dbphd_src
	#TODO add others too
	dbphd.cpp
	mongodb/mongodb.cpp
	mysqldb/mysqldb.cpp
	postgresql/postgresql.cpp
)

# Compile the library
add_library(${DBPHD_LIB_NAME} ${DBPHD_LIB_TYPE} ${dbphd_src})
target_include_directories(${DBPHD_LIB_NAME} PUBLIC ${DBPHD_LIB_NAME} ${CMAKE_THREAD_LIBS_INIT} ${MATH_LIBS} ${PQXX_INCLUDE_DIRS} ${CONCPP_INCLUDE_DIR})
target_link_directories(${DBPHD_LIB_NAME} PUBLIC ${CONCPP_LIB_DIR})
target_link_libraries(${DBPHD_LIB_NAME} PUBLIC ${CMAKE_THREAD_LIBS_INIT} ${MATH_LIBS} ${PQXX_LDFLAGS} ${PQ_LDFLAGS} ${CONCPP_LIBS} ${CONCPP_JDBC_LIBS})

# Compile the executable
add_executable(dbphd_exe main.cpp)

# setup include paths
target_include_directories(dbphd_exe PUBLIC ${DBPHD_LIB_NAME} ${CMAKE_THREAD_LIBS_INIT} ${MATH_LIBS} ${PQXX_INCLUDE_DIRS} ${CONCPP_INCLUDE_DIR})

# Link the executable to the dbphd library (and thread/math libs).
target_link_directories(dbphd_exe PUBLIC ${CONCPP_LIB_DIR})
target_link_libraries(dbphd_exe PUBLIC ${DBPHD_LIB_NAME} ${CMAKE_THREAD_LIBS_INIT} ${MATH_LIBS} ${PQXX_LDFLAGS} ${PQ_LDFLAGS} ${CONCPP_LIBS}  ${CONCPP_JDBC_LIBS})

# How and what to install
install(TARGETS ${DBPHD_LIB_NAME} LIBRARY DESTINATION lib ARCHIVE DESTINATION lib)
install(TARGETS dbphd_exe RUNTIME DESTINATION bin)
install(DIRECTORY ../include/dbphd DESTINATION include)
