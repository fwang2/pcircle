cmake_minimum_required(VERSION 3.9.0)
project(P2 LANGUAGES CXX)
 

# disable 4.8.5 as regex is broken
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "4.8.5")
         message(FATAL_ERROR "Insufficient gcc version, require 4.8 or above.")
    endif()
    SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -pedantic")
endif()

# disable in soure build
if ( ${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR}  )
        file(REMOVE test.txt)
        message( FATAL_ERROR "In-source builds not allowed. Please make a new
        directory (called a build directory) and run CMake from there. You may
        need to remove CMakeCache.txt."  )
endif()


# bad practice?
SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -Wall")

# in case we need to install
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

# Some options
option(P2_USE_VALGRIND "Perform SelfTests with Valgrind" OFF)
option(P2_ENABLE_WERROR "Enable all warnings as errors" OFF)
option(P2_BUILD_FPROF "Build fprof" ON)
option(P2_BUILD_FFIND "Build ffind" OFF)
option(P2_BUILD_TESTING "Build P2 Test" OFF)


# Define some folders
set(P2_DIR ${CMAKE_SOURCE_DIR})


## MPI
find_package(MPI REQUIRED)

## Boost
find_package(Boost 1.56 COMPONENTS serialization)

if(Boost_FOUND)
set(HAVE_BOOST_SERIALIZATION ON)
endif()

configure_file(
  ${CMAKE_SOURCE_DIR}/config-h.in.cmake
  ${CMAKE_BINARY_DIR}/config.h
)

# Add POP and externals
add_subdirectory(pop)
add_subdirectory(externals)
add_subdirectory(utils)


# Add fprof
if(P2_BUILD_FPROF)
add_executable(fprof fprof.cc)
target_link_libraries(fprof 
    PRIVATE
        ${MPI_LIBRARIES} 
        pop utils spdlog cli11
)
endif()

# Add ffind
if(P2_BUILD_FFIND)
# find_package(Protobuf)
# protobuf_generate_cpp(FFIND_PROTO_SRCS FFIND_PROTO_HDRS ffind.proto)
add_executable(ffind ffind.cc flist.cc flist.h)
target_link_libraries(ffind 
    PRIVATE
        ${MPI_LIBRARIES}
        Boost::serialization
        pop utils spdlog cli11
)
endif()

if (P2_BUILD_TESTING)
    add_subdirectory(tests)
endif()


set(CPACK_PACKAGE_NAME pcircle)
set(CPACK_PACKAGE_DESCRIPTION "A set of parallel filesystem tools")
set(CPACK_PACKAGE_VERSION_MAJOR 0)
set(CPACK_PACKAGE_VERSION_MINOR 19)
set(CPACK_PACKAGE_VERSION_PATCH 1)
set(CPACK_PACKAGE_HOMEPAGE_URL http://www.github.com/olcf/pcircle)
set(CPACK_GENERATOR "RPM")
set(CPACK_RPM_PACKAGE_REQUIRES_PRE "openmpi")
install(TARGETS fprof RUNTIME DESTINATION bin)
include(CPack)
