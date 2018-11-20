



# Header only library
add_library(catch INTERFACE)
target_include_directories(catch INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/catch2)


# Header only library
add_library(cli11 INTERFACE)
target_include_directories(cli11 INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/cli)


# Header only library
add_library(spdlog INTERFACE)
target_include_directories(spdlog INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/)




