project(pop)

set(POP_SRC cw.cc cw.h ring.cc ring.h)
add_library (pop STATIC ${POP_SRC})

# Make sure the compiler can find include files for our library
# when other libraries or executables link to pop

target_include_directories (pop 
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${MPI_INCLUDE_PATH})

target_link_libraries(pop PRIVATE ${MPI_LIBRARIES} spdlog) 

target_compile_features(pop PUBLIC cxx_std_11)