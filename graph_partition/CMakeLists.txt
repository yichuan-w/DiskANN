# Graph partition CMakeLists.txt
# This is included as a subdirectory from the main DiskANN CMakeLists.txt

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Include directories
include_directories(include)
include_directories(../include)

# OpenMP - disabled for compatibility
message(STATUS "OpenMP disabled for compatibility")


# OpenMP - disabled for compatibility
message(STATUS "OpenMP disabled for compatibility")

# `partitioner` is a CLI tool and not required for Python wheel builds.
# Skip it under PYBIND to avoid unnecessary Boost linkage in CI wheel jobs.
if(NOT PYBIND)
    find_package(Boost COMPONENTS program_options REQUIRED)
    add_executable(partitioner src/partitioner.cpp)
    target_link_libraries(partitioner PUBLIC Boost::program_options)
endif()

add_executable(index_relayout index_relayout.cpp)
target_link_libraries(index_relayout PUBLIC)
