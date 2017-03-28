cmake_minimum_required(VERSION 3.6)
project(producer_consumer)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

set(SOURCE_FILES main.cpp)
add_executable(producer_consumer ${SOURCE_FILES})