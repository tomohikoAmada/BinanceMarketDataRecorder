cmake_minimum_required(VERSION 3.24)
project(bmcp_l2 LANGUAGES CXX)

set(BMD_PROJECTION_SOURCE_DIR "" CACHE PATH "Path to BinanceMarketDataProjection source checkout")
if(NOT BMD_PROJECTION_SOURCE_DIR AND EXISTS "${CMAKE_CURRENT_LIST_DIR}/../../../BinanceMarketDataProjection/CMakeLists.txt")
    set(BMD_PROJECTION_SOURCE_DIR "${CMAKE_CURRENT_LIST_DIR}/../../../BinanceMarketDataProjection")
endif()

if(BMD_PROJECTION_SOURCE_DIR AND EXISTS "${BMD_PROJECTION_SOURCE_DIR}/CMakeLists.txt")
    set(BMD_PROJECTION_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(BMD_PROJECTION_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
    set(BMD_PROJECTION_BUILD_FUZZERS OFF CACHE BOOL "" FORCE)
    set(BMD_PROJECTION_BUILD_PROTO_ADAPTER OFF CACHE BOOL "" FORCE)
    set(BMD_PROJECTION_ENABLE_WERROR OFF CACHE BOOL "" FORCE)
    add_subdirectory("${BMD_PROJECTION_SOURCE_DIR}" projection EXCLUDE_FROM_ALL)
else()
    find_package(BinanceMarketDataProjection 0.1.0 CONFIG REQUIRED COMPONENTS Core)
endif()

find_package(simdjson CONFIG REQUIRED)

add_library(bmcp_l2 SHARED
    l2_native.cpp
    canonical_cbor.cpp
)
target_compile_features(bmcp_l2 PRIVATE cxx_std_20)
target_link_libraries(bmcp_l2 PRIVATE
    BinanceMarketDataProjection::Core
    simdjson::simdjson
)
set_target_properties(bmcp_l2 PROPERTIES
    WINDOWS_EXPORT_ALL_SYMBOLS ON
    PREFIX "lib"
)
if(WIN32)
    # On Windows keep standard bmcp_l2.dll / libbmcp_l2.dll compatibility (no forced native-only ISA).
    set_target_properties(bmcp_l2 PROPERTIES PREFIX "")
endif()
