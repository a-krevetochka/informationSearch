cmake_minimum_required(VERSION 3.14)
project(MongoTest)

set(CMAKE_CXX_STANDARD 17)

# MongoDB C++ драйвер через vcpkg
find_package(mongocxx REQUIRED CONFIG)
find_package(bsoncxx REQUIRED CONFIG)

# Добавляем include для Crow (header-only)
# Укажи путь к vcpkg или скачанному Crow
include_directories(C:/Users/meteo/Desktop/informationSearch/vcpkg/installed/x64-windows/include)
# или если скачал Crow в проект:
# include_directories(${CMAKE_SOURCE_DIR}/Crow/include)

add_executable(MongoTest main.cpp)

target_link_libraries(MongoTest PRIVATE
        mongo::mongocxx_shared
        mongo::bsoncxx_shared
)
