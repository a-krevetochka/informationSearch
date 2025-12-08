# Многостадийная сборка
FROM ubuntu:22.04 AS builder

# Установка зависимостей для сборки
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    libmongocxx-dev \
    libbsoncxx-dev \
    libboost-all-dev \
    libssl-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копирование файлов из homework3
COPY . .

# Скачивание crow_all.h (header-only библиотека) если его нет
RUN if [ ! -f crow_all.h ]; then \
    curl -L https://github.com/CrowCpp/Crow/releases/download/v1.3.3/crow_all.h -o crow_all.h; \
    fi

# Проверка и создание CMakeLists.txt если его нет
RUN if [ ! -f CMakeLists.txt ]; then \
    echo 'cmake_minimum_required(VERSION 3.14)
project(SearchServer)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

# Поиск зависимостей
find_package(mongocxx REQUIRED)
find_package(bsoncxx REQUIRED)
find_package(Boost REQUIRED COMPONENTS system thread)

# Добавление текущей директории для crow_all.h
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_executable(search_server main.cpp)

target_link_libraries(search_server PRIVATE
    mongocxx_shared
    bsoncxx_shared
    Boost::system
    Boost::thread
    ssl
    crypto
    pthread
)

# Настройки сборки
set_target_properties(search_server PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
    RUNTIME_OUTPUT_NAME search_server
)' > CMakeLists.txt; \
    fi

# Сборка
RUN mkdir -p build && cd build && \
    cmake .. -DCMAKE_BUILD_TYPE=Release && \
    cmake --build . --parallel $(nproc)

# Финальный образ
FROM ubuntu:22.04

# Установка рантайм зависимостей
RUN apt-get update && apt-get install -y \
    libmongocxx-3.7.0 \
    libbsoncxx-3.7.0 \
    libboost-system1.74.0 \
    libboost-thread1.74.0 \
    libssl3 \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копирование бинарника
COPY --from=builder /app/build/search_server .

# Создание пользователя
RUN useradd -m -u 1001 search && chown -R search:search /app
USER search

EXPOSE 18080

CMD ["./search_server"]