# Перейдите в корень проекта
cd informationSearch

# 1. Соберите все образы
docker-compose build --no-cache

# 2. Запустите все сервисы в фоновом режиме
docker-compose up -d

# 3. Проверьте статус контейнеров
docker-compose ps