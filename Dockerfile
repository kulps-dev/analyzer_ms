# Используем легкий образ nginx на базе Alpine Linux
FROM nginx:alpine

# Копируем файлы фронтенда в рабочую директорию nginx
COPY ./frontend /usr/share/nginx/html

# Копируем конфигурацию nginx
COPY nginx.conf /etc/nginx/nginx.conf

# Открываем 80 порт для веб-доступа
EXPOSE 80