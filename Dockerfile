# Используем легкий образ nginx на базе Alpine Linux
FROM nginx:alpine

# Копируем файлы фронтенда в рабочую директорию nginx
COPY ./frontend /usr/share/nginx/html

# Копируем конфигурацию nginx
COPY nginx.conf /etc/nginx/nginx.conf

# Открываем 80 порт для веб-доступа
EXPOSE 80

# Запускаем nginx в foreground режиме
CMD ["nginx", "-g", "daemon off;"]