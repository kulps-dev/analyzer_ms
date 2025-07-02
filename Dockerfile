FROM nginx:1.25-alpine

# Копируем статические файлы фронтенда
COPY frontend/ /usr/share/nginx/html/

# Копируем конфиг nginx
COPY nginx.conf /etc/nginx/nginx.conf

# Создаем папку для логов
RUN mkdir -p /var/log/nginx

# Открываем порт
EXPOSE 80

# Запускаем nginx
CMD ["nginx", "-g", "daemon off;"]