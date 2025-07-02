document.addEventListener('DOMContentLoaded', function() {
    // 1. Инициализация datepicker
    const datepickers = document.querySelectorAll('.datepicker');
    if (datepickers.length > 0) {
        flatpickr(".datepicker", {
            dateFormat: "Y-m-d",
            locale: "ru",
            defaultDate: new Date()
        });
    }

    // 2. Функции для отображения статусов
    const showStatus = (message, type = 'info') => {
        const statusBar = document.getElementById('status-bar');
        if (!statusBar) return;
        
        const icons = {
            success: 'check-circle',
            error: 'times-circle',
            warning: 'exclamation-circle',
            info: 'info-circle',
            loading: 'spinner fa-spin'
        };
        
        statusBar.innerHTML = `<i class="fas fa-${icons[type] || 'info-circle'}"></i> ${message}`;
        statusBar.className = `status-bar show ${type}`;
    };

    const showAlert = (message, type = 'info') => {
        const alert = document.createElement('div');
        alert.className = `custom-alert ${type} show`;
        alert.innerHTML = `
            <i class="fas fa-${type === 'success' ? 'check-circle' : 'exclamation-circle'}"></i>
            ${message}
        `;
        document.body.appendChild(alert);
        
        setTimeout(() => {
            alert.classList.remove('show');
            setTimeout(() => alert.remove(), 500);
        }, 3000);
    };

    // 3. Универсальная функция для запросов к API
    const makeApiRequest = async (url, method, body) => {
        try {
            const response = await fetch(url, {
                method,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body)
            });

            if (!response.ok) {
                const error = await response.text();
                throw new Error(error || 'Ошибка сервера');
            }

            return response;
        } catch (error) {
            console.error(`API Request Error (${url}):`, error);
            throw error;
        }
    };

    // 4. Функция скачивания файла (улучшенная)
    const downloadFile = async (response, defaultFilename = 'report.xlsx') => {
        try {
            // Проверяем, что ответ содержит данные
            if (!response.ok || !response.body) {
                throw new Error('Неверный ответ сервера');
            }
    
            // Получаем имя файла из заголовков
            const contentDisposition = response.headers.get('content-disposition');
            let filename = defaultFilename;
            
            if (contentDisposition) {
                const match = contentDisposition.match(/filename="?([^"]+)"?/);
                if (match) filename = match[1];
            }
    
            // Создаем Blob
            const blob = await response.blob();
            
            // Проверяем, что Blob создан
            if (!blob || blob.size === 0) {
                throw new Error('Пустой файл или ошибка создания Blob');
            }
    
            // Создаем ссылку для скачивания
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.style.display = 'none';
            a.href = url;
            a.download = filename;
            
            // Добавляем ссылку в DOM и эмулируем клик
            document.body.appendChild(a);
            a.click();
    
            // Очистка
            window.URL.revokeObjectURL(url);
            setTimeout(() => {
                document.body.removeChild(a);
            }, 100);
    
            return true;
        } catch (error) {
            console.error('Download Error:', error);
            throw error;
        }
    };

    // 5. Обработчик для кнопки Excel

    document.getElementById('export-excel-btn').addEventListener('click', async function() {
        const startDate = document.getElementById('start-date').value;
        const endDate = document.getElementById('end-date').value;
        
        if (!startDate || !endDate) {
            showAlert('Пожалуйста, укажите период анализа', 'error');
            return;
        }
    
        const btn = this;
        const originalText = btn.innerHTML;
        
        try {
            btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Формирование...';
            btn.disabled = true;
            showStatus('Формирование отчета...', 'loading');
    
            const response = await fetch('/api/export/excel', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    start_date: `${startDate} 00:00:00`,
                    end_date: `${endDate} 23:59:59`
                })
            });
    
            // Проверяем тип ответа
            const contentType = response.headers.get('content-type');
            
            if (contentType.includes('application/json')) {
                // Если получили JSON (ошибка)
                const error = await response.json();
                throw new Error(error.detail || 'Ошибка сервера');
            } else if (contentType.includes('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')) {
                // Если получили Excel файл
                const blob = await response.blob();
                const url = window.URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = `report_${startDate}_${endDate}.xlsx`;
                document.body.appendChild(a);
                a.click();
                setTimeout(() => {
                    document.body.removeChild(a);
                    window.URL.revokeObjectURL(url);
                }, 100);
                
                showStatus('Отчет успешно сформирован', 'success');
            } else {
                throw new Error('Неизвестный формат ответа');
            }
        } catch (error) {
            console.error('Ошибка:', error);
            showStatus('Ошибка формирования отчета', 'error');
            showAlert(error.message || 'Ошибка при создании отчета', 'error');
        } finally {
            btn.innerHTML = originalText;
            btn.disabled = false;
        }
    });

    // 6. Обработчик для кнопки сохранения в БД
    document.getElementById('save-to-db-btn')?.addEventListener('click', async function() {
        const startDate = document.getElementById('start-date')?.value;
        const endDate = document.getElementById('end-date')?.value;
        
        if (!startDate || !endDate) {
            showAlert('Пожалуйста, укажите период анализа', 'error');
            return;
        }

        const btn = this;
        const originalText = btn.innerHTML;
        
        try {
            btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Сохранение...';
            btn.disabled = true;
            showStatus('Сохранение данных в БД...', 'loading');

            const response = await makeApiRequest('/api/save-to-db', 'POST', {
                start_date: `${startDate} 00:00:00`,
                end_date: `${endDate} 23:59:59`
            });

            const result = await response.json();
            
            showStatus('Данные сохраняются в фоне...', 'info');
            showAlert('Сохранение начато. Статус можно проверить позже.', 'success');
            
            // Функция проверки статуса
            const checkStatus = async (taskId) => {
                try {
                    const statusResponse = await fetch(`/api/task-status/${taskId}`);
                    const status = await statusResponse.json();
                    
                    showStatus(`${status.message} (${status.progress})`, 
                              status.status === 'completed' ? 'success' : 
                              status.status === 'failed' ? 