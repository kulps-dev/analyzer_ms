document.addEventListener('DOMContentLoaded', function() {
    // Инициализация datepicker
    flatpickr(".datepicker", {
        dateFormat: "Y-m-d",
        locale: "ru"
    });

    // Обработчик кнопки "Скачать Excel"
    document.getElementById('export-excel-btn').addEventListener('click', async function() {
        const startDate = document.getElementById('start-date').value;
        const endDate = document.getElementById('end-date').value;
        
        if (!startDate || !endDate) {
            showAlert('Пожалуйста, укажите период анализа', 'error');
            return;
        }

        try {
            showStatus('Загрузка данных...', 'loading');
            
            const response = await fetch('/api/export/excel', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    start_date: startDate + " 00:00:00",
                    end_date: endDate + " 23:59:59"
                })
            });

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(errorText || 'Ошибка при загрузке файла');
            }

            // Получаем blob напрямую из ответа
            const blob = await response.blob();
            
            // Получаем имя файла из заголовков, если есть
            const contentDisposition = response.headers.get('Content-Disposition');
            let filename = `report_${startDate}_to_${endDate}.xlsx`;
            
            if (contentDisposition) {
                const filenameMatch = contentDisposition.match(/filename="?(.+)"?/i);
                if (filenameMatch && filenameMatch[1]) {
                    filename = filenameMatch[1];
                }
            }
            
            // Создаем ссылку для скачивания
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = filename;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
            
            showStatus('Данные успешно загружены', 'success');
            showAlert('Excel файл успешно сформирован', 'success');
        } catch (error) {
            console.error('Ошибка:', error);
            showStatus('Ошибка при загрузке данных', 'error');
            showAlert(error.message || 'Произошла ошибка при загрузке файла', 'error');
        }
    });

    // Обработчик кнопки "Сохранить в БД"
    document.getElementById('save-to-db-btn').addEventListener('click', async function() {
        const startDate = document.getElementById('start-date').value;
        const endDate = document.getElementById('end-date').value;
        
        if (!startDate || !endDate) {
            showAlert('Пожалуйста, укажите период анализа', 'error');
            return;
        }

        try {
            showStatus('Запуск обработки данных...', 'loading');
            
            const response = await fetch('/api/save-to-db', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    start_date: startDate + " 00:00:00",
                    end_date: endDate + " 23:59:59"
                })
            });

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(errorText || 'Ошибка при сохранении данных');
            }

            const result = await response.json();
            
            if (result.task_id) {
                // Начинаем проверку статуса задачи
                showStatus('Обработка данных запущена...', 'loading');
                checkTaskStatus(result.task_id);
            } else {
                showStatus('Данные успешно сохранены', 'success');
                showAlert(result.message, 'success');
            }
        } catch (error) {
            console.error('Ошибка:', error);
            showStatus('Ошибка при сохранении данных', 'error');
            showAlert(error.message || 'Произошла ошибка', 'error');
        }
    });

    // Функция для проверки статуса задачи
    async function checkTaskStatus(taskId) {
        const intervalId = setInterval(async () => {
            try {
                const response = await fetch(`/api/task-status/${taskId}`);
                const status = await response.json();
                
                if (status.status === 'completed') {
                    clearInterval(intervalId);
                    showStatus('Данные успешно сохранены', 'success');
                    showAlert(status.message, 'success');
                } else if (status.status === 'failed') {
                    clearInterval(intervalId);
                    showStatus('Ошибка при обработке данных', 'error');
                    showAlert(status.message, 'error');
                } else if (status.status === 'processing' || status.status === 'fetching') {
                    showStatus(`${status.message} (${status.progress})`, 'loading');
                }
            } catch (error) {
                clearInterval(intervalId);
                showStatus('Ошибка при проверке статуса', 'error');
                showAlert('Не удалось проверить статус задачи', 'error');
            }
        }, 2000); // Проверяем каждые 2 секунды
    }

    function showStatus(message, type) {
        const statusBar = document.getElementById('status-bar');
        statusBar.innerHTML = `<i class="fas fa-${type === 'success' ? 'check-circle' : type === 'error' ? 'times-circle' : 'spinner fa-spin'}"></i> ${message}`;
        statusBar.className = `status-bar show ${type}`;
    }

    function showAlert(message, type) {
        const alert = document.createElement('div');
        alert.className = `custom-alert ${type}`;
        alert.innerHTML = `<i class="fas fa-${type === 'success' ? 'check-circle' : 'exclamation-circle'}"></i> ${message}`;
        document.body.appendChild(alert);
        
        setTimeout(() => alert.classList.add('show'), 10);
        setTimeout(() => {
            alert.classList.remove('show');
            setTimeout(() => document.body.removeChild(alert), 500);
        }, 3000);
    }
});