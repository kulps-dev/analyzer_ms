document.addEventListener('DOMContentLoaded', function() {
    // Инициализация datepicker
    flatpickr(".datepicker", {
        dateFormat: "Y-m-d",
        locale: "ru"
    });

    // Функция для показа статуса
    function showStatus(message, type) {
        const statusBar = document.getElementById('status-bar');
        statusBar.innerHTML = `<i class="fas fa-${type === 'success' ? 'check-circle' : type === 'error' ? 'times-circle' : 'spinner'}"></i> ${message}`;
        statusBar.className = 'status-bar show ' + type;
    }

    // Функция для показа алерта
    function showAlert(message, type) {
        const alert = document.createElement('div');
        alert.className = `custom-alert ${type} show`;
        alert.innerHTML = `<i class="fas fa-${type === 'success' ? 'check-circle' : 'exclamation-circle'}"></i> ${message}`;
        document.body.appendChild(alert);
        
        setTimeout(() => {
            alert.classList.remove('show');
            setTimeout(() => alert.remove(), 500);
        }, 3000);
    }

    // Функция для скачивания Excel
    function downloadExcel(hexData, filename) {
        const bytes = new Uint8Array(hexData.match(/.{1,2}/g).map(byte => parseInt(byte, 16)));
        const blob = new Blob([bytes], {type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'});
        const link = document.createElement('a');
        link.href = URL.createObjectURL(blob);
        link.download = filename;
        link.click();
    }

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
                throw new Error(await response.text());
            }

            const result = await response.json();
            downloadExcel(result.file, result.filename);
            
            showStatus('Данные успешно загружены', 'success');
            showAlert('Excel файл успешно сформирован', 'success');
        } catch (error) {
            console.error('Ошибка:', error);
            showStatus('Ошибка при загрузке данных', 'error');
            showAlert(error.message, 'error');
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
            showStatus('Сохранение данных в БД...', 'loading');
            
            const response = await fetch('/api/save-to-db', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    start_date: startDate + " 00:00:00",
                    end_date: endDate + " 23:59:59"
                })
            });

            if (!response.ok) {
                throw new Error(await response.text());
            }

            const result = await response.json();
            showStatus('Данные сохраняются в фоне...', 'info');
            showAlert('Начато сохранение данных в БД. Проверьте статус позже.', 'success');
            
            // Проверка статуса задачи
            checkTaskStatus(result.task_id);
        } catch (error) {
            console.error('Ошибка:', error);
            showStatus('Ошибка при сохранении данных', 'error');
            showAlert(error.message, 'error');
        }
    });

    // Обработчик кнопки "Отправить в Google Sheets"
    document.getElementById('export-gsheet-btn').addEventListener('click', async function() {
        const startDate = document.getElementById('start-date').value;
        const endDate = document.getElementById('end-date').value;
        
        if (!startDate || !endDate) {
            showAlert('Пожалуйста, укажите период анализа', 'error');
            return;
        }
    
        try {
            // Показываем загрузку
            const btn = this;
            btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Создание...';
            btn.disabled = true;
            
            showStatus('Создание Google таблицы...', 'loading');
            console.log('Отправка запроса на /api/export/gsheet'); // Логирование
    
            const response = await fetch('/api/export/gsheet', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    start_date: startDate + " 00:00:00",
                    end_date: endDate + " 23:59:59"
                })
            });
    
            console.log('Получен ответ:', response); // Логирование
    
            if (!response.ok) {
                const error = await response.text();
                throw new Error(error || 'Ошибка сервера');
            }
    
            const result = await response.json();
            console.log('Результат:', result); // Логирование
            
            if (result.url) {
                window.open(result.url, '_blank');
                showAlert('Таблица создана: ' + result.url, 'success');
            } else {
                throw new Error('Не получена ссылка на таблицу');
            }
            
            showStatus('Готово', 'success');
        } catch (error) {
            console.error('Ошибка экспорта:', error);
            showStatus('Ошибка', 'error');
            showAlert(error.message, 'error');
        } finally {
            // Восстанавливаем кнопку
            btn.innerHTML = '<i class="fab fa-google"></i> Отправить в Google Sheets';
            btn.disabled = false;
        }
    });

    // Функция для проверки статуса задачи
    function checkTaskStatus(taskId) {
        const interval = setInterval(async () => {
            try {
                const response = await fetch(`/api/task-status/${taskId}`);
                if (!response.ok) {
                    clearInterval(interval);
                    throw new Error(await response.text());
                }

                const status = await response.json();
                showStatus(`${status.message} (${status.progress})`, 
                          status.status === 'completed' ? 'success' : 
                          status.status === 'failed' ? 'error' : 'loading');

                if (status.status === 'completed' || status.status === 'failed') {
                    clearInterval(interval);
                    if (status.status === 'completed') {
                        showAlert('Данные успешно сохранены в БД', 'success');
                    } else {
                        showAlert('Ошибка при сохранении данных в БД', 'error');
                    }
                }
            } catch (error) {
                clearInterval(interval);
                console.error('Ошибка проверки статуса:', error);
                showStatus('Ошибка при проверке статуса', 'error');
            }
        }, 2000);
    }
});

// Вебхук переключатель
document.getElementById('webhook-toggle').addEventListener('change', function() {
    const webhookUrlContainer = document.getElementById('webhook-url-container');
    const webhookStatus = document.getElementById('webhook-status');
    
    if (this.checked) {
        webhookStatus.textContent = 'Вебхук включен';
        webhookStatus.style.color = 'var(--success)';
        webhookUrlContainer.style.display = 'block';
        
        // Можно добавить сохранение состояния в localStorage
        localStorage.setItem('webhookEnabled', 'true');
    } else {
        webhookStatus.textContent = 'Вебхук выключен';
        webhookStatus.style.color = 'var(--text)';
        webhookUrlContainer.style.display = 'none';
        
        localStorage.setItem('webhookEnabled', 'false');
    }
});

// Проверка сохраненного состояния при загрузке
document.addEventListener('DOMContentLoaded', function() {
    const webhookToggle = document.getElementById('webhook-toggle');
    const webhookEnabled = localStorage.getItem('webhookEnabled') === 'true';
    
    if (webhookEnabled) {
        webhookToggle.checked = true;
        webhookToggle.dispatchEvent(new Event('change'));
    }
    
    // Можно добавить сохранение URL вебхука
    const savedUrl = localStorage.getItem('webhookUrl');
    if (savedUrl) {
        document.getElementById('webhook-url').value = savedUrl;
    }
    
    // Сохранение URL при изменении
    document.getElementById('webhook-url').addEventListener('change', function() {
        localStorage.setItem('webhookUrl', this.value);
    });
});