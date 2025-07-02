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
            // Проверяем тип содержимого
            const contentType = response.headers.get('content-type');
            if (!contentType.includes('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')) {
                throw new Error(`Неверный тип файла: ${contentType}`);
            }

            // Получаем имя файла из заголовков
            const contentDisposition = response.headers.get('content-disposition');
            let filename = defaultFilename;
            
            if (contentDisposition) {
                const match = contentDisposition.match(/filename="?([^"]+)"?/);
                if (match) filename = match[1];
            }

            // Создаем Blob и ссылку для скачивания
            const blob = await response.blob();
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = filename;
            document.body.appendChild(a);
            a.click();
            
            // Очистка
            setTimeout(() => {
                document.body.removeChild(a);
                URL.revokeObjectURL(url);
            }, 100);

            return true;
        } catch (error) {
            console.error('Download Error:', error);
            throw error;
        }
    };

    // 5. Обработчик для кнопки Excel
    document.getElementById('export-excel-btn')?.addEventListener('click', async function() {
        const startDate = document.getElementById('start-date')?.value;
        const endDate = document.getElementById('end-date')?.value;
        
        if (!startDate || !endDate) {
            showAlert('Пожалуйста, укажите период анализа', 'error');
            return;
        }

        const btn = this;
        const originalText = btn.innerHTML;
        
        try {
            // Показываем статус загрузки
            btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Формирование...';
            btn.disabled = true;
            showStatus('Формирование Excel отчета...', 'loading');

            // Отправляем запрос
            const response = await makeApiRequest('/api/export/excel', 'POST', {
                start_date: `${startDate} 00:00:00`,
                end_date: `${endDate} 23:59:59`
            });

            // Скачиваем файл
            await downloadFile(response);
            
            showStatus('Excel файл успешно сформирован', 'success');
            showAlert('Отчет готов к скачиванию', 'success');
            
        } catch (error) {
            console.error('Excel Export Error:', error);
            showStatus('Ошибка при формировании отчета', 'error');
            showAlert(error.message || 'Ошибка при создании Excel файла', 'error');
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
                              status.status === 'failed' ? 'error' : 'info');
                    
                    if (status.status !== 'completed' && status.status !== 'failed') {
                        setTimeout(() => checkStatus(taskId), 2000);
                    }
                } catch (e) {
                    console.error('Status Check Error:', e);
                }
            };
            
            checkStatus(result.task_id);
            
        } catch (error) {
            console.error('DB Save Error:', error);
            showStatus('Ошибка при сохранении данных', 'error');
            showAlert(error.message || 'Ошибка при сохранении в БД', 'error');
        } finally {
            btn.innerHTML = originalText;
            btn.disabled = false;
        }
    });

    // 7. Обработчик для кнопки Google Sheets
    document.getElementById('export-gsheet-btn')?.addEventListener('click', async function() {
        const startDate = document.getElementById('start-date')?.value;
        const endDate = document.getElementById('end-date')?.value;
        
        if (!startDate || !endDate) {
            showAlert('Пожалуйста, укажите период анализа', 'error');
            return;
        }

        const btn = this;
        const originalText = btn.innerHTML;
        
        try {
            btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Создание...';
            btn.disabled = true;
            showStatus('Создание Google таблицы...', 'loading');

            const response = await makeApiRequest('/api/export/gsheet', 'POST', {
                start_date: `${startDate} 00:00:00`,
                end_date: `${endDate} 23:59:59`
            });

            const result = await response.json();
            
            if (result.url) {
                window.open(result.url, '_blank');
                showStatus('Таблица создана', 'success');
                showAlert('Google таблица успешно создана', 'success');
            } else {
                throw new Error('Не получена ссылка на таблицу');
            }
            
        } catch (error) {
            console.error('Google Sheets Error:', error);
            showStatus('Ошибка при создании таблицы', 'error');
            showAlert(error.message || 'Ошибка при создании Google таблицы', 'error');
        } finally {
            btn.innerHTML = originalText;
            btn.disabled = false;
        }
    });
});