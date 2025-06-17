// scripts/moysklad.js
document.addEventListener('DOMContentLoaded', function() {
    // Инициализация Flatpickr для выбора дат
    flatpickr(".datepicker", {
        locale: "ru",
        dateFormat: "d.m.Y",
        defaultDate: new Date(),
        maxDate: new Date()
    });

    // Установка дат по умолчанию (последние 30 дней)
    const endDate = new Date();
    const startDate = new Date();
    startDate.setDate(endDate.getDate() - 30);
    
    // Форматирование дат для flatpickr
    document.getElementById('start-date')._flatpickr.setDate(startDate);
    document.getElementById('end-date')._flatpickr.setDate(endDate);
    
    // Инициализация статус-бара
    const statusBar = document.getElementById('status-bar');
    
    // Функция для обновления статус-бара
    function updateStatus(message, type = 'info') {
        statusBar.innerHTML = `<i class="fas fa-${type === 'success' ? 'check-circle' : type === 'error' ? 'times-circle' : type === 'loading' ? 'spinner fa-pulse' : 'info-circle'}"></i> ${message}`;
        statusBar.className = 'status-bar show ' + type;
        
        // Автоматическое скрытие через 5 секунд, если не loading
        if (type !== 'loading') {
            setTimeout(() => {
                statusBar.classList.remove('show');
            }, 5000);
        }
    }

    // Обработчик для кнопки экспорта в Excel
    document.getElementById('export-excel-btn').addEventListener('click', async () => {
        const button = document.getElementById('export-excel-btn');
        const originalText = button.innerHTML;
        button.innerHTML = '<i class="fas fa-spinner fa-pulse"></i> Создание Excel...';
        button.disabled = true;
        
        updateStatus('Создание Excel файла...', 'loading');
        
        try {
            const startDate = document.getElementById('start-date').value;
            const endDate = document.getElementById('end-date').value;
            const project = document.getElementById('project-filter').value;
            const channel = document.getElementById('channel-filter').value;
            
            const response = await fetch('/api/export/excel', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    start_date: startDate,
                    end_date: endDate,
                    project: project || null,
                    channel: channel || null
                })
            });
            
            if (!response.ok) {
                const error = await response.json();
                throw new Error(error.detail || 'Ошибка сервера');
            }
            
            const result = await response.json();
            
            // Создаем Excel файл на клиенте
            const workbook = XLSX.utils.book_new();
            const worksheet = XLSX.utils.json_to_sheet(result.data);
            XLSX.utils.book_append_sheet(workbook, worksheet, "Отгрузки");
            XLSX.writeFile(workbook, result.filename);
            
            updateStatus('Excel файл успешно создан и скачан!', 'success');
        } catch (error) {
            console.error('Ошибка экспорта:', error);
            updateStatus(`Ошибка: ${error.message}`, 'error');
        } finally {
            button.innerHTML = originalText;
            button.disabled = false;
        }
    });

    // Обработчик для кнопки экспорта в Google Sheets
    document.getElementById('export-gsheet-btn').addEventListener('click', async () => {
        const button = document.getElementById('export-gsheet-btn');
        const originalText = button.innerHTML;
        button.innerHTML = '<i class="fas fa-spinner fa-pulse"></i> Отправка в Google Sheets...';
        button.disabled = true;
        
        updateStatus('Отправка данных в Google Sheets...', 'loading');
        
        try {
            const startDate = document.getElementById('start-date').value;
            const endDate = document.getElementById('end-date').value;
            const project = document.getElementById('project-filter').value;
            const channel = document.getElementById('channel-filter').value;
            
            const response = await fetch('/api/export/google-sheets', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    start_date: startDate,
                    end_date: endDate,
                    project: project || null,
                    channel: channel || null
                })
            });
            
            if (!response.ok) {
                const error = await response.json();
                throw new Error(error.detail || 'Ошибка сервера');
            }
            
            const result = await response.json();
            
            updateStatus(`Данные успешно отправлены в <a href="${result.url}" target="_blank" style="color: white; text-decoration: underline;">Google Sheets</a>`, 'success');
            showAlert('Данные успешно загружены в Google Sheets!', 'success');
        } catch (error) {
            console.error('Ошибка загрузки:', error);
            updateStatus(`Ошибка: ${error.message}`, 'error');
            showAlert(`Ошибка: ${error.message}`, 'error');
        } finally {
            button.innerHTML = originalText;
            button.disabled = false;
        }
    });

    // Функция для показа всплывающих уведомлений
    function showAlert(message, type = 'info') {
        const alert = document.createElement('div');
        alert.className = `custom-alert ${type}`;
        alert.innerHTML = `<i class="fas fa-${type === 'success' ? 'check-circle' : type === 'error' ? 'times-circle' : 'info-circle'}"></i> ${message}`;
        document.body.appendChild(alert);
        
        setTimeout(() => {
            alert.classList.add('show');
        }, 10);
        
        setTimeout(() => {
            alert.classList.remove('show');
            setTimeout(() => {
                document.body.removeChild(alert);
            }, 500);
        }, 5000);
    }
});