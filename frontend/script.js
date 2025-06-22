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
                    start_date: startDate + " 00:00:00",  // Добавляем время
                    end_date: endDate + " 23:59:59"       // Добавляем время
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
    // Добавьте обработчик для новой кнопки
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
            showStatus('Данные успешно сохранены', 'success');
            showAlert(result.message, 'success');
        } catch (error) {
            console.error('Ошибка:', error);
            showStatus('Ошибка при сохранении данных', 'error');
            showAlert(error.message, 'error');
        }
    });

    function downloadExcel(hexData, filename) {
        const bytes = new Uint8Array(hexData.match(/.{1,2}/g).map(byte => parseInt(byte, 16)));
        const blob = new Blob([bytes], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
        const url = URL.createObjectURL(blob);
        
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
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

// Пример кода для фронтенда
async function checkTaskStatus(taskId) {
    try {
        const response = await fetch(`/api/task-status/${taskId}`);
        const data = await response.json();
        
        // Обновляем UI
        updateProgressBar(data);
        
        if (data.status === 'processing' || data.status === 'fetching') {
            // Продолжаем проверять статус каждые 2 секунды
            setTimeout(() => checkTaskStatus(taskId), 2000);
        }
    } catch (error) {
        console.error('Ошибка при проверке статуса:', error);
    }
}

function updateProgressBar(taskData) {
    const progressBar = document.getElementById('progress-bar');
    const progressText = document.getElementById('progress-text');
    const statusText = document.getElementById('status-text');
    const detailsDiv = document.getElementById('task-details');
    
    // Обновляем прогресс-бар
    if (taskData.details && taskData.details.total > 0) {
        const percent = (taskData.details.processed / taskData.details.total) * 100;
        progressBar.style.width = `${percent}%`;
        progressBar.setAttribute('aria-valuenow', percent);
    }
    
    // Обновляем текст
    progressText.textContent = taskData.progress || '';
    statusText.textContent = taskData.message || '';
    
    // Показываем детали
    if (taskData.details) {
        detailsDiv.innerHTML = `
            <p>Обработано: ${taskData.details.processed}</p>
            <p>Успешно: ${taskData.details.saved}</p>
            <p>Ошибок: ${taskData.details.errors}</p>
            <p>Время выполнения: ${taskData.details.duration || '--'} сек</p>
        `;
    }
    
    // Меняем цвет в зависимости от статуса
    progressBar.className = `progress-bar ${getStatusClass(taskData.status)}`;
}

function getStatusClass(status) {
    switch(status) {
        case 'completed': return 'bg-success';
        case 'failed': return 'bg-danger';
        case 'processing': return 'bg-info progress-bar-striped progress-bar-animated';
        default: return 'bg-secondary';
    }
}