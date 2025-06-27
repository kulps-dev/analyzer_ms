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
            showStatus('Ошибка при сохранении данных