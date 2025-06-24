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
            
            console.log('1. Отправляем запрос...');
            
            const response = await fetch('/api/export/excel', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    start_date: startDate + " 00:00:00",
                    end_date: endDate + " 23:59:59"
                })
            });

            console.log('2. Получен ответ:', response);
            console.log('   Status:', response.status);
            console.log('   Headers:', Object.fromEntries(response.headers.entries()));

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(errorText || 'Ошибка при загрузке файла');
            }

            console.log('3. Получаем blob...');
            const blob = await response.blob();
            console.log('4. Blob получен:', blob);
            console.log('   Size:', blob.size);
            console.log('   Type:', blob.type);

            if (blob.size === 0) {
                throw new Error('Получен пустой файл');
            }

            console.log('5. Создаем ссылку для скачивания...');
            const url = window.URL.createObjectURL(blob);
            console.log('6. URL создан:', url);

            const a = document.createElement('a');
            a.href = url;
            a.download = `report_${startDate}_to_${endDate}.xlsx`;
            a.style.display = 'none';
            
            console.log('7. Добавляем ссылку в DOM и кликаем...');
            document.body.appendChild(a);
            
            // Используем setTimeout для гарантии, что элемент добавлен в DOM
            setTimeout(() => {
                a.click();
                console.log('8. Клик выполнен');
                
                // Очищаем через некоторое время
                setTimeout(() => {
                    window.URL.revokeObjectURL(url);
                    document.body.removeChild(a);
                    console.log('9. Очистка выполнена');
                }, 100);
            }, 0);
            
            showStatus('Данные успешно загружены', 'success');
            showAlert('Excel файл успешно сформирован', 'success');
        } catch (error) {
            console.error('ОШИБКА:', error);
            showStatus('Ошибка при загрузке данных', 'error');
            showAlert(error.message || 'Произошла ошибка при загрузке файла', 'error');
        }
    });

    function showStatus(message, type) {
        const statusBar = document.getElementById('status-bar');
        if (statusBar) {
            statusBar.innerHTML = `<i class="fas fa-${type === 'success' ? 'check-circle' : type === 'error' ? 'times-circle' : 'spinner fa-spin'}"></i> ${message}`;
            statusBar.className = `status-bar show ${type}`;
        }
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