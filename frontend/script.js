document.addEventListener('DOMContentLoaded', function() {
    // Устанавливаем даты по умолчанию (последние 30 дней)
    const endDate = new Date();
    const startDate = new Date();
    startDate.setDate(endDate.getDate() - 30);
    
    document.getElementById('startDate').valueAsDate = startDate;
    document.getElementById('endDate').valueAsDate = endDate;
    
    // Обновляем дату последнего обновления
    updateLastUpdateTime();
});

function updateLastUpdateTime() {
    const now = new Date();
    const options = { 
        year: 'numeric', 
        month: 'long', 
        day: 'numeric',
        hour: '2-digit', 
        minute: '2-digit',
        second: '2-digit'
    };
    document.getElementById('lastUpdate').textContent = `Обновлено: ${now.toLocaleDateString('ru-RU', options)}`;
}

document.getElementById('apiButton').addEventListener('click', async () => {
    const button = document.getElementById('apiButton');
    const originalText = button.innerHTML;
    button.innerHTML = '<span class="loading"></span> Загрузка...';
    button.disabled = true;
    
    try {
        const startDate = document.getElementById('startDate').value;
        const endDate = document.getElementById('endDate').value;
        
        const response = await fetch(`/api/demand?startDate=${startDate}&endDate=${endDate}`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json'
            }
        });
        
        if (!response.ok) throw new Error(`Ошибка HTTP! Статус: ${response.status}`);
        
        const data = await response.json();
        document.getElementById('response').textContent = JSON.stringify(data, null, 2);
        
        // Обновляем статистику (заглушка - в реальном приложении нужно вычислять из данных)
        updateStatistics(data);
        
        updateLastUpdateTime();
        
    } catch (error) {
        console.error('Ошибка загрузки:', error);
        document.getElementById('response').textContent = 'Ошибка: ' + error.message;
    } finally {
        button.innerHTML = originalText;
        button.disabled = false;
    }
});

document.getElementById('downloadExcel').addEventListener('click', async () => {
    const button = document.getElementById('downloadExcel');
    const originalText = button.innerHTML;
    button.innerHTML = '<span class="loading"></span> Создание Excel...';
    button.disabled = true;
    
    try {
        const startDate = document.getElementById('startDate').value;
        const endDate = document.getElementById('endDate').value;
        
        const response = await fetch(`/api/export/excel?startDate=${startDate}&endDate=${endDate}`, {
            method: 'GET'
        });
        
        if (!response.ok) throw new Error(`Ошибка при создании Excel! Статус: ${response.status}`);
        
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `отгрузки_${startDate}_${endDate}.xlsx`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);
        
    } catch (error) {
        console.error('Ошибка экспорта:', error);
        alert('Ошибка при создании Excel: ' + error.message);
    } finally {
        button.innerHTML = originalText;
        button.disabled = false;
    }
});

document.getElementById('uploadToSheets').addEventListener('click', async () => {
    const button = document.getElementById('uploadToSheets');
    const originalText = button.innerHTML;
    button.innerHTML = '<span class="loading"></span> Загрузка в Google Sheets...';
    button.disabled = true;
    
    try {
        const startDate = document.getElementById('startDate').value;
        const endDate = document.getElementById('endDate').value;
        
        const response = await fetch(`/api/export/google-sheets?startDate=${startDate}&endDate=${endDate}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        });
        
        if (!response.ok) throw new Error(`Ошибка при загрузке в Google Sheets! Статус: ${response.status}`);
        
        const result = await response.json();
        alert(`Данные успешно загружены в Google Sheets! Ссылка: ${result.url}`);
        
    } catch (error) {
        console.error('Ошибка загрузки:', error);
        alert('Ошибка при загрузке в Google Sheets: ' + error.message);
    } finally {
        button.innerHTML = originalText;
        button.disabled = false;
    }
});

function updateStatistics(data) {
    // В реальном приложении здесь нужно анализировать данные и вычислять статистику
    // Это примерная реализация
    
    if (data && Array.isArray(data)) {
        document.getElementById('totalShipments').textContent = data.length;
        
        const totalAmount = data.reduce((sum, item) => sum + (item.sum || 0), 0);
        document.getElementById('totalAmount').textContent = `${totalAmount.toLocaleString('ru-RU')} ₽`;
        
        const avgCheck = data.length > 0 ? totalAmount / data.length : 0;
        document.getElementById('averageCheck').textContent = `${avgCheck.toLocaleString('ru-RU', {maximumFractionDigits: 2})} ₽`;
        
        // Находим самый популярный товар
        const products = {};
        data.forEach(order => {
            if (order.positions && Array.isArray(order.positions)) {
                order.positions.forEach(pos => {
                    if (pos.name) {
                        products[pos.name] = (products[pos.name] || 0) + (pos.quantity || 1);
                    }
                });
            }
        });
        
        let popularProduct = 'Нет данных';
        let maxCount = 0;
        for (const [name, count] of Object.entries(products)) {
            if (count > maxCount) {
                maxCount = count;
                popularProduct = name;
            }
        }
        
        document.getElementById('popularProduct').textContent = popularProduct;
    } else {
        document.getElementById('totalShipments').textContent = '-';
        document.getElementById('totalAmount').textContent = '-';
        document.getElementById('averageCheck').textContent = '-';
        document.getElementById('popularProduct').textContent = '-';
    }
}