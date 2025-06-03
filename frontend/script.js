document.getElementById('apiButton').addEventListener('click', async () => {
    try {
        const response = await fetch('http://45.12.230.148:5000/api/demand', {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json'
            }
        });
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        document.getElementById('response').textContent = JSON.stringify(data, null, 2);
        
        // Скачивание файла
        const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'text/plain' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'moysklad_data.txt';
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
        
    } catch (error) {
        console.error('Error:', error);
        document.getElementById('response').textContent = `Error: ${error.message}`;
    }
});