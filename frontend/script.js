document.getElementById('apiButton').addEventListener('click', async () => {
    try {
        const response = await fetch('/api/demand', {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json'
            }
        });
        
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        
        const data = await response.json();
        document.getElementById('response').textContent = JSON.stringify(data, null, 2);
        
    } catch (error) {
        console.error('Fetch error:', error);
        document.getElementById('response').textContent = 'Error: ' + error.message;
    }
});

document.getElementById('processedDataButton').addEventListener('click', async () => {
    try {
        const response = await fetch('/api/processed-data', {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json'
            }
        });
        
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        
        const data = await response.json();
        document.getElementById('processedResponse').textContent = JSON.stringify(data, null, 2);
        
    } catch (error) {
        console.error('Fetch error:', error);
        document.getElementById('processedResponse').textContent = 'Error: ' + error.message;
    }
});