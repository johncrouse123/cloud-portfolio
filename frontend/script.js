// Example API call (future backend integration)
async function testAPI() {
    try {
        const response = await fetch('https://lxsitpxm80.execute-api.af-south-1.amazonaws.com/dev/products');
        const data = await response.json();
        console.log('API Response:', data);
        alert('API Working: ' + JSON.stringify(data));
    } catch (error) {
        console.error('Error calling API:', error);
        alert('API Error: ' + error);
    }
}

// Optional: Run on page load
document.addEventListener('DOMContentLoaded', () => {
    console.log('Frontend loaded successfully');
});
