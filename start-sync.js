const { spawn } = require('child_process');
const os = require('os');

console.clear();
console.log('================================================================');
console.log('       🚀 KRISHAN POS - 100% FREE REALTIME MULTI-DEVICE SYNC    ');
console.log('          (Zero Monthly Cost - 100% Free Forever)               ');
console.log('================================================================\n');

// 1. Get Local LAN IP
const nets = os.networkInterfaces();
let localIp = '127.0.0.1';
for (const name of Object.keys(nets)) {
    for (const net of nets[name]) {
        if (net.family === 'IPv4' && !net.internal) {
            localIp = net.address;
            break;
        }
    }
}

// 2. Fetch Public IP for Localtunnel Password
(async () => {
    let publicIp = '';
    try {
        const res = await fetch('https://api.ipify.org');
        if (res.ok) publicIp = (await res.text()).trim();
    } catch(e) {}

    console.log('📶 [1] SHOP WI-FI / LOCAL NETWORK ACCESS:');
    console.log(`     👉 http://${localIp}:3000`);
    console.log('     (Open on any Phone/Tablet/Laptop connected to Shop Wi-Fi for 0-latency instant sync)\n');

    console.log('🌐 [2] WORLDWIDE INTERNET LINK (4G/5G / Home / Anywhere):');
    console.log('     Generating your Free Secure HTTPS Public Link below...');
    if (publicIp) {
        console.log(`     💡 Tunnel Password (if asked on first open): ${publicIp}`);
    }
    console.log('----------------------------------------------------------------\n');

    const subdomain = 'krishan-pos-' + Math.floor(1000 + Math.random() * 9000);
    const lt = spawn('npx.cmd', ['-y', 'localtunnel', '--port', '3000', '--subdomain', subdomain], { stdio: 'inherit', shell: true });

    lt.on('error', (err) => {
        console.error('Tunnel launch error:', err.message);
    });
})();
