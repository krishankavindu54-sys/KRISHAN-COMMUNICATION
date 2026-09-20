const express = require('express');
const http = require('http');
const path = require('path');
const cors = require('cors');
const cookieParser = require('cookie-parser');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');
const { Server } = require('socket.io');
require('dotenv').config();

const { dbService } = require('./database');

const app = express();
const server = http.createServer(app);

const isServerless = Boolean(process.env.VERCEL || process.env.AWS_LAMBDA_FUNCTION_NAME || process.env.LAMBDA_TASK_ROOT);

// Setup Socket.io for persistent connections (Local / VPS / Render)
let io;
if (!isServerless) {
    io = new Server(server, {
        cors: {
            origin: true,
            credentials: true
        }
    });

    let connectedClients = 0;

    io.on('connection', (socket) => {
        connectedClients++;
        console.log(`⚡ [Socket.io] Client connected (ID: ${socket.id}). Total active devices: ${connectedClients}`);

        socket.emit('sync:welcome', {
            deviceCount: connectedClients,
            timestamp: new Date().toISOString(),
            serverTime: Date.now()
        });

        io.emit('sync:device_count', { count: connectedClients });

        socket.on('disconnect', () => {
            connectedClients = Math.max(0, connectedClients - 1);
            console.log(`🔌 [Socket.io] Client disconnected (ID: ${socket.id}). Remaining: ${connectedClients}`);
            io.emit('sync:device_count', { count: connectedClients });
        });
    });
}

function broadcastSync(type, data, excludeSocketId = null) {
    if (!io) return;
    const payload = {
        type,
        data,
        timestamp: new Date().toISOString()
    };
    if (excludeSocketId) {
        io.except(excludeSocketId).emit('sync:event', payload);
    } else {
        io.emit('sync:event', payload);
    }
}

const JWT_SECRET = process.env.JWT_SECRET || 'krishan_pos_secure_studio_jwt_secret_2026';
const SESSION_DURATION = 12 * 60 * 60 * 1000;
const PORT = process.env.PORT || 3000;

app.use(cors({ origin: true, credentials: true }));
app.use(express.json({ limit: '10mb' }));
app.use(cookieParser());
app.use(express.static(__dirname));

// JWT Helpers
function createToken(user) {
    return jwt.sign(
        { id: user.id, username: user.username, role: user.role, name: user.name },
        JWT_SECRET,
        { expiresIn: '12h' }
    );
}

function verifyToken(req) {
    const token = req.cookies?.pos_token || req.headers.authorization?.replace('Bearer ', '');
    if (!token) return null;
    try {
        return jwt.verify(token, JWT_SECRET);
    } catch (err) {
        return null;
    }
}

// Middleware
function requireAuth(req, res, next) {
    const user = verifyToken(req);
    if (!user) {
        return res.status(401).json({ success: false, message: 'Unauthorized. Please log in.' });
    }
    req.user = user;
    next();
}

function requireAdmin(req, res, next) {
    const user = verifyToken(req);
    if (!user || user.role !== 'admin') {
        return res.status(403).json({ success: false, message: 'Forbidden. Admin access required.' });
    }
    req.user = user;
    next();
}

function getSocketId(req) {
    return req.headers['x-socket-id'] || null;
}

// ──────────────────────────────────────────────
// AUTHENTICATION ROUTES
// ──────────────────────────────────────────────

app.post('/api/auth/login', async (req, res) => {
    try {
        const { username, password } = req.body || {};
        if (!username || !password) {
            return res.status(400).json({ success: false, message: 'Username and password are required.' });
        }

        const user = await dbService.getUserByUsername(username.trim());
        if (!user) {
            return res.status(401).json({ success: false, message: 'Invalid username or password.' });
        }

        const isMatch = await bcrypt.compare(password, user.password_hash);
        if (!isMatch) {
            return res.status(401).json({ success: false, message: 'Invalid username or password.' });
        }

        const payload = { id: user.id, username: user.username, role: user.role, name: user.name };
        const token = createToken(payload);

        const isHttps = Boolean(req.secure || req.headers['x-forwarded-proto'] === 'https' || isServerless);
        res.cookie('pos_token', token, {
            httpOnly: true,
            sameSite: isHttps ? 'none' : 'lax',
            secure: isHttps,
            maxAge: SESSION_DURATION
        });

        return res.json({
            success: true,
            message: 'Login successful',
            user: payload,
            token
        });
    } catch (error) {
        console.error('Login error:', error);
        return res.status(500).json({ success: false, message: 'Server error during login.' });
    }
});

app.post('/api/auth/logout', (req, res) => {
    res.clearCookie('pos_token');
    res.json({ success: true, message: 'Logged out successfully.' });
});

app.get('/api/auth/me', (req, res) => {
    const user = verifyToken(req);
    if (!user) {
        return res.status(401).json({ authenticated: false, message: 'No active session' });
    }
    return res.json({ authenticated: true, user });
});

app.post('/api/auth/change-password', requireAuth, async (req, res) => {
    try {
        const { currentPassword, newPassword } = req.body;
        if (!newPassword || newPassword.length < 4) {
            return res.status(400).json({ success: false, message: 'Password must be at least 4 characters.' });
        }

        const user = await dbService.getUserByUsername(req.user.username);
        if (!user) {
            return res.status(404).json({ success: false, message: 'User not found.' });
        }

        const isMatch = await bcrypt.compare(currentPassword, user.password_hash);
        if (!isMatch) {
            return res.status(400).json({ success: false, message: 'Current password is incorrect.' });
        }

        await dbService.updateUser(user.id, { password: newPassword });
        res.json({ success: true, message: 'Password updated successfully.' });
    } catch (err) {
        res.status(500).json({ success: false, message: 'Error updating password.' });
    }
});

// ──────────────────────────────────────────────
// USER MANAGEMENT (Admin Only)
// ──────────────────────────────────────────────

app.get('/api/users', requireAdmin, async (req, res) => {
    try {
        const users = await dbService.getUsers();
        res.json({ success: true, users });
    } catch (e) {
        res.status(500).json({ success: false, message: e.message });
    }
});

app.post('/api/users', requireAdmin, async (req, res) => {
    try {
        const { username, password, role, name } = req.body;
        if (!username || !password || !name) {
            return res.status(400).json({ success: false, message: 'Username, password, and name are required.' });
        }

        const existing = await dbService.getUserByUsername(username);
        if (existing) {
            return res.status(400).json({ success: false, message: 'Username already exists.' });
        }

        const newUser = await dbService.createUser({ username, password, role: role || 'cashier', name });
        res.json({ success: true, user: newUser });
    } catch (e) {
        res.status(500).json({ success: false, message: e.message });
    }
});

app.put('/api/users/:id', requireAdmin, async (req, res) => {
    try {
        const { name, role, password } = req.body;
        const updated = await dbService.updateUser(req.params.id, { name, role, password });
        if (!updated) return res.status(404).json({ success: false, message: 'User not found.' });
        res.json({ success: true, user: updated });
    } catch (e) {
        res.status(500).json({ success: false, message: e.message });
    }
});

app.delete('/api/users/:id', requireAdmin, async (req, res) => {
    try {
        const userId = Number(req.params.id);
        if (req.user.id === userId) {
            return res.status(400).json({ success: false, message: 'You cannot delete your own account.' });
        }
        await dbService.deleteUser(userId);
        res.json({ success: true, message: 'User deleted.' });
    } catch (e) {
        res.status(500).json({ success: false, message: e.message });
    }
});

// ──────────────────────────────────────────────
// POS DATA CRUD ENDPOINTS (WITH REALTIME SYNC)
// ──────────────────────────────────────────────

// Items / Inventory
app.get('/api/items', requireAuth, async (req, res) => {
    try {
        const items = await dbService.getItems();
        res.json(items);
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

app.post('/api/items', requireAuth, async (req, res) => {
    try {
        const item = await dbService.createItem(req.body);
        broadcastSync('ITEM_CREATED', item, getSocketId(req));
        res.json(item);
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

app.put('/api/items/:id', requireAuth, async (req, res) => {
    try {
        const item = await dbService.updateItem(req.params.id, req.body);
        if (!item) return res.status(404).json({ message: 'Item not found' });
        broadcastSync('ITEM_UPDATED', item, getSocketId(req));
        res.json(item);
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

app.post('/api/items/:id/adjust-stock', requireAuth, async (req, res) => {
    try {
        const delta = Number(req.body.delta) || 0;
        const item = await dbService.adjustItemStock(req.params.id, delta);
        if (!item) return res.status(404).json({ message: 'Item not found' });
        broadcastSync('STOCK_CHANGED', { id: Number(req.params.id), stock: item.stock, delta, item }, getSocketId(req));
        res.json(item);
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

app.delete('/api/items/:id', requireAuth, async (req, res) => {
    try {
        const id = Number(req.params.id);
        await dbService.deleteItem(id);
        broadcastSync('ITEM_DELETED', { id }, getSocketId(req));
        res.json({ success: true });
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

// Sales
app.get('/api/sales', requireAuth, async (req, res) => {
    try {
        const sales = await dbService.getSales();
        res.json(sales);
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

app.post('/api/sales', requireAuth, async (req, res) => {
    try {
        const saleData = { ...req.body, userId: req.user.id };
        const sale = await dbService.createSale(saleData);
        const allItems = await dbService.getItems();
        
        broadcastSync('SALE_CREATED', {
            sale,
            items: allItems,
            cashier: req.user.name || req.user.username
        }, getSocketId(req));
        
        res.json(sale);
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

app.delete('/api/sales/:id', requireAuth, async (req, res) => {
    try {
        const id = Number(req.params.id);
        await dbService.deleteSale(id);
        broadcastSync('SALE_DELETED', { id }, getSocketId(req));
        res.json({ success: true });
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

// Repairs
app.get('/api/repairs', requireAuth, async (req, res) => {
    try {
        const repairs = await dbService.getRepairs();
        res.json(repairs);
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

app.post('/api/repairs', requireAuth, async (req, res) => {
    try {
        const repair = await dbService.createRepair(req.body);
        broadcastSync('REPAIR_CREATED', repair, getSocketId(req));
        res.json(repair);
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

app.put('/api/repairs/:id', requireAuth, async (req, res) => {
    try {
        const repair = await dbService.updateRepair(req.params.id, req.body);
        if (!repair) return res.status(404).json({ message: 'Repair not found' });
        broadcastSync('REPAIR_UPDATED', repair, getSocketId(req));
        res.json(repair);
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

app.delete('/api/repairs/:id', requireAuth, async (req, res) => {
    try {
        const id = Number(req.params.id);
        await dbService.deleteRepair(id);
        broadcastSync('REPAIR_DELETED', { id }, getSocketId(req));
        res.json({ success: true });
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

// Expenses
app.get('/api/expenses', requireAuth, async (req, res) => {
    try {
        const expenses = await dbService.getExpenses();
        res.json(expenses);
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

app.post('/api/expenses', requireAuth, async (req, res) => {
    try {
        const expense = await dbService.createExpense(req.body);
        broadcastSync('EXPENSE_CREATED', expense, getSocketId(req));
        res.json(expense);
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

app.delete('/api/expenses/:id', requireAuth, async (req, res) => {
    try {
        const id = Number(req.params.id);
        await dbService.deleteExpense(id);
        broadcastSync('EXPENSE_DELETED', { id }, getSocketId(req));
        res.json({ success: true });
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

// Creditors
app.get('/api/creditors', requireAuth, async (req, res) => {
    try {
        const creditors = await dbService.getCreditors();
        res.json(creditors);
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

app.post('/api/creditors', requireAuth, async (req, res) => {
    try {
        const creditor = await dbService.createCreditor(req.body);
        broadcastSync('CREDITOR_CREATED', creditor, getSocketId(req));
        res.json(creditor);
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

app.put('/api/creditors/:id', requireAuth, async (req, res) => {
    try {
        const creditor = await dbService.updateCreditor(req.params.id, req.body);
        if (!creditor) return res.status(404).json({ message: 'Creditor not found' });
        broadcastSync('CREDITOR_UPDATED', creditor, getSocketId(req));
        res.json(creditor);
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

app.delete('/api/creditors/:id', requireAuth, async (req, res) => {
    try {
        const id = Number(req.params.id);
        await dbService.deleteCreditor(id);
        broadcastSync('CREDITOR_DELETED', { id }, getSocketId(req));
        res.json({ success: true });
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

// Bank Transactions
app.get('/api/bank-transactions', requireAuth, async (req, res) => {
    try {
        const txs = await dbService.getBankTransactions();
        res.json(txs);
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

app.post('/api/bank-transactions', requireAuth, async (req, res) => {
    try {
        const tx = await dbService.createBankTransaction(req.body);
        broadcastSync('BANK_TX_CREATED', tx, getSocketId(req));
        res.json(tx);
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

app.delete('/api/bank-transactions/:id', requireAuth, async (req, res) => {
    try {
        const id = Number(req.params.id);
        await dbService.deleteBankTransaction(id);
        broadcastSync('BANK_TX_DELETED', { id }, getSocketId(req));
        res.json({ success: true });
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

// Suppliers
app.get('/api/suppliers', requireAuth, async (req, res) => {
    try {
        const suppliers = await dbService.getSuppliers();
        res.json(suppliers);
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

app.post('/api/suppliers', requireAuth, async (req, res) => {
    try {
        const supplier = await dbService.createSupplier(req.body);
        broadcastSync('SUPPLIER_CREATED', supplier, getSocketId(req));
        res.json(supplier);
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

app.delete('/api/suppliers/:id', requireAuth, async (req, res) => {
    try {
        const id = Number(req.params.id);
        await dbService.deleteSupplier(id);
        broadcastSync('SUPPLIER_DELETED', { id }, getSocketId(req));
        res.json({ success: true });
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

// Purchase Bills
app.get('/api/purchase-bills', requireAuth, async (req, res) => {
    try {
        const bills = await dbService.getPurchaseBills();
        res.json(bills);
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

app.post('/api/purchase-bills', requireAuth, async (req, res) => {
    try {
        const bill = await dbService.createPurchaseBill(req.body);
        broadcastSync('BILL_CREATED', bill, getSocketId(req));
        res.json(bill);
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

app.put('/api/purchase-bills/:id', requireAuth, async (req, res) => {
    try {
        const bill = await dbService.updatePurchaseBill(req.params.id, req.body);
        if (!bill) return res.status(404).json({ message: 'Purchase bill not found' });
        broadcastSync('BILL_UPDATED', bill, getSocketId(req));
        res.json(bill);
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

app.delete('/api/purchase-bills/:id', requireAuth, async (req, res) => {
    try {
        const id = Number(req.params.id);
        await dbService.deletePurchaseBill(id);
        broadcastSync('BILL_DELETED', { id }, getSocketId(req));
        res.json({ success: true });
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

// Settings
app.get('/api/settings', async (req, res) => {
    try {
        const settings = await dbService.getSettings();
        res.json(settings);
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

app.post('/api/settings', requireAuth, async (req, res) => {
    try {
        const { key, value } = req.body;
        await dbService.setSetting(key, value);
        broadcastSync('SETTINGS_UPDATED', { key, value }, getSocketId(req));
        res.json({ success: true });
    } catch (e) {
        res.status(500).json({ message: e.message });
    }
});

// Full Sync & Diagnostics
app.get('/api/sync/full', requireAuth, async (req, res) => {
    try {
        const data = await dbService.exportAllData();
        res.json({ success: true, data, timestamp: new Date().toISOString() });
    } catch (e) {
        res.status(500).json({ success: false, message: e.message });
    }
});

// Batch Sync (Offline Queue Processor)
app.post('/api/sync/batch', requireAuth, async (req, res) => {
    try {
        const { operations } = req.body || {};
        if (!Array.isArray(operations) || operations.length === 0) {
            return res.json({ success: true, processed: 0 });
        }

        const results = [];
        for (const op of operations) {
            try {
                switch (op.type) {
                    case 'create_item':
                        results.push({ opId: op.id, result: await dbService.createItem(op.payload) });
                        break;
                    case 'update_item':
                        results.push({ opId: op.id, result: await dbService.updateItem(op.targetId, op.payload) });
                        break;
                    case 'create_sale':
                        results.push({ opId: op.id, result: await dbService.createSale({ ...op.payload, userId: req.user.id }) });
                        break;
                    case 'create_repair':
                        results.push({ opId: op.id, result: await dbService.createRepair(op.payload) });
                        break;
                    case 'update_repair':
                        results.push({ opId: op.id, result: await dbService.updateRepair(op.targetId, op.payload) });
                        break;
                    case 'create_expense':
                        results.push({ opId: op.id, result: await dbService.createExpense(op.payload) });
                        break;
                    case 'create_creditor':
                        results.push({ opId: op.id, result: await dbService.createCreditor(op.payload) });
                        break;
                    case 'update_creditor':
                        results.push({ opId: op.id, result: await dbService.updateCreditor(op.targetId, op.payload) });
                        break;
                    case 'create_bank_tx':
                        results.push({ opId: op.id, result: await dbService.createBankTransaction(op.payload) });
                        break;
                    case 'create_bill':
                        results.push({ opId: op.id, result: await dbService.createPurchaseBill(op.payload) });
                        break;
                    default:
                        break;
                }
            } catch (err) {
                console.error(`Error processing batch operation ${op.id}:`, err);
                results.push({ opId: op.id, error: err.message });
            }
        }

        broadcastSync('BATCH_SYNC_COMPLETED', { timestamp: new Date().toISOString() }, getSocketId(req));

        res.json({ success: true, processed: results.length, results });
    } catch (e) {
        console.error('Batch sync error:', e);
        res.status(500).json({ success: false, message: e.message });
    }
});

// Export Backup
app.get('/api/backup/export', requireAuth, async (req, res) => {
    try {
        const data = await dbService.exportAllData();
        res.setHeader('Content-Type', 'application/json');
        res.setHeader('Content-Disposition', `attachment; filename=pos_backup_${Date.now()}.json`);
        res.send(JSON.stringify(data, null, 2));
    } catch (e) {
        res.status(500).json({ success: false, message: e.message });
    }
});

// Front-end Page routing
app.get('/login', (req, res) => {
    res.sendFile(path.join(__dirname, 'login.html'));
});

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

const os = require('os');

function getLocalIPAddress() {
    const interfaces = os.networkInterfaces();
    for (const devName in interfaces) {
        const ifaceList = interfaces[devName];
        for (let i = 0; i < ifaceList.length; i++) {
            const alias = ifaceList[i];
            if (alias.family === 'IPv4' && alias.address !== '127.0.0.1' && !alias.internal) {
                return alias.address;
            }
        }
    }
    return 'localhost';
}

const localIP = getLocalIPAddress();

app.server = server;
app.io = io;
module.exports = app;

if (!process.env.VERCEL) {
    server.listen(PORT, '0.0.0.0', () => {
        console.log(`=========================================`);
        console.log(`🚀 Krishan POS Realtime Server Running!`);
        console.log(`💻 This Computer:  http://localhost:${PORT} or http://krishanpos.local`);
        console.log(`📱 Other Devices:  http://${localIP}:${PORT}`);
        console.log(`⚡ Realtime Sync:  WebSocket Active on Port ${PORT}`);
        console.log(`🔑 Default Admin:  admin / admin123`);
        console.log(`=========================================`);
    });
}
