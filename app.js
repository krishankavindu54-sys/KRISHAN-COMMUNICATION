
// Database Initialization
const db = new Dexie('KrishanPOS_DB');
db.version(1).stores({
    items: '++id, name, barcode, category, type', // type: 'product' | 'service'
    repairs: '++id, customerName, phoneModel, status, createdAt',
    sales: '++id, date, total, paymentMethod', // date is ISO string
    expenses: '++id, date, category',
    creditors: '++id, name, amount, lastUpdated, type' // type: 'payable' | 'receivable'
});

// Update for versioning if needed - strictly keeping v1 for simplicity unless migration needed.
// Dexie handles schema changes dynamically often, but best practice is versioning.
// Since we are adding a store, we can just add it to the existing definition if the DB hasn't been blocked.
// However, the cleanest way for a running app is to bump version.
db.version(2).stores({
    items: '++id, name, barcode, category, type',
    repairs: '++id, customerName, phoneModel, status, createdAt',
    sales: '++id, date, total, paymentMethod',
    expenses: '++id, date, category',
    creditors: '++id, name, amount, lastUpdated, type'
});

db.version(5).stores({
    items: '++id, name, barcode, category, type',
    repairs: '++id, customerName, phoneModel, status, createdAt',
    sales: '++id, date, total, paymentMethod',
    expenses: '++id, date, category',
    creditors: '++id, name, amount, lastUpdated, type',
    categorySettings: 'name',
    bankTransactions: '++id, date, type, amount, note',
    suppliers: '++id, name, company',
    purchaseBills: '++id, supplierId, date, status'
});

// Seed initial data if empty
db.on('populate', () => {
    db.items.bulkAdd([
        { name: "Photocopy (A4)", category: "Service", type: "service", price: 10, cost: 2, barcode: "SERV001", stock: 0 },
        { name: "Passport Photo", category: "Studio", type: "service", price: 350, cost: 50, barcode: "SERV002", stock: 0 },
        { name: "Tempered Glass", category: "Accessories", type: "product", price: 500, cost: 150, barcode: "ACC001", stock: 20, minStock: 5 },
        { name: "CR Books", category: "Stationery", type: "product", price: 250, cost: 180, barcode: "STAT001", stock: 50, minStock: 10 }
    ]);
});

// App Logic
const app = {
    state: {
        cart: [],
        currentView: 'dashboard',
        posCategory: null, // null means "Category Selection Mode"
        inventoryCategory: 'All',
        posCategory: null, // null means "Category Selection Mode"
        inventoryCategory: 'All',
        lastAddedCategory: 'General', // Default for new items
        scanner: null, // Global scanner instance
        modalScanner: null, // Item Modal scanner instance
        selectedCreditor: null // For POS credit sales
    },

    getServerUrl: () => {
        const saved = localStorage.getItem('krishan_pos_custom_server_url');
        if (saved && saved.trim()) {
            return saved.trim().replace(/\/+$/, '');
        }
        if (window.location.protocol === 'file:' || (window.location.port !== '3000' && (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1'))) {
            return 'http://localhost:3000';
        }
        if (window.location.hostname.endsWith('github.io')) {
            return saved ? saved.trim().replace(/\/+$/, '') : '';
        }
        return window.location.origin;
    },

    getApiUrl: (path) => {
        const serverUrl = app.getServerUrl();
        const cleanPath = path.startsWith('/') ? path : '/' + path;
        if (serverUrl) {
            return `${serverUrl}${cleanPath}`;
        }
        return cleanPath;
    },

    getAuthHeaders: (customHeaders = {}) => {
        const headers = { 'Content-Type': 'application/json', ...customHeaders };
        const token = localStorage.getItem('pos_token');
        if (token) {
            headers['Authorization'] = `Bearer ${token}`;
        }
        const socketId = app.realtime?.getSocketId?.();
        if (socketId) {
            headers['x-socket-id'] = socketId;
        }
        return headers;
    },

    currentUser: null,

    // Realtime Multi-Device Sync Engine (Socket.io + HTTP Polling Fallback + IndexedDB)
    realtime: {
        socket: null,
        status: 'connecting', // 'connected' | 'syncing' | 'offline'
        syncMode: 'websocket', // 'websocket' | 'polling'
        deviceCount: 1,
        lastSyncTime: null,
        pendingQueue: JSON.parse(localStorage.getItem('pos_offline_queue') || '[]'),
        pollingInterval: null,
        keepAliveInterval: null,

        init: () => {
            app.realtime.startKeepAlive();
            const serverUrl = app.getServerUrl();
            const isVercel = window.location.hostname.includes('vercel.app');
            const isGitHubPages = window.location.hostname.endsWith('github.io');

            // On Vercel (Serverless REST cloud), start Cloud REST Polling Sync immediately
            if (isVercel || (!serverUrl && isGitHubPages)) {
                console.log('⚡ [Realtime] Initializing Cloud REST Sync Engine for Vercel/Cloud...');
                app.realtime.syncMode = 'polling';
                app.realtime.setStatus('connected');
                app.realtime.startPollingFallback();
                app.syncWithBackend(false);
                return;
            }

            try {
                if (typeof io === 'undefined') {
                    console.warn('Socket.io library not detected. Starting HTTP Cloud Polling fallback.');
                    app.realtime.syncMode = 'polling';
                    app.realtime.setStatus('connected');
                    app.realtime.startPollingFallback();
                    app.syncWithBackend(false);
                    return;
                }

                const socketUrl = serverUrl || window.location.origin;

                const socket = io(socketUrl, {
                    reconnection: true,
                    reconnectionAttempts: 5,
                    reconnectionDelay: 2000,
                    reconnectionDelayMax: 10000,
                    timeout: 10000,
                    transports: ['websocket', 'polling']
                });

                app.realtime.socket = socket;

                socket.on('connect', () => {
                    console.log('⚡ [Realtime] Connected to POS WebSocket, Socket ID:', socket.id);
                    app.realtime.syncMode = 'websocket';
                    app.realtime.setStatus('connected');
                    app.realtime.flushOfflineQueue();
                    app.syncWithBackend(false);
                });

                socket.on('disconnect', (reason) => {
                    console.warn('🔌 [Realtime] WebSocket disconnected (switching to HTTP Cloud Polling):', reason);
                    app.realtime.syncMode = 'polling';
                    app.realtime.setStatus('connected');
                    app.realtime.startPollingFallback();
                });

                socket.on('connect_error', (err) => {
                    console.warn('⚠️ [Realtime] WebSocket connection issue (using HTTP Cloud Polling):', err?.message);
                    app.realtime.syncMode = 'polling';
                    app.realtime.setStatus('connected');
                    app.realtime.startPollingFallback();
                });

                socket.on('sync:welcome', (data) => {
                    if (data && data.deviceCount !== undefined) {
                        app.realtime.deviceCount = data.deviceCount;
                        app.realtime.updateStatusUI();
                    }
                });

                socket.on('sync:device_count', (data) => {
                    if (data && data.count !== undefined) {
                        app.realtime.deviceCount = data.count;
                        app.realtime.updateStatusUI();
                    }
                });

                socket.on('sync:event', (event) => {
                    app.realtime.handleIncomingEvent(event);
                });

            } catch (err) {
                console.error('Socket init error:', err);
                app.realtime.syncMode = 'polling';
                app.realtime.setStatus('connected');
                app.realtime.startPollingFallback();
            }
        },

        // Cloud Keep-Alive: Sends ping every 25 seconds to prevent free cloud instances (Render/Vercel) from sleeping
        startKeepAlive: () => {
            if (app.realtime.keepAliveInterval) clearInterval(app.realtime.keepAliveInterval);
            app.realtime.keepAliveInterval = setInterval(async () => {
                const targetUrl = app.getApiUrl('/api/auth/me');
                try {
                    await fetch(targetUrl, { method: 'GET', headers: app.getAuthHeaders(), credentials: 'include' });
                } catch (e) {
                    // Ignore background ping errors
                }
            }, 25000);
        },

        // HTTP Cloud Polling Fallback (ensures Vercel Serverless / Free Cloud stays 100% Live with realtime stock sync)
        startPollingFallback: () => {
            if (app.realtime.pollingInterval) return; // Already polling

            console.log('🔄 [Realtime] Started HTTP Cloud Polling & Stock Sync fallback...');
            let isSyncing = false;

            const executePoll = async () => {
                if (isSyncing) return;
                isSyncing = true;
                try {
                    const targetUrl = app.getApiUrl('/api/items');
                    const res = await fetch(targetUrl, { method: 'GET', headers: app.getAuthHeaders(), credentials: 'include' });
                    if (res.ok) {
                        const serverItems = await res.json();
                        if (Array.isArray(serverItems)) {
                            let hasChanges = false;
                            const localItems = await db.items.toArray();
                            const localMap = new Map(localItems.map(i => [i.id, i]));

                            for (const sItem of serverItems) {
                                const lItem = localMap.get(sItem.id);
                                if (!lItem || lItem.stock !== sItem.stock || lItem.price !== sItem.price || lItem.name !== sItem.name) {
                                    await db.items.put(sItem);
                                    hasChanges = true;
                                }
                            }

                            // If stock or items changed, immediately update UI without disrupting inputs
                            if (hasChanges) {
                                console.log('⚡ [Cloud Stock Sync] Live stock change synced from cloud, updating view...');
                                if (app.state.currentView === 'pos') app.renderPOS();
                                if (app.state.currentView === 'products') app.renderInventory();
                                if (app.state.currentView === 'dashboard') app.renderDashboard();
                            }

                            app.realtime.syncMode = 'polling';
                            app.realtime.setStatus('connected');
                            app.realtime.lastSyncTime = new Date();

                            // Also flush offline queue if any
                            if (app.realtime.pendingQueue.length > 0) {
                                await app.realtime.flushOfflineQueue();
                            }
                        }
                    } else if (res.status === 401) {
                        // Backend is active and responded
                        app.realtime.syncMode = 'polling';
                        app.realtime.setStatus('connected');
                    }
                } catch (err) {
                    console.warn('Polling check:', err?.message);
                } finally {
                    isSyncing = false;
                }
            };

            // Run first poll immediately
            executePoll();
            app.realtime.pollingInterval = setInterval(executePoll, 2500);
        },

        setStatus: (status) => {
            app.realtime.status = status;
            app.realtime.updateStatusUI();
        },

        updateStatusUI: () => {
            const widget = document.getElementById('realtime-sync-widget');
            const pulse = document.getElementById('sync-pulse');
            const dot = document.getElementById('sync-dot');
            const text = document.getElementById('sync-status-text');
            const badge = document.getElementById('sync-device-badge');

            if (!widget) return;

            const count = app.realtime.deviceCount || 1;
            const mode = app.realtime.syncMode || 'websocket';

            if (app.realtime.status === 'connected') {
                widget.className = 'flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-emerald-50 dark:bg-emerald-950/40 border border-emerald-200 dark:border-emerald-800 text-emerald-700 dark:text-emerald-300 text-xs font-bold cursor-pointer transition-all hover:scale-105 shadow-sm';
                if (pulse) pulse.className = 'animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75';
                if (dot) dot.className = 'relative inline-flex rounded-full h-2 w-2 bg-emerald-500';
                if (text) text.textContent = mode === 'polling' ? 'Cloud Live' : 'Live';
                if (badge) {
                    badge.textContent = mode === 'polling' ? 'Cloud Sync' : (count > 1 ? `${count} devices` : 'Live');
                    badge.className = 'px-1.5 py-0.2 rounded-full bg-emerald-200/60 dark:bg-emerald-800/60 text-[10px]';
                }
                widget.title = `Realtime Live Sync Active (${mode === 'polling' ? 'HTTP Cloud Sync' : count + ' connected device(s)'}). Click for details.`;
            } else if (app.realtime.status === 'syncing') {
                widget.className = 'flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-amber-50 dark:bg-amber-950/40 border border-amber-200 dark:border-amber-800 text-amber-700 dark:text-amber-300 text-xs font-bold cursor-pointer transition-all hover:scale-105 shadow-sm';
                if (pulse) pulse.className = 'animate-ping absolute inline-flex h-full w-full rounded-full bg-amber-400 opacity-75';
                if (dot) dot.className = 'relative inline-flex rounded-full h-2 w-2 bg-amber-500';
                if (text) text.textContent = 'Syncing...';
                if (badge) {
                    badge.textContent = '...';
                    badge.className = 'px-1.5 py-0.2 rounded-full bg-amber-200/60 dark:bg-amber-800/60 text-[10px]';
                }
                widget.title = 'Synchronizing with server...';
            } else {
                // Offline
                widget.className = 'flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-rose-50 dark:bg-rose-950/40 border border-rose-200 dark:border-rose-800 text-rose-700 dark:text-rose-300 text-xs font-bold cursor-pointer transition-all hover:scale-105 shadow-sm';
                if (pulse) pulse.className = 'hidden';
                if (dot) dot.className = 'relative inline-flex rounded-full h-2 w-2 bg-rose-500';
                if (text) text.textContent = 'Offline';
                if (badge) {
                    const qCount = app.realtime.pendingQueue.length;
                    badge.textContent = qCount > 0 ? `${qCount} queued` : 'Local';
                    badge.className = 'px-1.5 py-0.2 rounded-full bg-rose-200/60 dark:bg-rose-800/60 text-[10px]';
                }
                widget.title = 'Offline mode (Working locally). Click for diagnostics & Server URL setup.';
            }
        },

        getSocketId: () => {
            return app.realtime.socket?.id || null;
        },

        queueOfflineMutation: (type, targetId, payload) => {
            const op = {
                id: 'op_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5),
                type,
                targetId,
                payload,
                queuedAt: new Date().toISOString()
            };
            app.realtime.pendingQueue.push(op);
            localStorage.setItem('pos_offline_queue', JSON.stringify(app.realtime.pendingQueue));
            app.realtime.updateStatusUI();
            console.log('📦 Queued offline action:', op);
        },

        flushOfflineQueue: async () => {
            if (app.realtime.pendingQueue.length === 0) return;
            console.log(`📤 Flushing ${app.realtime.pendingQueue.length} offline operations to server...`);
            try {
                const res = await fetch(app.getApiUrl('/api/sync/batch'), {
                    method: 'POST',
                    headers: app.getAuthHeaders(),
                    credentials: 'include',
                    body: JSON.stringify({ operations: app.realtime.pendingQueue })
                });
                if (res.ok) {
                    const data = await res.json();
                    if (data.success) {
                        console.log('✅ Offline queue synced successfully:', data.processed);
                        app.realtime.pendingQueue = [];
                        localStorage.removeItem('pos_offline_queue');
                        app.realtime.updateStatusUI();
                        await app.syncWithBackend(false);
                    }
                }
            } catch (err) {
                console.warn('Could not flush offline queue:', err);
            }
        },

        handleIncomingEvent: async (event) => {
            if (!event || !event.type) return;
            console.log('⚡ [Realtime Sync Event]:', event.type, event.data);
            app.realtime.lastSyncTime = new Date();

            try {
                switch (event.type) {
                    case 'ITEM_CREATED':
                    case 'ITEM_UPDATED': {
                        const item = event.data;
                        if (item && item.id) {
                            await db.items.put(item);
                            if (app.state.currentView === 'pos') app.renderPOS();
                            if (app.state.currentView === 'products') app.renderInventory();
                            if (app.state.currentView === 'dashboard') app.renderDashboard();
                        }
                        break;
                    }
                    case 'ITEM_DELETED': {
                        const { id } = event.data || {};
                        if (id) {
                            await db.items.delete(Number(id));
                            if (app.state.currentView === 'pos') app.renderPOS();
                            if (app.state.currentView === 'products') app.renderInventory();
                            if (app.state.currentView === 'dashboard') app.renderDashboard();
                        }
                        break;
                    }
                    case 'STOCK_CHANGED': {
                        const { id, stock } = event.data || {};
                        if (id && stock !== undefined) {
                            await db.items.update(Number(id), { stock: Number(stock) });
                            if (app.state.currentView === 'pos') app.renderPOS();
                            if (app.state.currentView === 'products') app.renderInventory();
                        }
                        break;
                    }
                    case 'SALE_CREATED': {
                        const { sale, items, cashier } = event.data || {};
                        if (sale) {
                            await db.sales.put(sale);
                            if (items && Array.isArray(items) && items.length > 0) {
                                for (const it of items) {
                                    await db.items.put(it);
                                }
                            }
                            if (app.state.currentView === 'pos') app.renderPOS();
                            if (app.state.currentView === 'sales') app.renderSalesHistory();
                            if (app.state.currentView === 'dashboard') app.renderDashboard();
                            if (app.state.currentView === 'reports') app.renderReports();
                            if (app.state.currentView === 'credits') app.renderCredits();

                            const cashierName = cashier ? ` (${cashier})` : '';
                            Swal.fire({
                                toast: true,
                                position: 'top-end',
                                icon: 'info',
                                title: `🛒 New Sale: LKR ${Number(sale.total).toFixed(2)}${cashierName}`,
                                timer: 2500,
                                showConfirmButton: false
                            });
                        }
                        break;
                    }
                    case 'SALE_DELETED': {
                        const { id } = event.data || {};
                        if (id) {
                            await db.sales.delete(Number(id));
                            if (app.state.currentView === 'sales') app.renderSalesHistory();
                            if (app.state.currentView === 'dashboard') app.renderDashboard();
                        }
                        break;
                    }
                    case 'REPAIR_CREATED':
                    case 'REPAIR_UPDATED': {
                        const repair = event.data;
                        if (repair && repair.id) {
                            await db.repairs.put(repair);
                            if (app.state.currentView === 'repairs') app.renderRepairs();
                            if (app.state.currentView === 'dashboard') app.renderDashboard();

                            Swal.fire({
                                toast: true,
                                position: 'top-end',
                                icon: 'info',
                                title: `🔧 Repair Updated: ${repair.phoneModel} (${repair.status})`,
                                timer: 2500,
                                showConfirmButton: false
                            });
                        }
                        break;
                    }
                    case 'REPAIR_DELETED': {
                        const { id } = event.data || {};
                        if (id) {
                            await db.repairs.delete(Number(id));
                            if (app.state.currentView === 'repairs') app.renderRepairs();
                            if (app.state.currentView === 'dashboard') app.renderDashboard();
                        }
                        break;
                    }
                    case 'EXPENSE_CREATED': {
                        const expense = event.data;
                        if (expense && expense.id) {
                            await db.expenses.put(expense);
                            if (app.state.currentView === 'expenses') app.renderExpenses();
                            if (app.state.currentView === 'dashboard') app.renderDashboard();
                            if (app.state.currentView === 'reports') app.renderReports();
                        }
                        break;
                    }
                    case 'EXPENSE_DELETED': {
                        const { id } = event.data || {};
                        if (id) {
                            await db.expenses.delete(Number(id));
                            if (app.state.currentView === 'expenses') app.renderExpenses();
                            if (app.state.currentView === 'dashboard') app.renderDashboard();
                        }
                        break;
                    }
                    case 'CREDITOR_CREATED':
                    case 'CREDITOR_UPDATED': {
                        const creditor = event.data;
                        if (creditor && creditor.id) {
                            await db.creditors.put(creditor);
                            if (app.state.currentView === 'credits') app.renderCredits();
                            if (app.state.currentView === 'pos') app.renderPOS();
                        }
                        break;
                    }
                    case 'CREDITOR_DELETED': {
                        const { id } = event.data || {};
                        if (id) {
                            await db.creditors.delete(Number(id));
                            if (app.state.currentView === 'credits') app.renderCredits();
                            if (app.state.currentView === 'pos') app.renderPOS();
                        }
                        break;
                    }
                    case 'BANK_TX_CREATED': {
                        const tx = event.data;
                        if (tx && tx.id) {
                            await db.bankTransactions.put(tx);
                            if (app.state.currentView === 'bank') app.renderBankTracker();
                            if (app.state.currentView === 'dashboard') app.renderDashboard();
                        }
                        break;
                    }
                    case 'BANK_TX_DELETED': {
                        const { id } = event.data || {};
                        if (id) {
                            await db.bankTransactions.delete(Number(id));
                            if (app.state.currentView === 'bank') app.renderBankTracker();
                        }
                        break;
                    }
                    case 'SUPPLIER_CREATED': {
                        const supplier = event.data;
                        if (supplier && supplier.id) {
                            await db.suppliers.put(supplier);
                            if (app.state.currentView === 'suppliers') app.renderSuppliers();
                        }
                        break;
                    }
                    case 'SUPPLIER_DELETED': {
                        const { id } = event.data || {};
                        if (id) {
                            await db.suppliers.delete(Number(id));
                            if (app.state.currentView === 'suppliers') app.renderSuppliers();
                        }
                        break;
                    }
                    case 'BILL_CREATED':
                    case 'BILL_UPDATED': {
                        const bill = event.data;
                        if (bill && bill.id) {
                            await db.purchaseBills.put(bill);
                            if (app.state.currentView === 'suppliers') app.renderSuppliers();
                        }
                        break;
                    }
                    case 'BILL_DELETED': {
                        const { id } = event.data || {};
                        if (id) {
                            await db.purchaseBills.delete(Number(id));
                            if (app.state.currentView === 'suppliers') app.renderSuppliers();
                        }
                        break;
                    }
                    case 'SETTINGS_UPDATED': {
                        const { key, value } = event.data || {};
                        if (key) {
                            localStorage.setItem(`krishan_pos_${key}`, value);
                            app.updateShopProfileHeader();
                        }
                        break;
                    }
                    case 'BATCH_SYNC_COMPLETED': {
                        await app.syncWithBackend(false);
                        break;
                    }
                    default:
                        break;
                }
            } catch (eventErr) {
                console.error('Error handling incoming realtime event:', eventErr);
            }
        }
    },

    // Unified socket-aware API Caller with offline fallback
    apiCall: async (path, method = 'GET', data = null, offlineAction = null, offlineId = null) => {
        const url = app.getApiUrl(path);
        const headers = app.getAuthHeaders();

        const options = {
            method,
            headers,
            credentials: 'include'
        };

        if (data && method !== 'GET') {
            options.body = JSON.stringify(data);
        }

        try {
            const res = await fetch(url, options);
            if (!res.ok) {
                throw new Error(`HTTP error ${res.status}`);
            }
            return await res.json();
        } catch (err) {
            console.warn(`API call failed for ${method} ${path}:`, err.message);
            if (offlineAction && data) {
                app.realtime.queueOfflineMutation(offlineAction, offlineId, data);
            }
            return null;
        }
    },

    // Full Backend Synchronization
    syncWithBackend: async (refreshView = false) => {
        try {
            const res = await fetch(app.getApiUrl('/api/sync/full'), {
                method: 'GET',
                headers: app.getAuthHeaders(),
                credentials: 'include'
            });
            if (!res.ok) return false;
            const payload = await res.json();
            if (!payload || !payload.success || !payload.data) return false;

            const data = payload.data;

            if (Array.isArray(data.items) && data.items.length > 0) {
                await db.items.clear();
                await db.items.bulkPut(data.items);
            }
            if (Array.isArray(data.sales)) {
                await db.sales.clear();
                if (data.sales.length > 0) await db.sales.bulkPut(data.sales);
            }
            if (Array.isArray(data.repairs)) {
                await db.repairs.clear();
                if (data.repairs.length > 0) await db.repairs.bulkPut(data.repairs);
            }
            if (Array.isArray(data.expenses)) {
                await db.expenses.clear();
                if (data.expenses.length > 0) await db.expenses.bulkPut(data.expenses);
            }
            if (Array.isArray(data.creditors)) {
                await db.creditors.clear();
                if (data.creditors.length > 0) await db.creditors.bulkPut(data.creditors);
            }
            if (Array.isArray(data.bankTransactions)) {
                await db.bankTransactions.clear();
                if (data.bankTransactions.length > 0) await db.bankTransactions.bulkPut(data.bankTransactions);
            }
            if (Array.isArray(data.suppliers)) {
                await db.suppliers.clear();
                if (data.suppliers.length > 0) await db.suppliers.bulkPut(data.suppliers);
            }
            if (Array.isArray(data.purchaseBills)) {
                await db.purchaseBills.clear();
                if (data.purchaseBills.length > 0) await db.purchaseBills.bulkPut(data.purchaseBills);
            }
            if (data.settings) {
                for (const k in data.settings) {
                    localStorage.setItem(`krishan_pos_${k}`, data.settings[k]);
                }
                app.updateShopProfileHeader();
            }

            app.realtime.lastSyncTime = new Date();

            if (refreshView) {
                if (app.state.currentView === 'dashboard') app.renderDashboard();
                else if (app.state.currentView === 'pos') app.renderPOS();
                else if (app.state.currentView === 'products') app.renderInventory();
                else if (app.state.currentView === 'sales') app.renderSalesHistory();
                else if (app.state.currentView === 'repairs') app.renderRepairs();
                else if (app.state.currentView === 'credits') app.renderCredits();
                else if (app.state.currentView === 'expenses') app.renderExpenses();
                else if (app.state.currentView === 'suppliers') app.renderSuppliers();
                else if (app.state.currentView === 'bank') app.renderBankTracker();
            }
            return true;
        } catch (e) {
            console.warn('Backend full sync error:', e.message);
            return false;
        }
    },

    triggerManualSync: async () => {
        const icon = document.getElementById('manual-sync-icon');
        if (icon) icon.classList.add('fa-spin');
        app.realtime.setStatus('syncing');

        try {
            await app.realtime.flushOfflineQueue();
            await app.syncWithBackend(true);
            app.realtime.setStatus('connected');
            Swal.fire({
                toast: true,
                position: 'top-end',
                icon: 'success',
                title: 'Data Synced Successfully!',
                timer: 1500,
                showConfirmButton: false
            });
        } catch (e) {
            Swal.fire({
                toast: true,
                position: 'top-end',
                icon: 'error',
                title: 'Sync failed: ' + (e.message || 'Server offline'),
                timer: 2000,
                showConfirmButton: false
            });
        } finally {
            if (icon) icon.classList.remove('fa-spin');
        }
    },

    showSyncStatusModal: () => {
        const status = app.realtime.status;
        const mode = app.realtime.syncMode || 'websocket';
        const count = app.realtime.deviceCount || 1;
        const lastSync = app.realtime.lastSyncTime ? new Date(app.realtime.lastSyncTime).toLocaleTimeString() : 'Just now';
        const queueCount = app.realtime.pendingQueue.length;
        const statusColor = status === 'connected' ? 'text-emerald-600 dark:text-emerald-400' : (status === 'syncing' ? 'text-amber-500' : 'text-rose-500');
        const statusBadge = status === 'connected' ? (mode === 'polling' ? '🟢 Cloud Live (Polling Sync)' : '🟢 Live (WebSocket)') : (status === 'syncing' ? '🟡 Syncing...' : '🔴 Offline Mode');
        const customUrl = localStorage.getItem('krishan_pos_custom_server_url') || '';
        const serverHost = customUrl || (window.location.host || 'http://localhost:3000');

        Swal.fire({
            title: '<div class="flex items-center justify-center gap-2 text-xl font-bold"><i class="fa-solid fa-tower-broadcast text-violet-600"></i> Realtime Sync Status</div>',
            html: `
                <div class="text-left space-y-4 my-2 text-sm">
                    <div class="p-4 rounded-2xl bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 space-y-2.5">
                        <div class="flex justify-between items-center">
                            <span class="font-semibold text-slate-500 dark:text-slate-400">Connection State:</span>
                            <span class="font-bold ${statusColor}">${statusBadge}</span>
                        </div>
                        <div class="flex justify-between items-center">
                            <span class="font-semibold text-slate-500 dark:text-slate-400">Sync Mode:</span>
                            <span class="font-bold text-slate-700 dark:text-slate-200 uppercase text-xs">${mode}</span>
                        </div>
                        <div class="flex justify-between items-center">
                            <span class="font-semibold text-slate-500 dark:text-slate-400">Connected Devices:</span>
                            <span class="font-bold text-slate-800 dark:text-white">${count} active device(s)</span>
                        </div>
                        <div class="flex justify-between items-center">
                            <span class="font-semibold text-slate-500 dark:text-slate-400">Last Synced:</span>
                            <span class="font-bold text-slate-800 dark:text-white">${lastSync}</span>
                        </div>
                        <div class="flex justify-between items-center">
                            <span class="font-semibold text-slate-500 dark:text-slate-400">Pending Offline Queue:</span>
                            <span class="font-bold ${queueCount > 0 ? 'text-amber-600' : 'text-slate-800 dark:text-white'}">${queueCount} actions</span>
                        </div>
                        <div class="flex justify-between items-center text-xs text-slate-400 pt-1 border-t border-slate-200 dark:border-slate-700">
                            <span>Backend Server URL:</span>
                            <span class="font-mono text-[11px] truncate max-w-[200px]" title="${serverHost}">${serverHost}</span>
                        </div>
                    </div>

                    <div class="flex gap-2">
                        <button type="button" onclick="app.configureServerUrlModal()" class="w-full py-2 px-3 rounded-xl bg-violet-50 dark:bg-violet-950/50 border border-violet-200 dark:border-violet-800 text-violet-700 dark:text-violet-300 font-bold text-xs hover:bg-violet-100 transition-colors flex items-center justify-center gap-1.5">
                            <i class="fa-solid fa-cloud-arrow-up"></i> Configure Cloud Server URL
                        </button>
                    </div>

                    <p class="text-xs text-slate-400 leading-relaxed">
                        ✨ Sales, inventory stock, repairs, expenses, and credit records are synced live across all counter PCs, mobile phones, and laptops in real time.
                    </p>
                </div>
            `,
            showCancelButton: true,
            confirmButtonText: '<i class="fa-solid fa-arrows-rotate mr-1.5"></i> Force Full Sync',
            cancelButtonText: 'Close',
            confirmButtonColor: '#7c3aed'
        }).then((res) => {
            if (res.isConfirmed) {
                app.triggerManualSync();
            }
        });
    },

    configureServerUrlModal: async () => {
        const currentUrl = localStorage.getItem('krishan_pos_custom_server_url') || '';
        const { value: url } = await Swal.fire({
            title: '<i class="fa-solid fa-server text-violet-600 mb-2"></i><br>Cloud Backend Server URL',
            html: `
                <div class="text-left text-xs text-slate-500 mb-3 leading-relaxed">
                    Enter your live backend server URL (e.g. Render, Railway, Cloudflare Tunnel, or Local Network IP). Leave blank to use default.
                </div>
                <input id="swal-server-url" class="swal2-input !mt-0 !w-full text-sm font-mono" placeholder="https://my-pos.onrender.com or http://192.168.8.185:3000" value="${currentUrl}">
                <div class="text-left text-[11px] text-slate-400 mt-2 space-y-1">
                    <div>💡 <strong>Render Free URL:</strong> <code>https://your-app.onrender.com</code></div>
                    <div>💡 <strong>Local Wi-Fi IP:</strong> <code>http://192.168.8.185:3000</code></div>
                </div>
            `,
            showCancelButton: true,
            confirmButtonText: 'Save & Test Connection',
            confirmButtonColor: '#7c3aed',
            preConfirm: () => {
                const val = document.getElementById('swal-server-url').value.trim();
                return val;
            }
        });

        if (url !== undefined) {
            if (url) {
                localStorage.setItem('krishan_pos_custom_server_url', url.replace(/\/+$/, ''));
            } else {
                localStorage.removeItem('krishan_pos_custom_server_url');
            }

            Swal.fire({
                title: 'Connecting...',
                text: 'Testing connection to server',
                allowOutsideClick: false,
                didOpen: () => {
                    Swal.showLoading();
                }
            });

            try {
                if (app.realtime.socket) {
                    app.realtime.socket.disconnect();
                }
                app.realtime.init();
                await app.syncWithBackend(true);
                Swal.fire({
                    icon: 'success',
                    title: 'Connected Successfully!',
                    text: 'Live sync connected to ' + (url || 'default server'),
                    timer: 2000,
                    showConfirmButton: false
                });
            } catch (e) {
                Swal.fire({
                    icon: 'warning',
                    title: 'Saved in Local Mode',
                    text: 'Server URL saved. ' + e.message,
                    timer: 2500,
                    showConfirmButton: false
                });
            }
        }
    },

    init: async () => {
        try {
            app.updateDateTime();
            setInterval(app.updateDateTime, 1000);
            app.initTheme();

            // 1. Initialize Realtime Engine
            app.realtime.init();

            // 2. Verify authentication
            const isAuth = await app.checkAuth();
            if (!isAuth) {
                app.showLoginOverlay();
                return;
            }

            // 3. Sync with SQLite backend database
            try {
                await app.syncWithBackend(false);
            } catch (syncErr) {
                console.warn('Backend sync skipped/failed:', syncErr);
            }

            app.updateShopProfileHeader();
            app.navigate('dashboard');
        } catch (e) {
            console.error('App init error:', e);
            try {
                app.navigate('dashboard');
            } catch (err) {}
        }

        // Global Error Handler
        window.addEventListener('unhandledrejection', (event) => {
            console.error('Unhandled promise rejection:', event.reason);
        });
    },

    toggleSidebar: (forceState) => {
        const sidebar = document.getElementById('main-sidebar');
        const backdrop = document.getElementById('sidebar-backdrop');
        if (!sidebar) return;
        
        const isHidden = sidebar.classList.contains('-translate-x-full');
        const shouldShow = forceState !== undefined ? forceState : isHidden;

        if (shouldShow) {
            sidebar.classList.remove('-translate-x-full');
            if (backdrop) backdrop.classList.remove('hidden');
        } else {
            sidebar.classList.add('-translate-x-full');
            if (backdrop) backdrop.classList.add('hidden');
        }
    },

    showLoginOverlay: () => {
        let overlay = document.getElementById('pos-login-overlay');
        if (!overlay) {
            overlay = document.createElement('div');
            overlay.id = 'pos-login-overlay';
            overlay.className = 'fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-900/90 backdrop-blur-md';
            overlay.innerHTML = `
                <div class="w-full max-w-md bg-white dark:bg-slate-800 rounded-3xl p-8 sm:p-10 shadow-2xl border border-slate-200 dark:border-slate-700">
                    <div class="text-center mb-6">
                        <div class="w-16 h-16 mx-auto rounded-2xl bg-gradient-to-tr from-violet-600 to-indigo-600 text-white flex items-center justify-center text-2xl shadow-lg shadow-violet-500/30 mb-3">
                            <i class="fa-solid fa-cash-register"></i>
                        </div>
                        <h2 class="text-2xl font-black text-slate-900 dark:text-white">Krishan POS</h2>
                        <p class="text-xs text-violet-600 dark:text-violet-400 font-semibold mt-0.5">Communication & Studio</p>
                        <p class="text-xs text-slate-400 mt-1">Sign in to access the Point of Sale System</p>
                    </div>

                    <form id="overlay-login-form" class="space-y-4" onsubmit="app.handleOverlayLogin(event)">
                        <div>
                            <label class="block text-xs font-bold text-slate-600 dark:text-slate-300 uppercase tracking-wider mb-1.5">
                                <i class="fa-solid fa-user text-violet-600 mr-1"></i> Username
                            </label>
                            <input type="text" id="overlay-username" required autocomplete="username"
                                class="w-full px-4 py-3 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-xl text-slate-800 dark:text-white text-sm font-medium focus:ring-2 focus:ring-violet-500 outline-none transition-all"
                                placeholder="Enter username (e.g. admin)">
                        </div>

                        <div>
                            <label class="block text-xs font-bold text-slate-600 dark:text-slate-300 uppercase tracking-wider mb-1.5">
                                <i class="fa-solid fa-lock text-violet-600 mr-1"></i> Password
                            </label>
                            <input type="password" id="overlay-password" required autocomplete="current-password"
                                class="w-full px-4 py-3 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-xl text-slate-800 dark:text-white text-sm font-medium focus:ring-2 focus:ring-violet-500 outline-none transition-all"
                                placeholder="••••••••">
                        </div>

                        <div id="overlay-error-banner" class="hidden p-3 rounded-xl bg-red-50 dark:bg-red-900/30 border border-red-200 dark:border-red-800 text-red-600 dark:text-red-300 text-xs font-semibold flex items-center gap-2">
                            <i class="fa-solid fa-circle-exclamation"></i>
                            <span id="overlay-error-text">Invalid credentials</span>
                        </div>

                        <button type="submit" id="overlay-login-btn"
                            class="w-full py-3.5 bg-gradient-to-r from-violet-600 to-indigo-600 hover:from-violet-700 hover:to-indigo-700 text-white font-bold rounded-xl shadow-lg shadow-violet-500/25 transition-all active:scale-[0.98] flex items-center justify-center gap-2 text-sm">
                            <span id="overlay-btn-text">Sign In to POS</span>
                            <i class="fa-solid fa-arrow-right"></i>
                        </button>
                    </form>

                    <div class="mt-5 pt-4 border-t border-slate-100 dark:border-slate-700 text-center">
                        <button type="button" onclick="app.fillOverlayAdmin()"
                            class="text-xs font-semibold px-3 py-1.5 rounded-lg bg-violet-50 dark:bg-violet-900/30 text-violet-700 dark:text-violet-300 hover:bg-violet-100 transition-colors border border-violet-100 dark:border-violet-800">
                            ✨ Quick Login: admin / admin123
                        </button>
                    </div>
                </div>
            `;
            document.body.appendChild(overlay);
        }
        overlay.classList.remove('hidden');
        setTimeout(() => {
            const userInp = document.getElementById('overlay-username');
            if (userInp) userInp.focus();
        }, 100);
    },

    fillOverlayAdmin: () => {
        const u = document.getElementById('overlay-username');
        const p = document.getElementById('overlay-password');
        if (u) u.value = 'admin';
        if (p) p.value = 'admin123';
        const form = document.getElementById('overlay-login-form');
        if (form) form.dispatchEvent(new Event('submit'));
    },

    handleOverlayLogin: async (e) => {
        if (e) e.preventDefault();
        const u = document.getElementById('overlay-username').value.trim();
        const p = document.getElementById('overlay-password').value.trim();
        const btn = document.getElementById('overlay-login-btn');
        const btnText = document.getElementById('overlay-btn-text');
        const errBanner = document.getElementById('overlay-error-banner');
        const errText = document.getElementById('overlay-error-text');

        if (!u || !p) return;

        errBanner.classList.add('hidden');
        btn.disabled = true;
        btnText.textContent = 'Signing in...';

        let user = null;
        try {
            // 1. Try backend authentication
            try {
                const res = await fetch(app.getApiUrl('/api/auth/login'), {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'include',
                    body: JSON.stringify({ username: u, password: p })
                });
                const data = await res.json().catch(() => ({}));
                if (res.ok && data.success) {
                    user = data.user;
                    if (data.token) {
                        localStorage.setItem('pos_token', data.token);
                    }
                }
            } catch (err) {
                // Backend offline
            }

            // 2. Fallback offline authentication
            if (!user) {
                if (u.toLowerCase() === 'admin' && p === 'admin123') {
                    user = { id: 1, username: 'admin', role: 'admin', name: 'Administrator' };
                } else if (u.toLowerCase() === 'cashier' && p === 'cashier123') {
                    user = { id: 2, username: 'cashier', role: 'cashier', name: 'Cashier' };
                } else {
                    const localUsers = JSON.parse(localStorage.getItem('pos_registered_users') || '[]');
                    const found = localUsers.find(x => x.username.toLowerCase() === u.toLowerCase() && x.password === p);
                    if (found) {
                        user = { id: found.id, username: found.username, role: found.role || 'cashier', name: found.name };
                    }
                }
            }

            if (!user) {
                throw new Error('Invalid username or password.');
            }

            app.currentUser = user;
            localStorage.setItem('pos_current_user', JSON.stringify(user));
            app.updateUserHeader();

            const overlay = document.getElementById('pos-login-overlay');
            if (overlay) overlay.classList.add('hidden');

            await app.syncWithBackend();
            app.updateShopProfileHeader();
            app.navigate('dashboard');

            Swal.fire({
                toast: true,
                position: 'top-end',
                icon: 'success',
                title: `Logged in as ${user.name || user.username}`,
                timer: 1500,
                showConfirmButton: false
            });

        } catch (err) {
            errText.textContent = err.message || 'Login failed';
            errBanner.classList.remove('hidden');
        } finally {
            btn.disabled = false;
            btnText.textContent = 'Sign In to POS';
        }
    },

    checkAuth: async () => {
        try {
            const res = await fetch(app.getApiUrl('/api/auth/me'), {
                method: 'GET',
                headers: app.getAuthHeaders(),
                credentials: 'include'
            });
            if (res.ok) {
                const data = await res.json();
                if (data && data.authenticated && data.user) {
                    app.currentUser = data.user;
                    localStorage.setItem('pos_current_user', JSON.stringify(data.user));
                    app.updateUserHeader();
                    return true;
                }
            }
        } catch (e) {
            // Offline / static mode
        }

        const saved = localStorage.getItem('pos_current_user');
        if (saved) {
            try {
                app.currentUser = JSON.parse(saved);
                app.updateUserHeader();
                return true;
            } catch (err) {}
        }
        return false;
    },

    updateUserHeader: () => {
        if (!app.currentUser) return;
        const nameEl = document.getElementById('user-display-name');
        const roleEl = document.getElementById('user-display-role');
        const avatarEl = document.getElementById('user-avatar-initials');
        const usersNav = document.getElementById('sidebar-users-item');

        if (nameEl) nameEl.textContent = app.currentUser.name || app.currentUser.username;
        if (roleEl) roleEl.textContent = app.currentUser.role || 'cashier';
        if (avatarEl) {
            const initial = (app.currentUser.name || app.currentUser.username || 'U').charAt(0).toUpperCase();
            avatarEl.textContent = initial;
        }

        if (usersNav) {
            if (app.currentUser.role === 'admin') {
                usersNav.classList.remove('hidden');
            } else {
                usersNav.classList.add('hidden');
            }
        }
    },

    logout: async () => {
        const result = await Swal.fire({
            title: 'Log out?',
            text: 'Are you sure you want to log out of Krishan POS?',
            icon: 'question',
            showCancelButton: true,
            confirmButtonText: 'Yes, Logout',
            cancelButtonText: 'Cancel',
            confirmButtonColor: '#ef4444'
        });

        if (result.isConfirmed) {
            try {
                await fetch(app.getApiUrl('/api/auth/logout'), { method: 'POST', credentials: 'include' });
            } catch (e) {}
            localStorage.removeItem('pos_current_user');
            app.currentUser = null;
            app.showLoginOverlay();
        }
    },

    syncWithBackend: async (refreshView = false) => {
        try {
            const [items, sales, repairs, expenses, creditors, bankTx, suppliers, bills, settings] = await Promise.all([
                fetch(app.getApiUrl('/api/items'), { credentials: 'include' }).then(r => r.ok ? r.json() : []).catch(() => []),
                fetch(app.getApiUrl('/api/sales'), { credentials: 'include' }).then(r => r.ok ? r.json() : []).catch(() => []),
                fetch(app.getApiUrl('/api/repairs'), { credentials: 'include' }).then(r => r.ok ? r.json() : []).catch(() => []),
                fetch(app.getApiUrl('/api/expenses'), { credentials: 'include' }).then(r => r.ok ? r.json() : []).catch(() => []),
                fetch(app.getApiUrl('/api/creditors'), { credentials: 'include' }).then(r => r.ok ? r.json() : []).catch(() => []),
                fetch(app.getApiUrl('/api/bank-transactions'), { credentials: 'include' }).then(r => r.ok ? r.json() : []).catch(() => []),
                fetch(app.getApiUrl('/api/suppliers'), { credentials: 'include' }).then(r => r.ok ? r.json() : []).catch(() => []),
                fetch(app.getApiUrl('/api/purchase-bills'), { credentials: 'include' }).then(r => r.ok ? r.json() : []).catch(() => []),
                fetch(app.getApiUrl('/api/settings'), { credentials: 'include' }).then(r => r.ok ? r.json() : {}).catch(() => ({}))
            ]);

            await db.transaction('rw', db.items, db.sales, db.repairs, db.expenses, db.creditors, db.bankTransactions, db.suppliers, db.purchaseBills, async () => {
                if (items && Array.isArray(items) && items.length > 0) {
                    await db.items.clear();
                    await db.items.bulkAdd(items);
                }
                if (sales && Array.isArray(sales) && sales.length > 0) {
                    await db.sales.clear();
                    await db.sales.bulkAdd(sales);
                }
                if (repairs && Array.isArray(repairs) && repairs.length > 0) {
                    await db.repairs.clear();
                    await db.repairs.bulkAdd(repairs);
                }
                if (expenses && Array.isArray(expenses) && expenses.length > 0) {
                    await db.expenses.clear();
                    await db.expenses.bulkAdd(expenses);
                }
                if (creditors && Array.isArray(creditors) && creditors.length > 0) {
                    await db.creditors.clear();
                    await db.creditors.bulkAdd(creditors);
                }
                if (bankTx && Array.isArray(bankTx) && bankTx.length > 0) {
                    await db.bankTransactions.clear();
                    await db.bankTransactions.bulkAdd(bankTx);
                }
                if (suppliers && Array.isArray(suppliers) && suppliers.length > 0) {
                    await db.suppliers.clear();
                    await db.suppliers.bulkAdd(suppliers);
                }
                if (bills && Array.isArray(bills) && bills.length > 0) {
                    await db.purchaseBills.clear();
                    await db.purchaseBills.bulkAdd(bills);
                }
            });

            if (settings && typeof settings === 'object') {
                if (settings.shop_name) localStorage.setItem('krishan_pos_shop_name', settings.shop_name);
                if (settings.owner_name) localStorage.setItem('krishan_pos_owner_name', settings.owner_name);
                if (settings.phone) localStorage.setItem('krishan_pos_phone', settings.phone);
                if (settings.address) localStorage.setItem('krishan_pos_address', settings.address);
                app.updateShopProfileHeader();
            }

            app.realtime.lastSyncTime = new Date();

            if (refreshView && app.state.currentView) {
                app.navigate(app.state.currentView);
            }
        } catch (err) {
            console.warn('Backend sync warning:', err);
        }
    },

    initTheme: () => {
        const isDark = localStorage.getItem('krishan_pos_theme') === 'dark';
        if (isDark) {
            document.documentElement.classList.add('dark');
            const icon = document.getElementById('theme-icon');
            if (icon) {
                icon.classList.remove('fa-moon');
                icon.classList.add('fa-sun');
            }
        } else {
            document.documentElement.classList.remove('dark');
            const icon = document.getElementById('theme-icon');
            if (icon) {
                icon.classList.remove('fa-sun');
                icon.classList.add('fa-moon');
            }
        }
    },

    toggleDarkMode: () => {
        const html = document.documentElement;
        const icon = document.getElementById('theme-icon');
        if (html.classList.contains('dark')) {
            html.classList.remove('dark');
            localStorage.setItem('krishan_pos_theme', 'light');
            if (icon) {
                icon.classList.remove('fa-sun');
                icon.classList.add('fa-moon');
            }
        } else {
            html.classList.add('dark');
            localStorage.setItem('krishan_pos_theme', 'dark');
            if (icon) {
                icon.classList.remove('fa-moon');
                icon.classList.add('fa-sun');
            }
        }
    },

    getShopProfile: () => ({
        shopName: localStorage.getItem('krishan_pos_shop_name') || 'Krishan Communication & Studio',
        ownerName: localStorage.getItem('krishan_pos_owner_name') || 'Owner',
        phone: localStorage.getItem('krishan_pos_phone') || '',
        address: localStorage.getItem('krishan_pos_address') || ''
    }),

    saveShopProfile: (profile) => {
        localStorage.setItem('krishan_pos_shop_name', profile.shopName || 'Krishan Communication & Studio');
        localStorage.setItem('krishan_pos_owner_name', profile.ownerName || 'Owner');
        localStorage.setItem('krishan_pos_phone', profile.phone || '');
        localStorage.setItem('krishan_pos_address', profile.address || '');

        app.apiCall('/api/settings', 'POST', { key: 'shop_name', value: profile.shopName || '' }, 'set_setting');
        app.apiCall('/api/settings', 'POST', { key: 'owner_name', value: profile.ownerName || '' }, 'set_setting');
        app.apiCall('/api/settings', 'POST', { key: 'phone', value: profile.phone || '' }, 'set_setting');
        app.apiCall('/api/settings', 'POST', { key: 'address', value: profile.address || '' }, 'set_setting');
    },

    updateShopProfileHeader: () => {
        const profile = app.getShopProfile();
        const shopLabel = document.getElementById('shop-profile-name');
        if (shopLabel) {
            shopLabel.textContent = profile.shopName;
        }
    },

    editShopProfile: async () => {
        const currentProfile = app.getShopProfile();
        const { value: profileData } = await Swal.fire({
            title: '<i class="fa-solid fa-store text-violet-600 mb-2"></i><br>Business Profile Details',
            html: `
                <div class="text-left text-sm text-slate-500 mb-3">Add or edit your shop details (shown on receipts & header).</div>
                <div class="grid grid-cols-1 md:grid-cols-2 gap-2 text-left">
                    <div class="md:col-span-2">
                        <label class="block text-xs font-bold uppercase tracking-wide text-slate-500 mb-1">Shop Name</label>
                        <input id="setup-shop-name" class="swal2-input !mt-0 !w-full" placeholder="Krishan Communication & Studio" value="${currentProfile.shopName || ''}">
                    </div>
                    <div>
                        <label class="block text-xs font-bold uppercase tracking-wide text-slate-500 mb-1">Owner Name</label>
                        <input id="setup-owner-name" class="swal2-input !mt-0 !w-full" placeholder="Owner Name" value="${currentProfile.ownerName || ''}">
                    </div>
                    <div>
                        <label class="block text-xs font-bold uppercase tracking-wide text-slate-500 mb-1">Phone Number</label>
                        <input id="setup-phone" class="swal2-input !mt-0 !w-full" placeholder="0771234567" value="${currentProfile.phone || ''}">
                    </div>
                    <div class="md:col-span-2">
                        <label class="block text-xs font-bold uppercase tracking-wide text-slate-500 mb-1">Address</label>
                        <textarea id="setup-address" class="swal2-textarea !mt-0 !w-full" rows="2" placeholder="Shop address">${currentProfile.address || ''}</textarea>
                    </div>
                </div>
            `,
            focusConfirm: false,
            showCancelButton: true,
            confirmButtonText: 'Save Details',
            confirmButtonColor: '#7c3aed',
            preConfirm: () => {
                const shopName = document.getElementById('setup-shop-name').value.trim();
                const ownerName = document.getElementById('setup-owner-name').value.trim();
                const phone = document.getElementById('setup-phone').value.trim();
                const address = document.getElementById('setup-address').value.trim();

                if (!shopName) {
                    Swal.showValidationMessage('Please enter shop name');
                    return false;
                }

                return { shopName, ownerName, phone, address };
            }
        });

        if (profileData) {
            app.saveShopProfile(profileData);
            app.updateShopProfileHeader();
            Swal.fire({
                toast: true,
                position: 'top-end',
                icon: 'success',
                title: 'Shop profile saved',
                showConfirmButton: false,
                timer: 2000
            });
        }
    },

    updateDateTime: () => {
        const now = new Date();
        document.getElementById('current-time').textContent = now.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
        document.getElementById('current-date').textContent = now.toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' });
    },

    navigate: async (view) => {
        const content = document.getElementById('app-content');
        app.state.currentView = view;

        // Update Sidebar Active State
        document.querySelectorAll('.sidebar-item').forEach(el => el.classList.remove('active', 'border-r-4', 'border-violet-600', 'bg-violet-50', 'text-violet-600'));
        const activeNav = document.getElementById(`nav-${view}`);
        if (activeNav) activeNav.classList.add('active', 'border-r-4', 'border-violet-600', 'bg-violet-50', 'text-violet-600');

        content.innerHTML = '<div class="flex items-center justify-center h-full"><i class="fa-solid fa-circle-notch fa-spin text-4xl text-violet-600"></i></div>';

        switch (view) {
            case 'dashboard':
                await app.renderDashboard();
                break;
            case 'pos':
                await app.renderPOS();
                break;
            case 'products':
                await app.renderInventory();
                break;
            case 'repairs':
                await app.renderRepairs();
                break;
            case 'sales':
                await app.renderSalesHistory();
                break;
            case 'reports':
                await app.renderReports();
                break;
            case 'expenses':
                await app.renderExpenses();
                break;
            case 'credits':
                await app.renderCredits();
                break;
            case 'utility':
                await app.renderUtilityBills();
                break;
            case 'suppliers':
                await app.renderSuppliers();
                break;
            case 'bank':
                await app.renderBankTracker();
                break;
            case 'users':
                await app.renderUsers();
                break;
            default:
                app.renderDashboard();
        }
    },

    renderUsers: async () => {
        if (!app.currentUser || app.currentUser.role !== 'admin') {
            Swal.fire('Access Denied', 'Only administrators can manage user accounts.', 'warning');
            app.navigate('dashboard');
            return;
        }

        let users = [];
        try {
            const res = await fetch(app.getApiUrl('/api/users'), { credentials: 'include' });
            const data = await res.json();
            if (data.success) users = data.users;
        } catch (e) {
            console.error('Error loading users:', e);
        }

        const html = `
            <div class="fade-in max-w-6xl mx-auto pb-16">
                <!-- Header -->
                <div class="flex flex-col md:flex-row justify-between items-start md:items-center mb-8 gap-4">
                    <div>
                        <h2 class="text-3xl font-black text-slate-800 dark:text-slate-100 flex items-center gap-3">
                            <i class="fa-solid fa-users-gear text-purple-600"></i> User Accounts & Roles
                        </h2>
                        <p class="text-slate-500 text-sm mt-1">Manage system administrators and cashier logins</p>
                    </div>
                    <button onclick="app.openUserModal()" class="bg-gradient-to-r from-purple-600 to-indigo-600 hover:from-purple-700 hover:to-indigo-700 text-white px-5 py-3 rounded-2xl font-bold shadow-lg shadow-purple-200 dark:shadow-none flex items-center gap-2 transition-all transform active:scale-95">
                        <i class="fa-solid fa-user-plus"></i> Add New User
                    </button>
                </div>

                <!-- Users Grid / Table -->
                <div class="bg-white dark:bg-slate-800 rounded-3xl shadow-sm border border-slate-200 dark:border-slate-700 overflow-hidden">
                    <div class="overflow-x-auto">
                        <table class="w-full text-left border-collapse">
                            <thead>
                                <tr class="bg-slate-50 dark:bg-slate-900/50 border-b border-slate-200 dark:border-slate-700 text-xs uppercase font-extrabold text-slate-500 tracking-wider">
                                    <th class="p-4 pl-6">User</th>
                                    <th class="p-4">Username</th>
                                    <th class="p-4">Role</th>
                                    <th class="p-4">Created Date</th>
                                    <th class="p-4 text-right pr-6">Actions</th>
                                </tr>
                            </thead>
                            <tbody class="divide-y divide-slate-100 dark:divide-slate-700/50 text-sm">
                                ${users.map(u => `
                                    <tr class="hover:bg-slate-50/50 dark:hover:bg-slate-700/20 transition-colors">
                                        <td class="p-4 pl-6 font-bold text-slate-800 dark:text-slate-200 flex items-center gap-3">
                                            <div class="w-10 h-10 rounded-full ${u.role === 'admin' ? 'bg-purple-600' : 'bg-blue-600'} text-white flex items-center justify-center font-bold text-sm">
                                                ${(u.name || u.username).charAt(0).toUpperCase()}
                                            </div>
                                            <div>
                                                <div class="font-bold text-slate-800 dark:text-slate-100">${u.name}</div>
                                                <div class="text-xs text-slate-400 font-normal">ID: #${u.id}</div>
                                            </div>
                                        </td>
                                        <td class="p-4 font-mono font-bold text-slate-700 dark:text-slate-300">
                                            @${u.username}
                                        </td>
                                        <td class="p-4">
                                            <span class="px-3 py-1 rounded-full text-xs font-bold ${
                                                u.role === 'admin'
                                                    ? 'bg-purple-100 text-purple-700 dark:bg-purple-900/40 dark:text-purple-300'
                                                    : 'bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300'
                                            }">
                                                <i class="fa-solid ${u.role === 'admin' ? 'fa-shield-halved' : 'fa-cash-register'} mr-1"></i>
                                                ${u.role.toUpperCase()}
                                            </span>
                                        </td>
                                        <td class="p-4 text-slate-500 text-xs">
                                            ${new Date(u.created_at || Date.now()).toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' })}
                                        </td>
                                        <td class="p-4 text-right pr-6">
                                            <div class="inline-flex items-center gap-2">
                                                <button onclick="app.openUserModal(${u.id}, '${u.username}', '${u.name}', '${u.role}')" class="p-2 text-violet-600 hover:bg-violet-50 dark:hover:bg-violet-900/30 rounded-lg transition-colors" title="Edit User">
                                                    <i class="fa-solid fa-pen-to-square"></i>
                                                </button>
                                                ${u.id !== app.currentUser.id ? `
                                                    <button onclick="app.deleteUser(${u.id}, '${u.username}')" class="p-2 text-red-500 hover:bg-red-50 dark:hover:bg-red-900/30 rounded-lg transition-colors" title="Delete User">
                                                        <i class="fa-solid fa-trash-can"></i>
                                                    </button>
                                                ` : '<span class="text-xs text-slate-400 italic px-2">You</span>'}
                                            </div>
                                        </td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        `;
        document.getElementById('app-content').innerHTML = html;
    },

    openUserModal: async (id = null, currentUsername = '', currentName = '', currentRole = 'cashier') => {
        const isEdit = Boolean(id);
        const { value: formValues } = await Swal.fire({
            title: `<i class="fa-solid ${isEdit ? 'fa-user-pen' : 'fa-user-plus'} text-purple-600 mb-2"></i><br>${isEdit ? 'Edit User Account' : 'Add New POS User'}`,
            html: `
                <div class="text-left space-y-3">
                    <div>
                        <label class="block text-xs font-bold uppercase text-slate-500 mb-1">Full Name</label>
                        <input id="swal-user-name" class="swal2-input !mt-0 !w-full" placeholder="e.g. Kasun Perera" value="${currentName}">
                    </div>
                    <div>
                        <label class="block text-xs font-bold uppercase text-slate-500 mb-1">Username (Login ID)</label>
                        <input id="swal-user-username" class="swal2-input !mt-0 !w-full" placeholder="e.g. kasun" value="${currentUsername}" ${isEdit ? 'disabled' : ''}>
                    </div>
                    <div>
                        <label class="block text-xs font-bold uppercase text-slate-500 mb-1">${isEdit ? 'New Password (leave blank to keep current)' : 'Password'}</label>
                        <input id="swal-user-pass" type="password" class="swal2-input !mt-0 !w-full" placeholder="${isEdit ? '••••••••' : 'At least 4 characters'}">
                    </div>
                    <div>
                        <label class="block text-xs font-bold uppercase text-slate-500 mb-1">Role / Permissions</label>
                        <select id="swal-user-role" class="swal2-select !mt-0 !w-full">
                            <option value="cashier" ${currentRole === 'cashier' ? 'selected' : ''}>Cashier (Sales & Inventory View)</option>
                            <option value="admin" ${currentRole === 'admin' ? 'selected' : ''}>Administrator (Full Access)</option>
                        </select>
                    </div>
                </div>
            `,
            focusConfirm: false,
            showCancelButton: true,
            confirmButtonText: isEdit ? 'Save Changes' : 'Create User',
            confirmButtonColor: '#9333ea',
            preConfirm: () => {
                const name = document.getElementById('swal-user-name').value.trim();
                const username = document.getElementById('swal-user-username').value.trim();
                const password = document.getElementById('swal-user-pass').value.trim();
                const role = document.getElementById('swal-user-role').value;

                if (!name || (!isEdit && !username)) {
                    Swal.showValidationMessage('Please fill all required fields');
                    return false;
                }
                if (!isEdit && (!password || password.length < 4)) {
                    Swal.showValidationMessage('Password must be at least 4 characters');
                    return false;
                }
                return { name, username, password, role };
            }
        });

        if (formValues) {
            try {
                if (isEdit) {
                    const res = await fetch(app.getApiUrl(`/api/users/${id}`), {
                        method: 'PUT',
                        headers: { 'Content-Type': 'application/json' },
                        credentials: 'include',
                        body: JSON.stringify(formValues)
                    });
                    const data = await res.json();
                    if (!res.ok) throw new Error(data.message || 'Update failed');
                    Swal.fire({ icon: 'success', title: 'User Updated', timer: 1500, showConfirmButton: false });
                } else {
                    const res = await fetch(app.getApiUrl('/api/users'), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        credentials: 'include',
                        body: JSON.stringify(formValues)
                    });
                    const data = await res.json();
                    if (!res.ok) throw new Error(data.message || 'Creation failed');
                    Swal.fire({ icon: 'success', title: 'User Created', timer: 1500, showConfirmButton: false });
                }
                app.renderUsers();
            } catch (err) {
                Swal.fire('Error', err.message, 'error');
            }
        }
    },

    deleteUser: async (id, username) => {
        const result = await Swal.fire({
            title: `Delete user @${username}?`,
            text: 'This account will no longer be able to log in.',
            icon: 'warning',
            showCancelButton: true,
            confirmButtonText: 'Yes, Delete',
            confirmButtonColor: '#ef4444',
            cancelButtonText: 'Cancel'
        });

        if (result.isConfirmed) {
            try {
                const res = await fetch(app.getApiUrl(`/api/users/${id}`), {
                    method: 'DELETE',
                    credentials: 'include'
                });
                const data = await res.json();
                if (!res.ok) throw new Error(data.message || 'Delete failed');
                Swal.fire({ icon: 'success', title: 'User Deleted', timer: 1500, showConfirmButton: false });
                app.renderUsers();
            } catch (err) {
                Swal.fire('Error', err.message, 'error');
            }
        }
    },

    renderBankTracker: async () => {
        let transactions = [];
        try {
            transactions = await db.bankTransactions.reverse().toArray();
        } catch (e) {
            console.error("Database error:", e);
            // If table doesn't exist, we might need to refresh or alert
            Swal.fire('Database Update Required', 'Logging you out to refresh system tables...', 'info').then(() => {
                location.reload();
            });
            return;
        }

        const totalDeposits = transactions.filter(t => t.type === 'deposit').reduce((sum, t) => sum + t.amount, 0);
        const totalWithdrawals = transactions.filter(t => t.type === 'withdrawal').reduce((sum, t) => sum + t.amount, 0);
        const balance = totalDeposits - totalWithdrawals;

        const html = `
            <div class="fade-in max-w-5xl mx-auto">
                <div class="flex flex-col md:flex-row justify-between items-center mb-10 gap-6">
                    <div>
                        <h2 class="text-3xl font-black text-slate-800">Bank Balance Tracker</h2>
                        <p class="text-slate-500">Manage your daily savings and withdrawals</p>
                    </div>
                    <div class="flex gap-3">
                        <button onclick="app.openBankTransactionModal('deposit')" class="bg-emerald-600 hover:bg-emerald-700 text-white px-6 py-3 rounded-2xl font-bold shadow-lg shadow-emerald-100 flex items-center gap-2 transition-all">
                            <i class="fa-solid fa-plus-circle"></i> Deposit Money
                        </button>
                        <button onclick="app.openBankTransactionModal('withdrawal')" class="bg-red-600 hover:bg-red-700 text-white px-6 py-3 rounded-2xl font-bold shadow-lg shadow-red-100 flex items-center gap-2 transition-all">
                            <i class="fa-solid fa-minus-circle"></i> Withdraw Money
                        </button>
                    </div>
                </div>

                <!-- Balance Summary Card -->
                <div class="bg-gradient-to-br from-blue-600 to-indigo-700 p-10 rounded-[2.5rem] shadow-2xl shadow-blue-200 text-white mb-10 relative overflow-hidden">
                    <div class="absolute top-0 right-0 p-10 opacity-10">
                        <i class="fa-solid fa-building-columns text-[10rem]"></i>
                    </div>
                    <div class="relative z-10">
                        <p class="text-blue-100 font-bold text-xl mb-2 uppercase tracking-widest">Available Balance</p>
                        <h1 class="text-7xl font-black tracking-tighter mb-8">LKR ${balance.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</h1>
                        
                        <div class="grid grid-cols-2 gap-8 pt-8 border-t border-white/20">
                            <div>
                                <p class="text-blue-200 text-sm font-bold uppercase mb-1">Total Put</p>
                                <p class="text-2xl font-black text-emerald-300">+ LKR ${totalDeposits.toLocaleString()}</p>
                            </div>
                            <div>
                                <p class="text-blue-200 text-sm font-bold uppercase mb-1">Total Taken</p>
                                <p class="text-2xl font-black text-red-300">- LKR ${totalWithdrawals.toLocaleString()}</p>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Transaction History -->
                <div class="bg-white p-8 rounded-[2rem] shadow-xl border border-slate-200 overflow-hidden">
                    <div class="flex items-center justify-between mb-8">
                        <h3 class="text-xl font-black text-slate-800 flex items-center gap-2">
                            <i class="fa-solid fa-clock-rotate-left text-blue-600"></i> Transaction History
                        </h3>
                    </div>
                    
                    <div class="overflow-x-auto">
                        <table class="w-full">
                            <thead>
                                <tr class="text-left text-slate-400 text-sm uppercase font-bold border-b border-slate-100">
                                    <th class="pb-4 px-2">Date</th>
                                    <th class="pb-4 px-2">Description</th>
                                    <th class="pb-4 px-2 text-right">Amount</th>
                                    <th class="pb-4 px-2 text-right">Actions</th>
                                </tr>
                            </thead>
                            <tbody class="divide-y divide-slate-50">
                                ${transactions.length === 0 ? `
                                    <tr>
                                        <td colspan="4" class="py-20 text-center">
                                            <div class="flex flex-col items-center opacity-30">
                                                <i class="fa-solid fa-receipt text-6xl mb-4"></i>
                                                <p class="text-xl font-bold">No transactions recorded yet</p>
                                                <p class="text-sm">Start by adding a deposit or withdrawal</p>
                                            </div>
                                        </td>
                                    </tr>
                                ` : transactions.map(t => `
                                    <tr class="hover:bg-slate-50/50 transition-colors group">
                                        <td class="py-5 px-2">
                                            <p class="font-bold text-slate-700">${new Date(t.date).toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' })}</p>
                                            <p class="text-xs text-slate-400">${new Date(t.date).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}</p>
                                        </td>
                                        <td class="py-5 px-2">
                                            <div class="flex items-center gap-3">
                                                <div class="w-10 h-10 rounded-xl flex items-center justify-center ${t.type === 'deposit' ? 'bg-emerald-50 text-emerald-600' : 'bg-red-50 text-red-600'}">
                                                    <i class="fa-solid ${t.type === 'deposit' ? 'fa-arrow-trend-up' : 'fa-arrow-trend-down'}"></i>
                                                </div>
                                                <div>
                                                    <p class="font-bold text-slate-800 capitalize">${t.type}</p>
                                                    <p class="text-xs text-slate-500">${t.note || 'No description'}</p>
                                                </div>
                                            </div>
                                        </td>
                                        <td class="py-5 px-2 text-right">
                                            <p class="text-lg font-black ${t.type === 'deposit' ? 'text-emerald-600' : 'text-red-500'}">
                                                ${t.type === 'deposit' ? '+' : '-'} ${t.amount.toFixed(2)}
                                            </p>
                                        </td>
                                        <td class="py-5 px-2 text-right">
                                            <button onclick="app.deleteBankTransaction(${t.id})" class="text-slate-300 hover:text-red-500 transition-colors p-2 lg:opacity-0 group-hover:opacity-100">
                                                <i class="fa-solid fa-trash-can"></i>
                                            </button>
                                        </td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        `;
        document.getElementById('app-content').innerHTML = html;
    },

    openBankTransactionModal: async (type) => {
        const { value: formValues } = await Swal.fire({
            title: `<i class="fa-solid ${type === 'deposit' ? 'fa-circle-plus text-emerald-600' : 'fa-circle-minus text-red-600'} mb-2"></i><br>${type === 'deposit' ? 'Add Deposit' : 'Record Withdrawal'}`,
            html: `
                <div class="text-sm text-slate-500 mb-4">Enter the amount and a brief note</div>
                <div class="relative mb-3">
                    <span class="absolute left-4 top-1/2 -translate-y-1/2 font-bold text-slate-400">LKR</span>
                    <input id="bank-amount" class="swal2-input !m-0 !pl-14" type="number" placeholder="0.00" autofocus>
                </div>
                <input id="bank-note" class="swal2-input !m-0" type="text" placeholder="Note (optional)">
            `,
            showCancelButton: true,
            confirmButtonText: type === 'deposit' ? 'Deposit' : 'Withdraw',
            confirmButtonColor: type === 'deposit' ? '#059669' : '#dc2626',
            preConfirm: () => {
                const amount = parseFloat(document.getElementById('bank-amount').value);
                const note = document.getElementById('bank-note').value;
                if (!amount || amount <= 0) {
                    Swal.showValidationMessage('Please enter a valid amount');
                    return false;
                }
                return { amount, note };
            }
        });

        if (formValues) {
            const txRecord = {
                date: new Date().toISOString(),
                type: type,
                amount: formValues.amount,
                note: formValues.note
            };
            const newId = await db.bankTransactions.add(txRecord);
            app.apiCall('/api/bank-transactions', 'POST', { id: newId, ...txRecord }, 'create_bank_tx');
            app.renderBankTracker();
            
            Swal.fire({
                icon: 'success',
                title: 'Transaction Saved',
                showConfirmButton: false,
                timer: 1500,
                toast: true,
                position: 'top-end'
            });
        }
    },

    deleteBankTransaction: async (id) => {
        const result = await Swal.fire({
            title: 'Delete Transaction?',
            text: "This action cannot be undone!",
            icon: 'warning',
            showCancelButton: true,
            confirmButtonColor: '#ef4444',
            confirmButtonText: 'Yes, delete it!'
        });

        if (result.isConfirmed) {
            await db.bankTransactions.delete(id);
            app.apiCall(`/api/bank-transactions/${id}`, 'DELETE', null, 'delete_bank_tx', id);
            app.renderBankTracker();
        }
    },

    renderSuppliers: async () => {
        const suppliers = await db.suppliers.toArray();
        const bills = await db.purchaseBills.toArray();

        const html = `
            <div class="fade-in max-w-6xl mx-auto">
                <div class="flex flex-col md:flex-row justify-between items-center mb-10 gap-6">
                    <div>
                        <h2 class="text-3xl font-black text-slate-800">Supplier & Purchase</h2>
                        <p class="text-slate-500">Track wholesale shops and delivery guys</p>
                    </div>
                    <button onclick="app.openAddSupplierModal()" class="bg-violet-600 hover:bg-violet-700 text-white px-6 py-3 rounded-2xl font-bold shadow-lg shadow-violet-100 flex items-center gap-2 transition-all">
                        <i class="fa-solid fa-plus"></i> Add New Supplier
                    </button>
                </div>

                <div class="grid grid-cols-1 lg:grid-cols-3 gap-8">
                    <!-- Supplier List -->
                    <div class="lg:col-span-1 space-y-4">
                        <h3 class="font-bold text-slate-700 flex items-center gap-2 px-2">
                            <i class="fa-solid fa-address-book text-violet-600"></i> My Suppliers
                        </h3>
                        <div class="bg-white rounded-[2rem] shadow-xl border border-slate-200 overflow-hidden min-h-[500px]">
                            ${suppliers.length === 0 ? `
                                <div class="p-20 text-center opacity-30">
                                    <i class="fa-solid fa-truck-field text-5xl mb-3"></i>
                                    <p class="text-xs font-bold uppercase tracking-widest">No Suppliers Found</p>
                                </div>
                            ` : `
                                <div class="divide-y divide-slate-100 max-h-[500px] overflow-y-auto">
                                    ${suppliers.map(s => {
                                        const supplierBills = bills.filter(b => b.supplierId === s.id);
                                        const pendingTotal = supplierBills.reduce((sum, b) => sum + (b.total - (b.paidAmount || 0)), 0);
                                        return `
                                            <div onclick="app.viewSupplierBills(${s.id})" 
                                                 class="supplier-item-${s.id} p-5 hover:bg-slate-50 cursor-pointer transition-all flex items-center justify-between group">
                                                <div class="space-y-1">
                                                    <h4 class="font-black text-slate-800 text-base group-hover:text-violet-600 transition-colors">${s.name}</h4>
                                                    <p class="text-xs text-slate-400 font-medium">${s.company || 'Direct Supplier'}</p>
                                                    ${pendingTotal > 0 ? `
                                                        <span class="inline-block text-[10px] font-black uppercase tracking-wider text-red-500 bg-red-50 px-2 py-0.5 rounded-md">
                                                            LKR ${pendingTotal.toLocaleString()} Due
                                                        </span>
                                                    ` : `
                                                        <span class="inline-block text-[10px] font-black uppercase tracking-wider text-emerald-500 bg-emerald-50 px-2 py-0.5 rounded-md">
                                                            Settled
                                                        </span>
                                                    `}
                                                </div>
                                                <button onclick="event.stopPropagation(); app.deleteSupplier(${s.id})" 
                                                        class="opacity-0 group-hover:opacity-100 p-2 text-slate-300 hover:text-red-500 transition-all">
                                                    <i class="fa-solid fa-trash-can"></i>
                                                </button>
                                            </div>
                                        `;
                                    }).join('')}
                                </div>
                            `}
                        </div>
                    </div>

                    <!-- Supplier Details & Bills -->
                    <div id="supplier-bills-view" class="lg:col-span-2">
                        <div class="h-full bg-slate-50/50 rounded-[2rem] border-2 border-dashed border-slate-200 flex flex-col items-center justify-center p-12 text-center text-slate-400">
                            <i class="fa-solid fa-hand-pointer text-4xl mb-4 text-slate-300"></i>
                            <p class="font-bold">Select a supplier from the list</p>
                            <p class="text-xs">to view purchase bills and settle payments</p>
                        </div>
                    </div>
                </div>
            </div>
        `;
        document.getElementById('app-content').innerHTML = html;
    },

    openAddSupplierModal: async () => {
        const { value: formValues } = await Swal.fire({
            title: 'Add New Supplier',
            html: `
                <div class="text-left mb-2 text-xs font-bold text-slate-400 uppercase tracking-widest">Supplier Details</div>
                <input id="sup-name" class="swal2-input !mt-0" placeholder="Contact Name (e.g. Sunil)">
                <input id="sup-company" class="swal2-input" placeholder="Shop/Company (e.g. City Wholesale)">
                <input id="sup-contact" class="swal2-input" placeholder="Contact number">
            `,
            showCancelButton: true,
            confirmButtonText: 'Save Supplier',
            confirmButtonColor: '#7c3aed',
            preConfirm: () => {
                const name = document.getElementById('sup-name').value;
                const company = document.getElementById('sup-company').value;
                const contact = document.getElementById('sup-contact').value;
                if (!name) {
                    Swal.showValidationMessage('Name is required');
                    return false;
                }
                return { name, company, contact };
            }
        });

        if (formValues) {
            const newId = await db.suppliers.add(formValues);
            app.apiCall('/api/suppliers', 'POST', { id: newId, ...formValues }, 'create_supplier');
            app.renderSuppliers();
            Swal.fire({ icon: 'success', title: 'Supplier added', toast: true, position: 'top-end', timer: 2000, showConfirmButton: false });
        }
    },

    viewSupplierBills: async (supplierId) => {
        const supplier = await db.suppliers.get(supplierId);
        const bills = await db.purchaseBills.where('supplierId').equals(supplierId).reverse().toArray();
        const pendingTotal = bills.reduce((sum, b) => sum + (b.total - (b.paidAmount || 0)), 0);

        // Highlight active supplier
        document.querySelectorAll('[class^="supplier-item-"]').forEach(el => el.classList.remove('bg-violet-50', 'border-l-4', 'border-violet-600'));
        const activeItem = document.querySelector(`.supplier-item-${supplierId}`);
        if (activeItem) activeItem.classList.add('bg-violet-50', 'border-l-4', 'border-violet-600');

        const html = `
            <div class="fade-in flex flex-col h-full bg-white rounded-[2rem] shadow-xl border border-slate-200 p-8">
                <div class="flex justify-between items-start mb-8">
                    <div>
                        <h3 class="text-2xl font-black text-slate-800">${supplier.name}</h3>
                        <p class="text-slate-500 font-bold uppercase text-[10px] tracking-widest">${supplier.company || 'Private'} • ${supplier.contact || 'No Contact'}</p>
                    </div>
                    <button onclick="app.openAddSupplierBillModal(${supplierId})" class="bg-slate-900 text-white px-5 py-2.5 rounded-xl text-sm font-bold hover:bg-slate-800 transition-all flex items-center gap-2 shadow-lg">
                        <i class="fa-solid fa-plus-circle"></i> බිල්පතක් එක් කරන්න (Add Bill)
                    </button>
                </div>

                <div class="grid grid-cols-2 gap-6 mb-8">
                    <div class="bg-slate-50 p-5 rounded-2xl border border-slate-100">
                        <p class="text-[10px] font-bold text-slate-400 uppercase tracking-widest mb-1">මුළු ඇණවුම් වටිනාකම (Total Ordered)</p>
                        <p class="text-2xl font-black text-slate-700">LKR ${bills.reduce((sum, b) => sum + b.total, 0).toLocaleString()}</p>
                    </div>
                    <div class="bg-red-50 p-5 rounded-2xl border border-red-100">
                        <p class="text-[10px] font-bold text-red-500 uppercase tracking-widest mb-1">ගෙවීමට ඇති මුදල (To be Paid)</p>
                        <p class="text-2xl font-black text-red-600">LKR ${pendingTotal.toLocaleString()}</p>
                    </div>
                </div>

                <div class="overflow-y-auto flex-1 scrollbar-hide pr-2">
                    <table class="w-full">
                        <thead class="sticky top-0 bg-white z-10">
                            <tr class="text-left text-[10px] text-slate-400 uppercase font-black tracking-widest border-b border-slate-100">
                                <th class="pb-3 px-2">Date / Info</th>
                                <th class="pb-3 px-2 text-right">බිල්පත (Bill)</th>
                                <th class="pb-3 px-2 text-right">ගෙවූ මුදල (Paid)</th>
                                <th class="pb-3 px-2 text-right">හිඟය (Balance)</th>
                                <th class="pb-3 px-2 text-center">Status</th>
                            </tr>
                        </thead>
                        <tbody class="divide-y divide-slate-50">
                            ${bills.length === 0 ? `
                                <tr><td colspan="5" class="py-20 text-center text-slate-300 font-bold">පර්චස් අයිතම හමු නොවීය (No purchase records found)</td></tr>
                            ` : bills.map(b => {
            const balance = b.total - (b.paidAmount || 0);
            return `
                                <tr class="group hover:bg-slate-50/50 transition-colors">
                                    <td class="py-5 px-2">
                                        <p class="font-bold text-slate-700 text-sm">${new Date(b.date).toLocaleDateString('en-GB', { day: '2-digit', month: 'short' })}</p>
                                        <p class="text-[10px] text-slate-400 font-medium">${b.note || 'No Invoice #'}</p>
                                    </td>
                                    <td class="py-5 px-2 text-right font-bold text-slate-400 text-sm">LKR ${b.total.toLocaleString()}</td>
                                    <td class="py-5 px-2 text-right font-bold text-emerald-600 text-sm">LKR ${(b.paidAmount || 0).toLocaleString()}</td>
                                    <td class="py-5 px-2 text-right font-black text-slate-800 text-base">LKR ${balance.toLocaleString()}</td>
                                    <td class="py-5 px-2 text-center">
                                        <button onclick="app.markBillAsPaid(${b.id}, ${supplierId})" 
                                                class="text-[10px] font-black uppercase px-3 py-1.5 rounded-full transition-all ${balance <= 0 ? 'bg-emerald-100 text-emerald-700 shadow-sm border border-emerald-200' : 'bg-red-100 text-red-700 hover:bg-red-600 hover:text-white shadow-md border border-red-200'}">
                                            ${balance <= 0 ? 'Paid' : 'Gawanna (Pay)'}
                                        </button>
                                    </td>
                                </tr>
                            `}).join('')}
                        </tbody>
                    </table>
                </div>
            </div>
        `;
        document.getElementById('supplier-bills-view').innerHTML = html;
    },

    openAddSupplierBillModal: async (supplierId) => {
        const { value: formValues } = await Swal.fire({
            title: 'නව පර්චස් බිල්පතක් (Record New Purchase)',
            html: `
                <div class="text-left mb-4">
                    <label class="text-[10px] font-black text-slate-400 uppercase tracking-widest pl-1">මුළු මුදල (Bill Amount - LKR)</label>
                    <div class="relative mt-1">
                        <span class="absolute left-4 top-1/2 -translate-y-1/2 font-bold text-slate-400">Rs.</span>
                        <input id="bill-total" type="number" class="swal2-input !m-0 !pl-14 !w-full" placeholder="Total Amount">
                    </div>
                </div>
                <div class="text-left mb-4">
                    <label class="text-[10px] font-black text-slate-400 uppercase tracking-widest pl-1">විස්තරය (Invoice / Note)</label>
                    <input id="bill-note" class="swal2-input !mt-1 !w-full" placeholder="e.g. Shop Bill #456">
                </div>
                <div class="text-left">
                    <label class="text-[10px] font-black text-slate-400 uppercase tracking-widest pl-1">තත්වය (Initial Status)</label>
                    <select id="bill-status" class="swal2-select !mt-1 !w-full !m-0">
                        <option value="pending">හිඟ මුදල් (Credit Bill)</option>
                        <option value="paid">ගෙවා නිම කළ (Paid Full)</option>
                    </select>
                </div>
            `,
            showCancelButton: true,
            confirmButtonText: 'සටහන් කරන්න (Record)',
            confirmButtonColor: '#0f172a',
            preConfirm: () => {
                const total = parseFloat(document.getElementById('bill-total').value);
                const note = document.getElementById('bill-note').value;
                const status = document.getElementById('bill-status').value;
                if (!total || total <= 0) {
                    Swal.showValidationMessage('මුදල ඇතුළත් කිරීම අනිවාර්යයි');
                    return false;
                }
                return { 
                    total, note, status, supplierId, 
                    date: new Date().toISOString(),
                    paidAmount: status === 'paid' ? total : 0 
                };
            }
        });

        if (formValues) {
            const newId = await db.purchaseBills.add(formValues);
            app.apiCall('/api/purchase-bills', 'POST', { id: newId, ...formValues }, 'create_bill');
            app.viewSupplierBills(supplierId);
            app.renderSuppliers();
        }
    },

    markBillAsPaid: async (billId, supplierId) => {
        const bill = await db.purchaseBills.get(billId);
        const currentPaid = bill.paidAmount || 0;
        const balance = bill.total - currentPaid;

        const { value: paidAmount } = await Swal.fire({
            title: 'මුදල් ගෙවීම (Record Payment)',
            html: `
                <div class="text-left mb-4 bg-slate-50 p-4 rounded-xl border border-slate-100 italic text-xs text-slate-500">
                    <div class="flex justify-between mb-1"><span>මුළු බිල්පත:</span> <span>LKR ${bill.total}</span></div>
                    <div class="flex justify-between mb-1"><span>කලින් ගෙවූ:</span> <span>LKR ${currentPaid}</span></div>
                    <div class="flex justify-between font-bold text-slate-700"><span>හිඟය:</span> <span>LKR ${balance}</span></div>
                </div>
                <div class="text-left">
                    <label class="text-[10px] font-black text-slate-400 uppercase tracking-widest pl-1">දැන් ගෙවන මුදල (Amount to Pay Now)</label>
                    <input id="pay-now-amount" type="number" class="swal2-input !mt-1 !w-full" value="${balance}">
                </div>
            `,
            showCancelButton: true,
            confirmButtonColor: '#10b981',
            confirmButtonText: 'මුදල් ගෙව්වා (Pay)',
            preConfirm: () => {
                const amount = parseFloat(document.getElementById('pay-now-amount').value);
                if (isNaN(amount) || amount <= 0) {
                    Swal.showValidationMessage('කරුණාකර නිවැරදි මුදලක් ඇතුළත් කරන්න');
                    return false;
                }
                return amount;
            }
        });

        if (paidAmount !== undefined) {
            const newTotalPaid = currentPaid + paidAmount;
            const newStatus = newTotalPaid >= bill.total ? 'paid' : 'pending';
            
            const updatedBill = { 
                ...bill,
                paidAmount: newTotalPaid,
                status: newStatus
            };
            await db.purchaseBills.update(billId, { 
                paidAmount: newTotalPaid,
                status: newStatus
            });
            app.apiCall(`/api/purchase-bills/${billId}`, 'PUT', updatedBill, 'update_bill', billId);
            
            app.viewSupplierBills(supplierId);
            app.renderSuppliers();
            Swal.fire({ icon: 'success', title: 'ගෙවීම සටහන් විය', timer: 1000, showConfirmButton: false });
        }
    },

    deleteSupplierBill: async (id, supplierId) => {
        if (confirm('Delete this bill record?')) {
            await db.purchaseBills.delete(id);
            app.apiCall(`/api/purchase-bills/${id}`, 'DELETE', null, 'delete_bill', id);
            app.viewSupplierBills(supplierId);
            app.renderSuppliers();
        }
    },

    deleteSupplier: async (id) => {
        if (confirm('Delete this supplier? All history will be deleted.')) {
            await db.suppliers.delete(id);
            await db.purchaseBills.where('supplierId').equals(id).delete();
            app.apiCall(`/api/suppliers/${id}`, 'DELETE', null, 'delete_supplier', id);
            app.renderSuppliers();
        }
    },

    // --- DASHBOARD ---
    renderDashboard: async () => {
        const today = new Date().toISOString().split('T')[0];
        const salesToday = await db.sales.where('date').startsWith(today).toArray();
        const totalRevenue = salesToday.reduce((sum, sale) => sum + sale.total, 0);
        const pendingRepairs = await db.repairs.where('status').equals('Pending').count();
        const lowStockItems = await db.items.filter(i => i.type === 'product' && i.stock <= (i.minStock || 5)).count();

        // Calculate comparison (mock for now, or fetch yesterday)
        // Simple logic: just show specific stats

        const html = `
            <div class="grid grid-cols-1 xl:grid-cols-3 gap-8 mb-10 fade-in">
                <div class="bg-gradient-to-br from-violet-500 to-indigo-600 p-8 rounded-3xl shadow-xl shadow-violet-200 text-white flex flex-col justify-between min-h-[160px]">
                    <div class="flex justify-between items-start mb-4">
                        <p class="text-violet-100 font-bold text-lg">Total Sales Today</p>
                        <div class="bg-white/20 p-3.5 rounded-xl shadow-sm">
                            <i class="fa-solid fa-coins text-3xl"></i>
                        </div>
                    </div>
                    <h2 class="text-5xl font-black tracking-tight">LKR ${totalRevenue.toFixed(2)}</h2>
                </div>

                <div class="bg-white p-8 rounded-3xl shadow-sm border border-slate-200 flex flex-col justify-between min-h-[160px]">
                    <div class="flex justify-between items-start mb-4">
                        <p class="text-slate-500 font-bold text-lg">Pending Repairs</p>
                        <div class="bg-orange-50 text-orange-600 p-3.5 rounded-xl border border-orange-100 shadow-sm">
                            <i class="fa-solid fa-screwdriver-wrench text-3xl"></i>
                        </div>
                    </div>
                    <h2 class="text-5xl font-black text-slate-800 tracking-tight">${pendingRepairs}</h2>
                </div>

                <div class="bg-white p-8 rounded-3xl shadow-sm border border-slate-200 flex flex-col justify-between min-h-[160px]">
                    <div class="flex justify-between items-start mb-4">
                        <p class="text-slate-500 font-bold text-lg">Low Stock Items</p>
                        <div class="bg-red-50 text-red-600 p-3.5 rounded-xl border border-red-100 shadow-sm">
                            <i class="fa-solid fa-triangle-exclamation text-3xl"></i>
                        </div>
                    </div>
                    <h2 class="text-5xl font-black text-red-600 tracking-tight">${lowStockItems}</h2>
                </div>
            </div>

            <div class="grid grid-cols-1 xl:grid-cols-2 gap-8 fade-in h-full pb-10" style="animation-delay: 0.1s">
                <!-- Recent Sales -->
                <div class="bg-white p-8 rounded-3xl shadow-sm border border-slate-200">
                    <div class="flex justify-between items-center mb-6">
                        <h3 class="font-black text-xl text-slate-800"><i class="fa-solid fa-clock-rotate-left mr-2 text-violet-600"></i> Recent Sales</h3>
                        <button onclick="app.navigate('sales')" class="text-sm font-bold text-violet-600 hover:text-violet-800 transition-colors">View All &rarr;</button>
                    </div>
                    <div class="overflow-x-auto">
                        <table class="w-full text-base text-left">
                            <thead class="text-sm text-slate-500 uppercase bg-slate-50 font-extrabold tracking-wider">
                                <tr>
                                    <th class="px-5 py-4 rounded-tl-xl border-b border-slate-100">Time</th>
                                    <th class="px-5 py-4 border-b border-slate-100">Sale ID</th>
                                    <th class="px-5 py-4 border-b border-slate-100">Total</th>
                                    <th class="px-5 py-4 rounded-tr-xl border-b border-slate-100">Method</th>
                                </tr>
                            </thead>
                            <tbody class="divide-y divide-slate-100">
                                ${salesToday.length === 0 ? `<tr><td colspan="4" class="px-5 py-10 text-center text-slate-400 font-medium text-lg">No sales yet today</td></tr>` :
                salesToday.slice(-5).reverse().map(sale => `
                                    <tr class="hover:bg-slate-50 transition-colors">
                                        <td class="px-5 py-4 font-bold text-slate-700">
                                            ${new Date(sale.date).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                                        </td>
                                        <td class="px-5 py-4 text-slate-500 font-medium">#${sale.id}</td>
                                        <td class="px-5 py-4 font-black text-emerald-600 text-lg">LKR ${sale.total.toFixed(2)}</td>
                                        <td class="px-5 py-4">
                                            <span class="bg-slate-100 text-slate-700 px-3 py-1.5 rounded-md text-xs font-bold uppercase tracking-wider">${sale.paymentMethod}</span>
                                        </td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>

                <div class="bg-white p-8 rounded-3xl shadow-sm border border-slate-200">
                    <h3 class="font-black text-xl mb-6 text-slate-800"><i class="fa-solid fa-bolt mr-2 text-violet-600"></i> Quick Actions</h3>
                    <div class="grid grid-cols-2 md:grid-cols-3 gap-6 h-full pb-4">
                        <button onclick="app.navigate('pos')" class="p-6 bg-violet-50 hover:bg-violet-100 rounded-2xl text-violet-700 transition flex flex-col items-center justify-center gap-3 border border-violet-100 shadow-sm hover:-translate-y-1 hover:shadow-md min-h-[140px]">
                            <i class="fa-solid fa-cash-register text-5xl mb-2"></i>
                            <span class="font-bold text-lg">New Sale</span>
                        </button>
                        <button onclick="app.openRepairModal()" class="p-6 bg-orange-50 hover:bg-orange-100 rounded-2xl text-orange-700 transition flex flex-col items-center justify-center gap-3 border border-orange-100 shadow-sm hover:-translate-y-1 hover:shadow-md min-h-[140px]">
                            <i class="fa-solid fa-tools text-5xl mb-2"></i>
                            <span class="font-bold text-lg">New Repair</span>
                        </button>
                        <button onclick="app.navigate('products')" class="p-6 bg-blue-50 hover:bg-blue-100 rounded-2xl text-blue-700 transition flex flex-col items-center justify-center gap-3 border border-blue-100 shadow-sm hover:-translate-y-1 hover:shadow-md min-h-[140px]">
                            <i class="fa-solid fa-box-open text-5xl mb-2"></i>
                            <span class="font-bold text-lg">Add Stock</span>
                        </button>
                        <button onclick="app.openExpenseModal()" class="p-6 bg-red-50 hover:bg-red-100 rounded-2xl text-red-700 transition flex flex-col items-center justify-center gap-3 border border-red-100 shadow-sm hover:-translate-y-1 hover:shadow-md min-h-[140px]">
                             <i class="fa-solid fa-receipt text-5xl mb-2"></i>
                            <span class="font-bold text-lg">Log Expense</span>
                        </button>
                        <button onclick="app.navigate('utility')" class="p-6 bg-emerald-50 hover:bg-emerald-100 rounded-2xl text-emerald-700 transition flex flex-col items-center justify-center gap-3 border border-emerald-100 shadow-sm hover:-translate-y-1 hover:shadow-md min-h-[140px]">
                             <i class="fa-solid fa-bolt-lightning text-5xl mb-2"></i>
                            <span class="font-bold text-lg">Utility Pay</span>
                        </button>
                        <button onclick="app.navigate('bank')" class="p-6 bg-blue-50 hover:bg-blue-100 rounded-2xl text-blue-700 transition flex flex-col items-center justify-center gap-3 border border-blue-100 shadow-sm hover:-translate-y-1 hover:shadow-md min-h-[140px]">
                             <i class="fa-solid fa-building-columns text-5xl mb-2"></i>
                            <span class="font-bold text-lg">Bank Tracker</span>
                        </button>
                        <button onclick="app.navigate('suppliers')" class="p-6 bg-orange-50 hover:bg-orange-100 rounded-2xl text-orange-700 transition flex flex-col items-center justify-center gap-3 border border-orange-100 shadow-sm hover:-translate-y-1 hover:shadow-md min-h-[140px]">
                             <i class="fa-solid fa-truck-field text-5xl mb-2"></i>
                            <span class="font-bold text-lg">Suppliers</span>
                        </button>
                    </div>
                </div>
            </div>
        `;
        document.getElementById('app-content').innerHTML = html;
    },

    // --- POS & SALES ---
    // Helper for category visuals
    getCategoryDetails: (category) => {
        const details = {
            'Accessories': { icon: 'fa-headphones', bg: 'bg-pink-100', text: 'text-pink-800', border: 'border-pink-500' },
            'Mobile Phones': { icon: 'fa-mobile-screen-button', bg: 'bg-blue-100', text: 'text-blue-800', border: 'border-blue-500' },
            'Stationery': { icon: 'fa-pen-ruler', bg: 'bg-yellow-100', text: 'text-yellow-800', border: 'border-yellow-500' },
            'Service': { icon: 'fa-screwdriver-wrench', bg: 'bg-orange-100', text: 'text-orange-800', border: 'border-orange-500' },
            'Studio': { icon: 'fa-camera', bg: 'bg-purple-100', text: 'text-purple-800', border: 'border-purple-500' },
            'Chargers': { icon: 'fa-bolt', bg: 'bg-teal-100', text: 'text-teal-800', border: 'border-teal-500' },
            'Cable': { icon: 'fa-plug', bg: 'bg-cyan-100', text: 'text-cyan-800', border: 'border-cyan-500' },
            'Book': { icon: 'fa-book', bg: 'bg-indigo-100', text: 'text-indigo-800', border: 'border-indigo-500' },
            'Photoframe': { icon: 'fa-image', bg: 'bg-rose-100', text: 'text-rose-800', border: 'border-rose-500' },
            'Chargers & Cable': { icon: 'fa-charging-station', bg: 'bg-emerald-100', text: 'text-emerald-800', border: 'border-emerald-500' },
            'Button Phone': { icon: 'fa-phone', bg: 'bg-slate-200', text: 'text-slate-800', border: 'border-slate-500' }
        };
        return details[category] || { icon: 'fa-layer-group', bg: 'bg-violet-100', text: 'text-violet-800', border: 'border-violet-500' };
    },

    renderPOS: async () => {
        try {
            const items = await db.items.toArray();
            const creditors = await db.creditors.where('type').equals('receivable').toArray();
            const categories = [...new Set(items.map(item => item.category))].sort();
            const settingsList = await db.categorySettings.toArray();
            const categoryMap = settingsList.reduce((acc, curr) => {
                acc[curr.name] = curr.image;
                return acc;
            }, {});

            const activeCategory = app.state.posCategory || 'All';
            const selectedCreditor = app.state.selectedCreditor;

            const html = `
                <div class="flex flex-col xl:flex-row gap-6 fade-in h-[calc(100vh-8rem)]">
                    <!-- Left: Categories -->
                    <div class="w-full xl:w-72 flex flex-col bg-white rounded-2xl shadow-sm border border-slate-200 overflow-hidden shrink-0">
                        <div class="p-4 bg-slate-50 border-b border-slate-100">
                            <h3 class="font-bold text-slate-700 text-sm tracking-tight uppercase">Categories</h3>
                        </div>
                        <div class="flex-1 overflow-y-auto p-3 space-y-2">
                            <button onclick="app.setPOSCategory('All')" class="w-full p-3 rounded-xl border ${activeCategory === 'All' ? 'bg-violet-600 border-violet-600 text-white font-bold' : 'bg-slate-50 border-slate-100 text-slate-600 hover:bg-slate-100'} transition-all text-left flex items-center gap-3">
                                <i class="fa-solid fa-border-all"></i> All Items
                            </button>
                            ${categories.map(cat => `
                                <button onclick="app.setPOSCategory('${cat}')" class="w-full p-3 rounded-xl border ${activeCategory === cat ? 'bg-violet-600 border-violet-600 text-white font-bold' : 'bg-slate-50 border-slate-100 text-slate-600 hover:bg-slate-100'} transition-all text-left flex items-center gap-3 truncate">
                                    <i class="fa-solid fa-tag opacity-50"></i> ${cat}
                                </button>
                            `).join('')}
                        </div>
                    </div>

                    <!-- Center: Items & Customer Info -->
                    <div class="flex-1 flex flex-col min-w-0">
                        <!-- Customer Selection Bar (High Visibility) -->
                        <div class="mb-4 bg-yellow-50 border-2 border-yellow-200 p-3 rounded-2xl flex items-center gap-4 shadow-sm">
                            <div class="w-10 h-10 rounded-full bg-yellow-400 text-white flex items-center justify-center shrink-0">
                                <i class="fa-solid fa-user-tag text-lg"></i>
                            </div>
                            <div class="flex-1">
                                <p class="text-[10px] font-black text-yellow-700 uppercase tracking-widest leading-none mb-1">ගනුදෙනුකරු තෝරන්න (Select Customer from Naya Potha)</p>
                                <div class="flex items-center gap-2">
                                    <select onchange="app.setPOSCustomer(this.value)" class="flex-1 bg-transparent border-none p-0 focus:ring-0 text-lg font-black text-slate-800 cursor-pointer appearance-none">
                                        <option value="">අත්පිට මුදලට (Walk-in Customer - Cash Sale)</option>
                                        ${creditors.map(c => `<option value="${c.id}" ${selectedCreditor?.id === c.id ? 'selected' : ''}>${c.name} - හිඟ මුදල: LKR ${c.amount}</option>`).join('')}
                                    </select>
                                    <button onclick="app.openCustomerSearch()" class="text-yellow-700 hover:text-yellow-900 bg-yellow-200/50 hover:bg-yellow-200 p-2 rounded-lg transition-all" title="Search Customer">
                                        <i class="fa-solid fa-magnifying-glass"></i>
                                    </button>
                                    ${selectedCreditor ? `
                                        <button onclick="app.updateCreditorAmount(${selectedCreditor.id}, -1)" class="bg-emerald-600 text-white px-3 py-2 rounded-lg text-[10px] font-black uppercase tracking-widest hover:bg-emerald-700 transition-all flex items-center gap-1 shadow-sm">
                                            <i class="fa-solid fa-hand-holding-dollar"></i> Paid
                                        </button>
                                    ` : ''}
                                </div>
                            </div>
                            <button onclick="app.openCreditorModal('receivable')" class="bg-white hover:bg-yellow-100 text-yellow-700 px-4 py-2 rounded-xl text-xs font-black uppercase tracking-widest border border-yellow-200 transition-all flex items-center gap-2">
                                <i class="fa-solid fa-plus"></i> New Customer
                            </button>
                        </div>

                        <!-- Top Search & Info -->
                        <div class="mb-4 flex gap-4">
                            <div class="relative flex-1">
                                <i class="fa-solid fa-magnifying-glass absolute left-4 top-1/2 -translate-y-1/2 text-slate-400"></i>
                                <input id="pos-search" type="text" placeholder="Search items..." class="w-full pl-11 pr-4 py-3 rounded-xl border border-slate-200 focus:outline-none focus:ring-1 focus:ring-violet-500 shadow-sm" oninput="app.filterPOSItems(this.value)" onkeydown="if(event.key==='Enter') app.handlePOSSearchEnter(this.value)">
                            </div>
                            <!-- Quick Amount Input -->
                            <div class="relative w-44">
                                <span class="absolute left-4 top-1/2 -translate-y-1/2 text-slate-400 font-bold text-sm">Rs.</span>
                                <input id="quick-amount-input" type="number" placeholder="Price" class="w-full pl-11 pr-4 py-3 rounded-xl border-2 border-violet-200 focus:border-violet-600 focus:outline-none focus:ring-0 font-black text-violet-700 shadow-sm bg-violet-50/30" onkeydown="if(event.key==='Enter') app.addDirectAmount(this.value)">
                            </div>
                            <button onclick="app.startScanner()" class="bg-white border border-slate-200 p-3 rounded-xl text-slate-500 hover:text-violet-600 transition-colors">
                                <i class="fa-solid fa-camera text-xl"></i>
                            </button>
                        </div>

                        <!-- Grid -->
                        <div id="pos-grid" class="flex-1 overflow-y-auto p-1">
                            <div class="grid grid-cols-2 md:grid-cols-3 2xl:grid-cols-4 gap-4 pb-20">
                                ${app.generatePOSGrid(items, activeCategory)}
                            </div>
                        </div>
                    </div>

                    <!-- Right: Cart Summary -->
                    <div class="w-full xl:w-96 flex flex-col bg-white rounded-2xl shadow-xl border border-slate-200 overflow-hidden shrink-0">
                        <div class="p-4 bg-slate-50 border-b border-slate-100 flex justify-between items-center">
                            <h3 class="font-bold text-slate-700 text-sm uppercase">Current Cart</h3>
                            <button onclick="app.clearCart()" class="text-[10px] text-red-500 font-bold uppercase hover:underline">Clear</button>
                        </div>
                        <div id="cart-items" class="flex-1 overflow-y-auto p-4 space-y-3">
                            <!-- Items Injected Here -->
                        </div>
                        <div id="cart-totals-area" class="p-6 bg-slate-50 border-t border-slate-100">
                            <!-- Totals Injected Here -->
                        </div>
                    </div>
                </div>
            `;
            document.getElementById('app-content').innerHTML = html;
            app.renderPOSCart();
            setTimeout(() => document.getElementById('pos-search')?.focus(), 100);
        } catch (err) {
            console.error("POS Render Error:", err);
            document.getElementById('app-content').innerHTML = `<div class="p-10 text-center text-red-500 font-bold">Error loading POS: ${err.message}</div>`;
        }
    },

    editCategoryPhoto: async (categoryName) => {
        const { value: file } = await Swal.fire({
            title: `Select Photo for ${categoryName}`,
            input: 'file',
            inputAttributes: {
                'accept': 'image/*',
                'aria-label': 'Upload your category picture'
            },
            showCancelButton: true
        });

        if (file) {
            const reader = new FileReader();
            reader.onload = async (e) => {
                const base64Image = e.target.result;
                try {
                    await db.categorySettings.put({ name: categoryName, image: base64Image });
                    Swal.fire({
                        icon: 'success',
                        title: 'Photo updated!',
                        showConfirmButton: false,
                        timer: 1500
                    });
                    app.renderPOS(); // Re-render to show updated photo
                } catch (error) {
                    Swal.fire('Error', 'Failed to save photo', 'error');
                }
            };
            reader.readAsDataURL(file);
        }
    },

    generatePOSGrid: (items, category = 'All') => {
        let filtered = items;
        if (category && category !== 'All') {
            filtered = items.filter(i => i.category === category);
        }

        if (filtered.length === 0) {
            return `
                <div class="col-span-full flex flex-col items-center justify-center py-12 text-slate-400">
                    <i class="fa-solid fa-box-open text-4xl mb-3 opacity-50"></i>
                    <p>No items found in this category.</p>
                </div>
            `;
        }

        return filtered.map(item => `
            <div onclick="app.addToCart(${typeof item.id === 'string' ? `'${item.id}'` : item.id})" class="bg-white rounded-2xl shadow-sm border border-slate-100 hover:shadow-xl hover:border-violet-400 cursor-pointer transition-all active:scale-95 group relative flex flex-col h-auto min-h-[240px] sm:min-h-[280px] overflow-hidden ${item.stock === 0 && item.type === 'product' ? 'opacity-50' : ''}">
                
                <!-- Large Image Section -->
                <div class="h-36 sm:h-48 w-full relative bg-slate-50 flex items-center justify-center border-b border-slate-100 flex-shrink-0 group-hover:bg-slate-100 transition-colors">
                    ${item.image ?
                `<img src="${item.image}" class="w-full h-full object-cover transform group-hover:scale-105 transition-transform duration-500">` :
                `<div class="h-full w-full ${item.type === 'service' ? 'bg-orange-50 text-orange-300 group-hover:text-orange-400' : 'bg-blue-50 text-blue-300 group-hover:text-blue-400'} flex flex-col items-center justify-center transition-colors">
                            <i class="fa-solid ${item.type === 'service' ? 'fa-bolt' : 'fa-box'} text-6xl mb-2 transform group-hover:scale-110 transition-transform duration-500"></i>
                        </div>`
            }
                    
                    ${item.type === 'product' ?
                `<span class="absolute top-3 right-3 text-[10px] sm:text-xs font-bold px-3 py-1.5 rounded-full shadow-md bg-white/90 backdrop-blur-sm border border-white/50 ${item.stock <= (item.minStock || 5) ? 'text-red-600' : 'text-slate-700'}">
                            ${item.stock} left
                         </span>`
                : ''}
                </div>

                <!-- Text & Price Section -->
                <div class="p-4 sm:p-5 flex flex-col flex-1 justify-between bg-white relative z-10 w-full">
                    <h4 class="font-extrabold text-slate-800 text-base sm:text-lg leading-tight mb-3 line-clamp-2" title="${item.name}">${item.name}</h4>
                    
                    <div class="flex justify-between items-end mt-auto pt-2 border-t border-slate-50">
                        <p class="text-violet-700 font-black text-xl sm:text-2xl">LKR ${item.price}</p>
                    </div>

                    <!-- Floating Add Button -->
                    <div class="absolute bottom-4 right-4 opacity-0 group-hover:opacity-100 transition-all duration-300 bg-violet-600 text-white w-8 h-8 sm:w-10 sm:h-10 rounded-full flex items-center justify-center shadow-lg transform translate-y-4 group-hover:translate-y-0">
                        <i class="fa-solid fa-plus sm:text-lg"></i>
                    </div>
                </div>
            </div>
        `).join('');
    },

    setPOSCategory: (category) => {
        app.state.posCategory = category;

        // Update active classes on buttons
        document.querySelectorAll('.pos-category-btn').forEach(btn => {
            const bgClass = btn.getAttribute('data-bg');
            const textClass = btn.getAttribute('data-text');
            const borderClass = btn.getAttribute('data-border');

            if (btn.getAttribute('data-category') === category) {
                btn.className = `pos-category-btn w-full h-full flex flex-col items-center justify-center p-5 rounded-xl transition-all min-h-[140px] text-center ${bgClass} ${textClass} font-bold border-2 ${borderClass} shadow-md active`;
            } else {
                btn.className = `pos-category-btn w-full h-full flex flex-col items-center justify-center p-5 rounded-xl transition-all min-h-[140px] text-center ${bgClass} ${textClass} hover:opacity-80 border border-transparent shadow-sm`;
            }
        });

        // Update the grid directly instead of full render
        const searchInput = document.getElementById('pos-search');
        app.filterPOSItems(searchInput ? searchInput.value : '');
    },

    filterPOSItems: async (query = '') => {
        const allItems = await db.items.toArray();
        let filtered = allItems;
        const activeCategory = app.state.posCategory || 'All'; // Default to All

        if (activeCategory !== 'All') {
            filtered = filtered.filter(i => i.category === activeCategory);
        }

        if (query) {
            const lowerQ = query.toLowerCase();
            // Check for exact barcode match first for scanning
            const barcodeMatch = filtered.find(i => i.barcode === query);
            if (barcodeMatch) {
                app.addToCart(barcodeMatch.id);
                document.getElementById('pos-search').value = '';
                return;
            }
            filtered = filtered.filter(i => 
                i.name.toLowerCase().includes(lowerQ) || 
                (i.barcode && i.barcode.toLowerCase().includes(lowerQ)) ||
                (i.price.toString() === query || i.price.toString().startsWith(query))
            );

            // Add virtual item if query looks like a valid number and it's not and exact match for an existing item barcode/price
            const numQuery = parseFloat(query);
            if (!isNaN(numQuery) && numQuery > 0) {
                // If it's a number, we also keep the "Custom Amount" card at the top
                filtered.unshift({
                    id: 'custom-' + numQuery, // Virtual ID, parsed in addToCart
                    name: 'Custom Amount',
                    price: numQuery,
                    category: 'Service',
                    type: 'service',
                    stock: 0
                });
            }
        }

        const gridHTML = app.generatePOSGrid(filtered, null);

        const posGrid = document.getElementById('pos-grid');
        if (posGrid) {
            posGrid.innerHTML = `
                <div class="mb-4 flex items-center justify-between">
                    <h2 class="text-xl font-bold text-slate-800 flex items-center">
                        <span class="text-slate-400 mr-2 font-normal">Category:</span> ${activeCategory}
                    </h2>
                    <span class="bg-slate-100 text-slate-600 px-3 py-1 rounded-full text-sm font-medium">
                        ${query ? 'Search Results' : filtered.length + ' results'}
                    </span>
                </div>
                <div class="grid grid-cols-2 sm:grid-cols-2 md:grid-cols-3 2xl:grid-cols-4 gap-6 content-start pb-20">
                    ${gridHTML}
                </div>
            `;
        }
    },

    handlePOSSearchEnter: async (query) => {
        if (!query) return;

        // If it's a number, and we have custom amount or exact price match, we handle it
        const allItems = await db.items.toArray();
        const activeCategory = app.state.posCategory || 'All';
        let filtered = activeCategory === 'All' ? allItems : allItems.filter(i => i.category === activeCategory);

        const lowerQ = query.toLowerCase();
        
        // 1. Check exact barcode match
        const barcodeMatch = filtered.find(i => i.barcode === query);
        if (barcodeMatch) {
            app.addToCart(barcodeMatch.id);
            document.getElementById('pos-search').value = '';
            app.filterPOSItems('');
            return;
        }

        // 2. Check if it's a pure number - if so, add as custom amount
        const numQuery = parseFloat(query);
        const nameMatches = filtered.filter(i => i.name.toLowerCase().includes(lowerQ));
        
        if (!isNaN(numQuery) && numQuery > 0 && nameMatches.length === 0) {
            // It's a number and no name matches, so add as custom amount
            app.addDirectAmount(query);
            document.getElementById('pos-search').value = '';
            app.filterPOSItems('');
            return;
        }

        // 3. If there is exactly one match in the filtered list, add it
        const priceMatches = filtered.filter(i => i.price.toString() === query);
        const results = [...nameMatches, ...priceMatches];
        // Remove duplicates if any
        const uniqueResults = [...new Map(results.map(item => [item.id, item])).values()];

        if (uniqueResults.length === 1) {
            app.addToCart(uniqueResults[0].id);
            document.getElementById('pos-search').value = '';
            app.filterPOSItems('');
        }
    },

    addDirectAmount: (amountStr) => {
        const amount = parseFloat(amountStr);
        if (isNaN(amount) || amount <= 0) return;

        const tempItem = {
            id: 'custom-' + Date.now() + '-' + Math.floor(Math.random() * 1000),
            name: 'Service',
            price: amount,
            type: 'service',
            qty: 1
        };
        app.state.cart.push(tempItem);

        // Reset input and re-render
        const input = document.getElementById('quick-amount-input');
        if (input) {
            input.value = '';
            input.focus();
        }
        app.renderPOSCart();
    },

    openCustomItemModal: async () => {
        const { value: price } = await Swal.fire({
            title: 'මුදල ඇතුළත් කරන්න (Enter Amount)',
            input: 'number',
            inputPlaceholder: '0.00',
            showCancelButton: true,
            confirmButtonText: 'එකතු කරන්න (Add)',
            confirmButtonColor: '#7c3aed',
            inputValidator: (value) => {
                if (!value || isNaN(value) || parseFloat(value) <= 0) {
                    return 'කරුණාකර නිවැරදි මිලක් ඇතුළත් කරන්න';
                }
            }
        });

        if (price) {
            const numericPrice = parseFloat(price);
            const tempItem = {
                id: 'custom-' + Date.now(),
                name: 'Service', // Default name as requested
                price: numericPrice,
                type: 'service',
                qty: 1
            };
            app.state.cart.push(tempItem);
            app.renderPOSCart();
        }
    },

    editCartItemPrice: async (index) => {
        const item = app.state.cart[index];
        const { value: newPrice } = await Swal.fire({
            title: 'Edit Price',
            input: 'number',
            inputLabel: `Current: LKR ${item.price}`,
            inputValue: item.price,
            showCancelButton: true,
            inputValidator: (value) => {
                if (!value || value < 0) {
                    return 'Please enter a valid price!';
                }
            }
        });

        if (newPrice !== null) {
            item.price = parseFloat(newPrice);
            app.renderPOSCart();
        }
    },

    addToCart: async (id) => {
        let item;
        if (typeof id === 'string' && id.startsWith('custom-')) {
            const price = parseFloat(id.split('-')[1]);
            item = {
                id: 'custom-' + Date.now() + '-' + Math.floor(Math.random() * 1000),
                name: 'Custom Amount',
                price: price,
                type: 'service',
                stock: 0
            };
        } else {
            const numericId = Number(id);
            item = await db.items.get(numericId);
        }

        if (!item) return;

        // Check stock for products
        if (item.type === 'product' && item.stock <= 0) {
            Swal.fire({ icon: 'error', title: 'Out of Stock', text: 'This item is currently out of stock.', timer: 1500, showConfirmButton: false });
            return;
        }

        const existing = app.state.cart.find(i => i.id === item.id);
        if (existing) {
            // Check if adding one more exceeds stock (only for products)
            if (item.type === 'product' && existing.qty + 1 > item.stock) {
                Swal.fire({ icon: 'warning', title: 'Insufficient Stock', text: `Only ${item.stock} items available.`, timer: 1500, showConfirmButton: false });
                return;
            }
            existing.qty++;
        } else {
            app.state.cart.push({ ...item, qty: 1 });
        }
        app.renderPOSCart();
    },

    renderPOSCart: () => {
        const cartContainer = document.getElementById('cart-items');
        if (cartContainer) {
            cartContainer.innerHTML = app.state.cart.length === 0 ?
                `<div class="h-full flex flex-col items-center justify-center text-slate-400">
                    <i class="fa-solid fa-basket-shopping text-5xl mb-4 text-slate-200"></i>
                    <p class="font-medium">Cart is empty</p>
                </div>` :
                app.state.cart.map((item, index) => `
                    <div class="flex flex-col bg-slate-50 p-3 rounded-xl border border-slate-100 group hover:border-violet-200 transition-colors">
                        <div class="flex justify-between items-start mb-2">
                            <p class="font-bold text-slate-800 text-sm line-clamp-2 leading-snug">${item.name}</p>
                            <button onclick="app.removeFromCart(${index})" class="text-slate-300 hover:text-red-500 ml-2 transition-colors"><i class="fa-solid fa-xmark"></i></button>
                        </div>
                        <div class="flex justify-between items-end">
                            <div class="text-xs text-slate-500">
                                ${item.price.toFixed(2)} x ${item.qty}
                            </div>
                            <div class="flex items-center gap-3">
                                <div class="flex items-center bg-white rounded-lg border border-slate-200 shadow-sm h-7">
                                    <button onclick="app.updateCartQty(${index}, -1)" class="w-7 h-full flex items-center justify-center text-slate-500 hover:bg-slate-100 rounded-l-lg transition-colors">-</button>
                                    <span class="text-xs font-bold w-6 text-center select-none">${item.qty}</span>
                                    <button onclick="app.updateCartQty(${index}, 1)" class="w-7 h-full flex items-center justify-center text-slate-500 hover:bg-slate-100 rounded-r-lg transition-colors">+</button>
                                </div>
                                <span class="font-bold text-xs text-violet-700 w-16 text-right">${(item.price * item.qty).toFixed(2)}</span>
                            </div>
                        </div>
                    </div>
                `).join('');

            const totalsArea = document.getElementById('cart-totals-area');
            const cartTotal = app.calculateTotal();
            const discount = app.state.discount || 0;
            const netCartAmount = cartTotal - discount;
            const creditor = app.state.selectedCreditor;

            if (totalsArea) {
                totalsArea.innerHTML = `
                    <div class="space-y-2 mb-4">
                        <div class="flex justify-between text-xs text-slate-500 font-bold uppercase tracking-wider">
                            <span>Cart Subtotal</span>
                            <span>LKR ${cartTotal.toFixed(2)}</span>
                        </div>
                        <div class="flex justify-between text-xs text-slate-500 font-bold uppercase tracking-wider">
                            <span>Discount</span>
                            <button class="text-blue-600 hover:underline" onclick="app.applyDiscount()">
                                ${discount > 0 ? '- LKR ' + discount.toFixed(2) : 'Add Discount'}
                            </button>
                        </div>
                        ${creditor ? `
                            <div class="flex justify-between text-xs text-slate-500 font-bold uppercase tracking-wider bg-red-50 p-2 rounded-lg border border-red-100 mt-2">
                                <span class="text-red-600">Old Debt (${creditor.name})</span>
                                <span class="text-red-700 font-black">LKR ${creditor.amount.toFixed(2)}</span>
                            </div>
                        ` : ''}
                    </div>
                    
                    <div class="flex justify-between items-center mb-6 pt-4 border-t border-slate-200">
                        <div>
                            <span class="block text-[10px] font-black text-slate-400 uppercase tracking-widest leading-none mb-1">Total Outstanding</span>
                            <span class="text-2xl font-black text-violet-700">LKR ${(netCartAmount + (creditor ? creditor.amount : 0)).toFixed(2)}</span>
                        </div>
                    </div>
                    
                    <button onclick="app.processCheckout()" class="w-full bg-slate-900 hover:bg-slate-800 text-white py-4 rounded-xl font-bold text-lg shadow-xl shadow-slate-200 transition-all transform active:scale-95 disabled:opacity-50 disabled:cursor-not-allowed" ${app.state.cart.length === 0 ? 'disabled' : ''}>
                        ${creditor ? 'Update & Checkout' : 'Charge LKR ' + netCartAmount.toFixed(2)}
                    </button>
                `;
            }
        }
    },

    updateCartQty: (index, change) => {
        const item = app.state.cart[index];
        const newQty = item.qty + change;

        if (newQty <= 0) {
            app.removeFromCart(index);
            return;
        }

        // Check stock limit for products
        if (item.type === 'product' && newQty > item.stock) {
            Swal.fire({ icon: 'warning', title: 'Max Stock Reached', timer: 1000, showConfirmButton: false });
            return;
        }

        item.qty = newQty;
        app.renderPOSCart();
    },

    removeFromCart: (index) => {
        app.state.cart.splice(index, 1);
        app.renderPOSCart();
    },

    clearCart: () => {
        app.state.cart = [];
        app.state.discount = 0;
        app.renderPOSCart();
    },

    calculateTotal: () => {
        return app.state.cart.reduce((sum, item) => sum + (item.price * item.qty), 0);
    },

    applyDiscount: async () => {
        const { value: discount } = await Swal.fire({
            title: 'Enter Discount Amount',
            input: 'number',
            inputLabel: 'Amount in LKR',
            inputValue: app.state.discount || 0,
            showCancelButton: true
        });

        if (discount !== null) {
            app.state.discount = parseFloat(discount);
            app.renderPOSCart();
        }
    },

    setPOSCustomer: async (id) => {
        const prevSearch = document.getElementById('pos-search')?.value || '';
        if (!id) {
            app.state.selectedCreditor = null;
        } else {
            app.state.selectedCreditor = await db.creditors.get(Number(id));
        }
        await app.renderPOS(); 
        if (prevSearch) {
            const newSearch = document.getElementById('pos-search');
            if (newSearch) {
                newSearch.value = prevSearch;
                app.filterPOSItems(prevSearch);
            }
        }
    },

    openCustomerSearch: async () => {
        const creditors = await db.creditors.where('type').equals('receivable').toArray();
        
        const { value: selectedId } = await Swal.fire({
            title: 'ගනුදෙනුකරු සොයන්න (Search Customer)',
            html: `
                <div class="text-left">
                    <input id="swal-cust-search" class="swal2-input !mt-0 !w-full" placeholder="නම හෝ ණය මුදල සොයන්න..." oninput="app.filterSwalCustomers(this.value)">
                    <div id="swal-cust-list" class="mt-4 max-h-[300px] overflow-y-auto divide-y divide-slate-100 border rounded-xl">
                        <div onclick="Swal.clickConfirm(); app.swalSelectedId = ''" class="p-4 hover:bg-slate-50 cursor-pointer flex justify-between items-center transition-colors">
                            <span class="font-bold text-slate-700">අත්පිට මුදල (Cash Sale)</span>
                            <span class="text-xs bg-emerald-100 text-emerald-700 px-2 py-1 rounded-full font-bold">Standard</span>
                        </div>
                        ${creditors.map(c => `
                            <div onclick="Swal.clickConfirm(); app.swalSelectedId = '${c.id}'" class="cust-item p-4 hover:bg-slate-50 cursor-pointer flex justify-between items-center transition-colors" data-name="${c.name.toLowerCase()}" data-amount="${c.amount}">
                                <div>
                                    <p class="font-bold text-slate-800">${c.name}</p>
                                    <p class="text-[10px] text-slate-400 font-bold uppercase tracking-wider">Customer / Debtor</p>
                                </div>
                                <div class="text-right">
                                    <p class="font-black text-red-600">LKR ${c.amount}</p>
                                    <p class="text-[10px] text-slate-400 font-bold uppercase tracking-wider">Outstanding</p>
                                </div>
                            </div>
                        `).join('')}
                    </div>
                </div>
            `,
            showConfirmButton: false,
            showCancelButton: true,
            cancelButtonText: 'Cancel',
            didOpen: () => {
                document.getElementById('swal-cust-search').focus();
            },
            preConfirm: () => {
                return app.swalSelectedId;
            }
        });

        if (selectedId !== undefined) {
            app.setPOSCustomer(selectedId);
        }
    },

    filterSwalCustomers: (query) => {
        const lowerQ = query.toLowerCase();
        document.querySelectorAll('.cust-item').forEach(el => {
            const name = el.getAttribute('data-name');
            const amount = el.getAttribute('data-amount');
            if (name.includes(lowerQ) || amount.includes(lowerQ)) {
                el.style.display = 'flex';
            } else {
                el.style.display = 'none';
            }
        });
    },

    processCheckout: async () => {
        if (app.state.cart.length === 0) return;

        const subTotal = app.calculateTotal();
        const discount = app.state.discount || 0;
        const cartTotal = subTotal - discount;
        const creditor = app.state.selectedCreditor;
        const currentDebt = creditor ? creditor.amount : 0;
        const totalOutstanding = cartTotal + currentDebt;

        let amountPaid = 0;
        let paymentMethod = 'cash';

        if (creditor) {
            const { value: paidVal } = await Swal.fire({
                title: 'ගෙවීම් සටහන් කිරීම (Payment Record)',
                html: `
                    <div class="space-y-4 text-left">
                        <div class="bg-slate-50 p-4 rounded-xl border border-slate-100 space-y-2">
                            <div class="flex justify-between text-sm">
                                <span class="text-slate-500 font-bold uppercase tracking-widest text-[10px]">නව බිල්පත (New Bill)</span>
                                <span class="font-black text-slate-700">LKR ${cartTotal.toFixed(2)}</span>
                            </div>
                            <div class="flex justify-between text-sm">
                                <span class="text-slate-500 font-bold uppercase tracking-widest text-[10px]">පැරණි ණය (Previous Debt)</span>
                                <span class="font-black text-red-600">LKR ${currentDebt.toFixed(2)}</span>
                            </div>
                            <div class="pt-2 border-t border-slate-200 flex justify-between">
                                <span class="text-slate-700 font-black uppercase tracking-widest text-xs">මුළු හිඟ මුදල (Total Due)</span>
                                <span class="font-black text-violet-700 text-lg">LKR ${totalOutstanding.toFixed(2)}</span>
                            </div>
                        </div>

                        <div class="relative">
                            <label class="block text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1 ml-1">අද ලැබුණු මුදල (Amount Received Now)</label>
                            <div class="relative">
                                <span class="absolute left-4 top-1/2 -translate-y-1/2 font-bold text-slate-400">LKR</span>
                                <input id="swal-paid" type="number" class="swal2-input !m-0 !pl-14 !w-full !text-2xl !font-black !text-emerald-600" value="${cartTotal.toFixed(0)}">
                            </div>
                            <div class="flex gap-2 mt-2">
                                <button type="button" onclick="document.getElementById('swal-paid').value = '0'" class="flex-1 py-2 bg-red-50 text-red-600 rounded-lg text-xs font-bold border border-red-100 uppercase tracking-widest">පරිපූර්ණ ණය (Full Credit)</button>
                                <button type="button" onclick="document.getElementById('swal-paid').value = '${cartTotal.toFixed(0)}'" class="flex-1 py-2 bg-emerald-50 text-emerald-600 rounded-lg text-xs font-bold border border-emerald-100 uppercase tracking-widest">සියල්ල ගෙව්වා (Full Paid)</button>
                            </div>
                            <p class="text-[10px] text-slate-400 font-medium mt-3 ml-1">මුළු හිග මුදලින් අද ලැබෙන මුදල ඇතුළත් කරන්න. (Enter amount paid towards total debt.)</p>
                        </div>
                    </div>
                `,
                showCancelButton: true,
                confirmButtonText: 'වාර්තාව සුරකින්න (Record Sale)',
                confirmButtonColor: '#7c3aed',
                preConfirm: () => {
                    const val = parseFloat(document.getElementById('swal-paid').value);
                    if (isNaN(val) || val < 0) {
                        Swal.showValidationMessage('කරුණාකර නිවැරදි මුදලක් ඇතුළත් කරන්න');
                        return false;
                    }
                    return val;
                }
            });

            if (paidVal === undefined) return;
            amountPaid = paidVal;
            paymentMethod = 'credit';
        } else {
            const { value: method } = await Swal.fire({
                title: 'Select Payment Method',
                input: 'radio',
                inputOptions: { 'cash': 'Cash', 'card': 'Card', 'transfer': 'Bank Transfer' },
                inputValue: 'cash',
                showCancelButton: true
            });
            if (!method) return;
            paymentMethod = method;
            amountPaid = cartTotal;
        }

        try {
            const saleRecord = {
                date: new Date().toISOString(),
                items: JSON.parse(JSON.stringify(app.state.cart)),
                subTotal,
                discount,
                total: cartTotal,
                amountPaid,
                paymentMethod,
                creditorId: creditor ? creditor.id : null,
                customerName: creditor ? creditor.name : '',
                customerPhone: creditor ? (creditor.contact || creditor.phone || '') : ''
            };

            // 1. Save Sale to Dexie Local Store
            const saleId = await db.sales.add(saleRecord);

            // 2. Update Creditor Balance if applicable
            if (creditor) {
                const newDebt = totalOutstanding - amountPaid;
                const updatedCred = { 
                    ...creditor,
                    amount: newDebt,
                    lastUpdated: new Date().toISOString() 
                };
                await db.creditors.update(creditor.id, { 
                    amount: newDebt,
                    lastUpdated: updatedCred.lastUpdated 
                });
                app.apiCall(`/api/creditors/${creditor.id}`, 'PUT', updatedCred, 'update_creditor', creditor.id);
            }

            // 3. Update Inventory Stock
            for (const item of app.state.cart) {
                if (item.type === 'product' && item.id && !String(item.id).startsWith('custom-')) {
                    const dbItem = await db.items.get(item.id);
                    if (dbItem) {
                        await db.items.update(item.id, { stock: Math.max(0, dbItem.stock - item.qty) });
                    }
                }
            }

            // 4. Send Sale to Server with Socket Header (broadcasts to all other devices)
            app.apiCall('/api/sales', 'POST', { id: saleId, ...saleRecord }, 'create_sale');

            // 5. Success & Cleanup
            app.state.cart = [];
            app.state.discount = 0;
            app.state.selectedCreditor = null;
            app.renderPOS();

            Swal.fire({
                icon: 'success',
                title: 'Sale Finished',
                text: creditor ? `Customer Balance: LKR ${(totalOutstanding - amountPaid).toFixed(2)}` : 'Payment confirmed.',
                timer: 2000,
                showConfirmButton: false,
                toast: true,
                position: 'top-end'
            });

            const printResult = await Swal.fire({
                title: 'Print Receipt?',
                icon: 'question',
                showCancelButton: true,
                confirmButtonText: 'Yes, Print',
                cancelButtonText: 'Done'
            });

            if (printResult.isConfirmed) {
                app.printReceipt(saleId);
            }
        } catch (error) {
            console.error(error);
            Swal.fire('Error', 'Checkout failed', 'error');
        }
    },

    // --- SCANNERS & GENERATORS ---
    startScanner: () => {
        // Open scanner modal
        Swal.fire({
            title: 'Scan Barcode',
            html: `
                <div id="reader" class="w-full"></div>
                <p class="text-xs text-slate-500 mt-2">Point camera at barcode</p>
            `,
            showConfirmButton: false,
            showCancelButton: true,
            cancelButtonText: 'Cancel',
            width: '600px',
            didOpen: () => {
                try {
                    app.scanner = new Html5QrcodeScanner("reader", {
                        fps: 10,
                        qrbox: { width: 250, height: 250 },
                        aspectRatio: 1.0
                    }, /* verbose= */ false);

                    app.scanner.render((decodedText) => {
                        // Success callback
                        if (app.scanner) {
                            app.scanner.clear();
                            app.scanner = null;
                        }
                        Swal.close();

                        // Play beep sound
                        const audio = new Audio('https://assets.mixkit.co/active_storage/sfx/2578/2578-preview.mp3');
                        audio.play().catch(e => console.log('Audio play failed', e));

                        // Process scan
                        app.handleScan(decodedText);
                    }, (errorMessage) => {
                        // parse error, ignore
                    });
                } catch (e) {
                    console.error("Scanner error:", e);
                    Swal.fire('Error', 'Could not start scanner. Please ensure camera permissions are granted.', 'error');
                }
            },
            willClose: () => {
                if (app.scanner) {
                    app.scanner.clear().catch(e => console.error("Failed to clear scanner", e));
                    app.scanner = null;
                }
            }
        });
    },

    handleScan: async (code) => {
        // Logic similar to filterPOSItems but focused on exact match first
        const items = await db.items.toArray();
        const item = items.find(i => i.barcode === code);

        if (item) {
            app.addToCart(item.id);
            const Toast = Swal.mixin({
                toast: true,
                position: 'top-end',
                showConfirmButton: false,
                timer: 3000,
                timerProgressBar: true
            });
            Toast.fire({
                icon: 'success',
                title: `${item.name} added to cart!`
            });
        } else {
            Swal.fire({
                icon: 'question',
                title: 'Item Not Found',
                text: `No item found with barcode: ${code}. Would you like to add it?`,
                showCancelButton: true,
                confirmButtonText: 'Add New Item'
            }).then((result) => {
                if (result.isConfirmed) {
                    app.openItemModal(null, code); // Pass code to modal
                }
            });
        }
    },

    generateBarcode: () => {
        const input = document.getElementById('swal-barcode');
        if (!input) return;

        // Generate a random EAN-13 like or Code128 format
        // Simple P + Timestamp + Random
        const code = 'ITM' + Date.now().toString().slice(-8) + Math.floor(Math.random() * 100);
        input.value = code;

        app.updateBarcodePreview(code);
    },

    updateBarcodePreview: (code) => {
        try {
            if (code) {
                JsBarcode("#barcode-svg", code, {
                    format: "CODE128",
                    lineColor: "#334155",
                    width: 2,
                    height: 40,
                    displayValue: true,
                    fontSize: 14,
                    textMargin: 0,
                    margin: 0
                });
                document.getElementById('barcode-preview-container').classList.remove('hidden');
            } else {
                document.getElementById('barcode-preview-container').classList.add('hidden');
            }
        } catch (e) {
            console.error(e);
        }
    },

    // --- INVENTORY ---
    renderInventory: async () => {
        const items = await db.items.toArray();
        const categories = ['All', ...new Set(items.map(item => item.category))];
        const settingsList = await db.categorySettings.toArray();
        const categoryMap = settingsList.reduce((acc, curr) => {
            acc[curr.name] = curr.image;
            return acc;
        }, {});

        // Initialize active category if not set
        if (app.state.inventoryCategory === undefined) app.state.inventoryCategory = 'All';
        const activeCategory = app.state.inventoryCategory;

        const html = `
            <div class="flex flex-col xl:flex-row gap-6 fade-in min-h-max xl:h-[calc(100vh-6rem)]">
                <!-- Categories Sidebar (Left) -->
                <div class="w-full xl:w-[320px] flex flex-col bg-white rounded-2xl shadow-xl border border-slate-200 h-[300px] xl:h-full flex-shrink-0">
                    <div class="p-4 border-b border-slate-100 flex justify-between items-center bg-slate-50 rounded-t-2xl">
                        <h3 class="font-bold text-slate-700"><i class="fa-solid fa-tags mr-2"></i> Categories</h3>
                    </div>
                    <div class="flex-1 overflow-y-auto p-4 grid grid-cols-2 sm:grid-cols-4 md:grid-cols-5 xl:grid-cols-2 gap-4 content-start pb-6">
                        <div class="relative group h-full">
                            <button onclick="app.setInventoryCategory('All')" class="w-full h-full flex flex-col items-center justify-center p-4 rounded-xl transition-all min-h-[120px] text-center ${activeCategory === 'All' ? 'bg-violet-100 text-violet-700 font-bold border-2 border-violet-500 shadow-md' : 'bg-white text-slate-600 hover:bg-slate-50 border border-slate-200 shadow-sm'}">
                                <i class="fa-solid fa-border-all text-4xl mb-3"></i>
                                <span class="text-sm font-extrabold leading-tight line-clamp-2 break-all">All Items</span>
                            </button>
                        </div>
                        ${categories.filter(c => c !== 'All').map(cat => {
            const details = app.getCategoryDetails(cat);
            const isActive = activeCategory === cat;
            const catImage = categoryMap[cat] || null;
            return `<div class="relative group h-full">
                                        <button onclick="app.setInventoryCategory('${cat}')" class="w-full h-full flex flex-col items-center justify-center p-4 rounded-xl transition-all min-h-[120px] text-center ${isActive ? 'bg-violet-100 text-violet-700 font-bold border-2 border-violet-500 shadow-md' : 'bg-white text-slate-600 hover:bg-slate-50 border border-slate-200 shadow-sm'}" title="${cat}">
                                            ${catImage ?
                    `<img src="${catImage}" class="w-16 h-16 object-cover rounded-xl mb-3 shadow-md border border-slate-100">` :
                    `<i class="fa-solid ${details.icon} text-4xl mb-3 ${isActive ? 'text-violet-600' : ''}"></i>`
                }
                                            <span class="text-sm font-extrabold leading-tight line-clamp-2 break-all">${cat}</span>
                                        </button>
                                    </div>`;
        }).join('')}
                    </div>
                </div>

                <!-- Main Area -->
                <div class="flex-1 flex flex-col min-h-[600px] xl:min-h-0 xl:h-full">
                    <!-- Top Bar: Title, Search & Add -->
                    <div class="mb-4 flex flex-col lg:flex-row gap-4 items-center justify-between bg-white p-4 rounded-2xl shadow-sm border border-slate-200">
                        <h2 class="text-xl font-bold text-slate-800 ml-2 hidden lg:block"><i class="fa-solid fa-box-open mr-2 text-violet-600"></i> Inventory</h2>
                        <div class="flex gap-3 w-full lg:w-auto flex-1 lg:max-w-xl">
                            <div class="relative flex-1">
                                <i class="fa-solid fa-magnifying-glass absolute left-4 top-1/2 transform -translate-y-1/2 text-slate-400 text-lg"></i>
                                <input type="text" id="inventory-search" placeholder="Search inventory..." 
                                    class="w-full pl-12 pr-4 py-3.5 rounded-xl border border-slate-200 focus:outline-none focus:ring-2 focus:ring-violet-500 shadow-sm text-lg"
                                    oninput="app.filterInventoryGrid(this.value)">
                            </div>
                            <button onclick="app.openItemModal()" class="bg-violet-600 hover:bg-violet-700 text-white px-6 rounded-xl font-bold shadow-lg shadow-violet-200 transition-all flex items-center whitespace-nowrap h-[54px]">
                                <i class="fa-solid fa-plus mr-2"></i> Add Item
                            </button>
                        </div>
                    </div>

                    <!-- Content Area -->
                    <div id="inventory-grid-container" class="flex-1 overflow-y-auto pb-20 p-2">
                        ${app.generateInventoryGridHTML(items, activeCategory)}
                    </div>
                </div>
            </div>
        `;
        document.getElementById('app-content').innerHTML = html;
        setTimeout(() => document.getElementById('inventory-search')?.focus(), 100);
    },

    setInventoryCategory: (category) => {
        app.state.inventoryCategory = category;
        app.renderInventory();
    },

    generateInventoryGridHTML: (items, category, query = '') => {
        let filtered = items;
        if (category && category !== 'All') {
            filtered = items.filter(i => i.category === category);
        }

        if (query) {
            const lowerQ = query.toLowerCase();
            filtered = filtered.filter(i => i.name.toLowerCase().includes(lowerQ) || (i.barcode && i.barcode.toLowerCase().includes(lowerQ)));
        }

        if (filtered.length === 0) {
            return `
                <div class="col-span-full flex flex-col items-center justify-center py-20 text-slate-400 bg-white rounded-3xl border border-slate-200 shadow-sm mt-4">
                    <i class="fa-solid fa-box-open text-6xl mb-6 opacity-30"></i>
                    <p class="text-xl font-bold">No inventory items found.</p>
                </div>
            `;
        }

        const gridBlocks = filtered.map(item => `
            <div class="bg-white p-5 rounded-2xl shadow-sm border border-slate-100 hover:shadow-xl hover:border-violet-300 transition-all group flex flex-col justify-between h-auto min-h-[220px]">
                <div>
                    <div class="flex justify-between items-start mb-4">
                        <div class="flex gap-2 items-center">
                            ${item.image ? `<img src="${item.image}" class="h-8 w-8 object-cover rounded shadow-sm border border-slate-100">` : ''}
                            <span class="px-3 py-1.5 rounded-md text-[10px] font-extrabold tracking-wider uppercase ${item.type === 'product' ? 'bg-blue-50 text-blue-600' : 'bg-orange-50 text-orange-600'}">
                                ${item.type}
                            </span>
                        </div>
                        ${item.type === 'product' ?
                `<span class="text-xs font-bold px-3 py-1.5 rounded-full shadow-sm ${item.stock <= (item.minStock || 5) ? 'bg-red-100 text-red-600' : 'bg-emerald-100 text-emerald-700'}">
                                ${item.stock} in stock
                             </span>`
                : ''}
                    </div>
                    <h4 class="font-bold text-slate-800 text-lg leading-tight mb-2 line-clamp-2" title="${item.name}">${item.name}</h4>
                    <p class="text-xs text-slate-400 mb-4 tracking-wide font-mono bg-slate-50 inline-block px-2 py-1 rounded border border-slate-100">${item.barcode || 'No barcode'}</p>
                </div>
                
                <div class="mt-auto">
                    <div class="flex justify-between items-end mb-4 pt-4 border-t border-slate-100">
                        <div>
                            <p class="text-[10px] text-slate-400 uppercase font-black tracking-wider mb-1">Selling Price</p>
                            <p class="text-violet-700 font-black text-xl">LKR ${item.price.toFixed(2)}</p>
                        </div>
                        ${item.cost ? `
                        <div class="text-right">
                            <p class="text-[10px] text-slate-400 uppercase font-black tracking-wider mb-1">Cost</p>
                            <p class="text-slate-500 font-bold text-sm">LKR ${item.cost.toFixed(2)}</p>
                        </div>` : ''}
                    </div>
                    
                    <div class="flex gap-2">
                        ${item.type === 'product' ? `
                        <button onclick="app.quickAddStock(${item.id})" class="flex-1 bg-emerald-50 hover:bg-emerald-100 text-emerald-600 py-2.5 rounded-xl text-sm font-bold transition-colors">
                            <i class="fa-solid fa-plus-minus mr-1"></i> Stock
                        </button>` : ''}
                        <button onclick="app.openItemModal(${item.id})" class="flex-1 bg-violet-50 hover:bg-violet-100 text-violet-600 py-2.5 rounded-xl text-sm font-bold transition-colors">
                            <i class="fa-solid fa-pen mr-1"></i> Edit
                        </button>
                        <button onclick="app.deleteItem(${item.id})" class="w-12 bg-red-50 hover:bg-red-100 text-red-600 py-2.5 rounded-xl transition-colors flex items-center justify-center shrink-0 shadow-sm border border-red-100">
                            <i class="fa-solid fa-trash"></i>
                        </button>
                    </div>
                </div>
            </div>
        `).join('');

        return `
            <div class="mb-4 flex items-center justify-between">
                <h2 class="text-xl font-bold text-slate-800 flex items-center">
                    <span class="text-slate-400 mr-2 font-normal">Showing:</span> ${category}
                </h2>
                <span class="bg-slate-200 text-slate-700 px-3 py-1 rounded-full text-sm font-bold shadow-inner">
                    ${filtered.length} items
                </span>
            </div>
            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 2xl:grid-cols-4 gap-6 content-start">
                ${gridBlocks}
            </div>
        `;
    },

    filterInventoryGrid: async (query) => {
        const items = await db.items.toArray();
        const html = app.generateInventoryGridHTML(items, app.state.inventoryCategory, query);
        const container = document.getElementById('inventory-grid-container');
        if (container) {
            container.innerHTML = html;
        }
    },

    quickAddStock: async (id) => {
        const item = await db.items.get(id);
        const { value: addAmount } = await Swal.fire({
            title: `Add Stock`,
            html: `<div class="mb-2"><strong>${item.name}</strong></div><div class="text-sm text-slate-500 mb-4">Current Stock: <span class="font-bold text-slate-800">${item.stock}</span></div>`,
            input: 'number',
            inputPlaceholder: 'Enter amount to add (e.g. 10)',
            showCancelButton: true,
            confirmButtonText: '<i class="fa-solid fa-plus mr-2"></i> Add Stock',
            confirmButtonColor: '#10b981',
            inputValidator: (value) => {
                const amount = parseInt(value);
                if (isNaN(amount) || amount <= 0) {
                    return 'Please enter a valid number greater than 0';
                }
            }
        });

        if (addAmount) {
            const amount = parseInt(addAmount);
            await db.items.update(id, { stock: item.stock + amount });
            app.apiCall(`/api/items/${id}/adjust-stock`, 'POST', { delta: amount }, 'adjust_stock', id);

            const Toast = Swal.mixin({
                toast: true,
                position: 'top-end',
                showConfirmButton: false,
                timer: 3000,
                timerProgressBar: true
            });
            Toast.fire({
                icon: 'success',
                title: `Added ${amount} items. New stock is ${item.stock + amount}.`
            });

            // Re-render inventory view
            if (app.state.currentView === 'products') {
                app.renderInventory();
            }
        }
    },

    openItemModal: async (id = null, prefillBarcode = '') => {
        // Default to last added category for new items, or 'General' if not set
        const defaultCategory = app.state.lastAddedCategory || 'General';
        let item = { name: '', barcode: prefillBarcode, category: defaultCategory, type: 'product', price: 0, cost: 0, stock: 0, minStock: 5 };

        if (id) {
            item = await db.items.get(id);
        }

        // Get existing categories from DB and merge with defaults
        const allItems = await db.items.toArray();
        const existingCategories = new Set(allItems.map(i => i.category));
        const defaultCategories = [
            'Accessories', 'Mobile Phones', 'Stationery', 'Service', 'Studio',
            'Chargers', 'Cable', 'Book', 'Photoframe', 'Chargers & Cable', 'Button Phone'
        ];
        defaultCategories.forEach(c => existingCategories.add(c));
        const sortedCategories = Array.from(existingCategories).sort();
        const categoryOptions = sortedCategories.map(c => `<option value="${c}">`).join('');

        const { value: formValues } = await Swal.fire({
            title: id ? 'Edit Item' : 'Add New Item',
            html: `
                <div class="space-y-4 text-left">
                    <div>
                        <label class="block text-xs font-bold text-slate-500 mb-1">Item Name</label>
                        <input id="swal-name" class="swal2-input m-0 w-full text-sm" placeholder="Item Name" value="${item.name}">
                    </div>
                    <div>
                        <label class="block text-xs font-bold text-slate-500 mb-1">Item Photo (Optional)</label>
                        <div class="flex items-center gap-3">
                            ${item.image ? `<img src="${item.image}" class="h-10 w-10 object-cover rounded shadow-sm border border-slate-200" alt="Item preview">` : ''}
                            <input id="swal-image" type="file" accept="image/*" class="w-full text-sm file:mr-4 file:py-2 file:px-4 file:rounded-lg file:border-0 file:text-sm file:font-semibold file:bg-violet-50 file:text-violet-700 hover:file:bg-violet-100">
                        </div>
                    </div>
                    
                    <div class="bg-slate-50 p-3 rounded-lg border border-slate-100">
                        <label class="block text-xs font-bold text-slate-500 mb-2">Barcode</label>
                        
                        <div id="scan-container" class="hidden mb-3 border border-slate-200 rounded-lg overflow-hidden bg-black">
                            <div id="modal-reader" class="w-full"></div>
                            <div class="p-1 bg-slate-800 text-white text-center text-[10px]">Scanning...</div>
                        </div>

                        <div class="flex gap-2 mb-2">
                            <input id="swal-barcode" class="swal2-input m-0 flex-1 text-sm h-10" placeholder="Scan or Enter" value="${item.barcode || ''}" oninput="app.updateBarcodePreview(this.value)">
                            <button type="button" onclick="app.toggleItemScanner()" class="bg-slate-800 hover:bg-slate-900 text-white w-10 h-10 rounded-lg flex items-center justify-center transition-colors shadow-sm" title="Scan with Camera">
                                <i class="fa-solid fa-camera"></i>
                            </button>
                            <button type="button" onclick="app.generateBarcode()" class="bg-violet-100 hover:bg-violet-200 text-violet-700 w-10 h-10 rounded-lg flex items-center justify-center transition-colors" title="Generate Random Barcode">
                                <i class="fa-solid fa-wand-magic-sparkles"></i>
                            </button>
                        </div>
                        <div id="barcode-preview-container" class="bg-white p-2 rounded border border-slate-200 flex justify-center ${item.barcode ? '' : 'hidden'}">
                            <svg id="barcode-svg" class="w-full"></svg>
                        </div>
                    </div>

                    <div class="grid grid-cols-2 gap-4">
                        <div>
                             <label class="block text-xs font-bold text-slate-500 mb-1">Category</label>
                             <input id="swal-category" class="swal2-input m-0 w-full text-sm" list="categories" value="${item.category}" placeholder="Select/Type">

                             <datalist id="categories">
                                ${categoryOptions}
                             </datalist>
                        </div>
                    </div>
                    <div class="grid grid-cols-2 gap-4">
                        <div>
                            <label class="block text-xs font-bold text-slate-500 mb-1">Type</label>
                            <select id="swal-type" class="swal2-input m-0 w-full" onchange="document.getElementById('stock-field').style.display = this.value === 'product' ? 'block' : 'none'">
                                <option value="product" ${item.type === 'product' ? 'selected' : ''}>Physical Product</option>
                                <option value="service" ${item.type === 'service' ? 'selected' : ''}>Service</option>
                            </select>
                        </div>
                        <div id="stock-field" style="${item.type === 'product' ? '' : 'display:none'}">
                             <label class="block text-xs font-bold text-slate-500 mb-1">Current Stock</label>
                             <input type="number" id="swal-stock" class="swal2-input m-0 w-full" value="${item.stock}">
                        </div>
                    </div>
                    <div class="grid grid-cols-2 gap-4">
                        <div>
                            <label class="block text-xs font-bold text-slate-500 mb-1">Selling Price (LKR)</label>
                            <input type="number" id="swal-price" class="swal2-input m-0 w-full" value="${item.price}">
                        </div>
                        <div>
                            <label class="block text-xs font-bold text-slate-500 mb-1">Cost Price (LKR)</label>
                            <input type="number" id="swal-cost" class="swal2-input m-0 w-full" value="${item.cost}">
                        </div>
                    </div>
                </div>
            `,
            customClass: {
                popup: 'rounded-2xl',
                confirmButton: 'bg-violet-600 px-6 py-2 rounded-lg',
                cancelButton: 'bg-slate-200 text-slate-600 px-6 py-2 rounded-lg'
            },
            didOpen: () => {
                // Initialize barcode preview if value exists
                const barcodeInput = document.getElementById('swal-barcode');
                if (barcodeInput && barcodeInput.value) {
                    app.updateBarcodePreview(barcodeInput.value);
                }
            },
            preConfirm: async () => {
                const name = document.getElementById('swal-name').value;
                const barcode = document.getElementById('swal-barcode').value;
                const category = document.getElementById('swal-category').value;
                const type = document.getElementById('swal-type').value;
                const stock = parseInt(document.getElementById('swal-stock').value) || 0;
                const price = parseFloat(document.getElementById('swal-price').value);
                const cost = parseFloat(document.getElementById('swal-cost').value) || 0;

                const fileInput = document.getElementById('swal-image');
                let imageBase64 = item.image || null;
                if (fileInput && fileInput.files.length > 0) {
                    const file = fileInput.files[0];
                    imageBase64 = await new Promise((resolve) => {
                        const reader = new FileReader();
                        reader.onload = (e) => resolve(e.target.result);
                        reader.readAsDataURL(file);
                    });
                }

                if (!name || isNaN(price) || !category) {
                    Swal.showValidationMessage('Please fill required fields (Name, Price, Category)');
                    return false;
                }
                return { name, barcode, category, type, price, cost, stock, minStock: 5, image: imageBase64 };
            }
        });

        if (formValues) {
            // Update last added category for next time
            app.state.lastAddedCategory = formValues.category;

            if (id) {
                await db.items.update(id, formValues);
                app.apiCall(`/api/items/${id}`, 'PUT', formValues, 'update_item', id);
            } else {
                const newId = await db.items.add(formValues);
                app.apiCall('/api/items', 'POST', { id: newId, ...formValues }, 'create_item');
            }
            app.renderInventory();
            Swal.fire({ icon: 'success', title: 'Saved', timer: 1000, showConfirmButton: false });
        }
    },

    deleteItem: async (id) => {
        const result = await Swal.fire({
            title: 'Are you sure?',
            text: "You won't be able to revert this!",
            icon: 'warning',
            showCancelButton: true,
            confirmButtonColor: '#d33',
            confirmButtonText: 'Yes, delete it!'
        });

        if (result.isConfirmed) {
            await db.items.delete(id);
            app.apiCall(`/api/items/${id}`, 'DELETE', null, 'delete_item', id);
            app.renderInventory();
            Swal.fire('Deleted!', 'Item has been deleted.', 'success');
        }
    },

    // --- REPAIRS ---
    renderRepairs: async () => {
        const repairs = await db.repairs.toArray();
        const html = `
            <div class="bg-white rounded-2xl shadow-sm border border-slate-100 p-6 fade-in h-full flex flex-col">
                <div class="flex justify-between items-center mb-6">
                    <h2 class="text-2xl font-bold text-slate-800">Repair Jobs</h2>
                    <button onclick="app.openRepairModal()" class="bg-violet-600 hover:bg-violet-700 text-white px-6 py-2 rounded-lg font-medium shadow-lg shadow-violet-200 transition-all">
                        <i class="fa-solid fa-plus mr-2"></i> New Job
                    </button>
                </div>

                <div class="flex-1 overflow-x-auto">
                    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                        ${repairs.length === 0 ? '<p class="text-slate-400 p-4">No active repair jobs.</p>' : ''}
                        ${repairs.map(job => `
                            <div class="border border-slate-200 rounded-xl p-4 hover:shadow-md transition bg-white relative">
                                <span class="absolute top-4 right-4 text-xs font-bold px-2 py-1 rounded ${app.getStatusColor(job.status)}">
                                    ${job.status}
                                </span>
                                <h3 class="font-bold text-slate-800 text-lg mb-1">${job.phoneModel}</h3>
                                <p class="text-sm text-slate-500 mb-3"><i class="fa-solid fa-user mr-1"></i> ${job.customerName}</p>
                                <div class="bg-slate-50 p-3 rounded-lg mb-4 text-sm text-slate-600">
                                    ${job.issue}
                                </div>
                                <div class="flex justify-between items-center">
                                    <span class="font-bold text-violet-700">LKR ${(job.cost || 0).toFixed(2)}</span>
                                    <div>
                                        <button onclick="app.updateRepairStatus(${job.id})" class="text-blue-500 hover:text-blue-700 mr-2 text-sm font-medium">Status</button>
                                        <button onclick="app.openRepairModal(${job.id})" class="text-violet-600 hover:text-violet-800 mr-2 text-sm font-medium"><i class="fa-solid fa-pen"></i> Edit</button>
                                        <button onclick="app.deleteRepair(${job.id})" class="text-slate-400 hover:text-red-500"><i class="fa-solid fa-trash"></i></button>
                                    </div>
                                </div>
                                <div class="text-xs text-slate-400 mt-3 text-right">
                                    ${new Date(job.createdAt).toLocaleDateString()}
                                </div>
                            </div>
                        `).join('')}
                    </div>
                </div>
            </div>
        `;
        document.getElementById('app-content').innerHTML = html;
    },

    getStatusColor: (status) => {
        switch (status) {
            case 'Pending': return 'bg-yellow-100 text-yellow-700';
            case 'In Progress': return 'bg-blue-100 text-blue-700';
            case 'Completed': return 'bg-emerald-100 text-emerald-700';
            case 'Delivered': return 'bg-slate-100 text-slate-600';
            default: return 'bg-gray-100 text-gray-700';
        }
    },

    openRepairModal: async (id = null) => {
        let job = { customerName: '', phoneModel: '', issue: '', cost: 0, status: 'Pending' };
        if (id) {
            job = await db.repairs.get(id);
        }

        const { value: formValues } = await Swal.fire({
            title: id ? 'Edit Repair Job' : 'New Repair Job',
            html: `
                <div class="space-y-3 text-left">
                    <div>
                        <label class="block text-xs font-bold text-slate-500 mb-1">Customer Name</label>
                        <input id="rep-name" class="swal2-input m-0 w-full" placeholder="Customer Name" value="${job.customerName || ''}">
                    </div>
                    <div>
                        <label class="block text-xs font-bold text-slate-500 mb-1">Device Model</label>
                        <input id="rep-model" class="swal2-input m-0 w-full" placeholder="Device Model" value="${job.phoneModel || ''}">
                    </div>
                    <div>
                        <label class="block text-xs font-bold text-slate-500 mb-1">Issue Description</label>
                        <textarea id="rep-issue" class="swal2-textarea m-0 w-full" placeholder="Issue Description">${job.issue || ''}</textarea>
                    </div>
                    <div>
                        <label class="block text-xs font-bold text-slate-500 mb-1">Estimated/Total Cost (LKR)</label>
                        <input id="rep-cost" type="number" class="swal2-input m-0 w-full" placeholder="Cost" value="${job.cost || 0}">
                    </div>
                    ${id ? `
                    <div>
                        <label class="block text-xs font-bold text-slate-500 mb-1">Status</label>
                        <select id="rep-status" class="swal2-input m-0 w-full">
                            <option value="Pending" ${job.status === 'Pending' ? 'selected' : ''}>Pending</option>
                            <option value="In Progress" ${job.status === 'In Progress' ? 'selected' : ''}>In Progress</option>
                            <option value="Completed" ${job.status === 'Completed' ? 'selected' : ''}>Completed</option>
                            <option value="Delivered" ${job.status === 'Delivered' ? 'selected' : ''}>Delivered</option>
                        </select>
                    </div>
                    ` : ''}
                </div>
            `,
            showCancelButton: true,
            confirmButtonText: id ? 'Update Job' : 'Create Job',
            preConfirm: () => {
                const customerName = document.getElementById('rep-name').value;
                const phoneModel = document.getElementById('rep-model').value;
                const issue = document.getElementById('rep-issue').value;
                const cost = parseFloat(document.getElementById('rep-cost').value) || 0;
                
                if (!customerName || !phoneModel || !issue) {
                    Swal.showValidationMessage('Please fill in Customer Name, Model and Issue');
                    return false;
                }

                const data = {
                    customerName,
                    phoneModel,
                    issue,
                    cost
                };

                if (id) {
                    data.status = document.getElementById('rep-status').value;
                } else {
                    data.status = 'Pending';
                    data.createdAt = new Date().toISOString();
                }

                return data;
            }
        });

        if (formValues) {
            if (id) {
                await db.repairs.update(id, formValues);
                app.apiCall(`/api/repairs/${id}`, 'PUT', formValues, 'update_repair', id);
                Swal.fire({ icon: 'success', title: 'Job Updated', timer: 1000, showConfirmButton: false });
            } else {
                const newId = await db.repairs.add(formValues);
                app.apiCall('/api/repairs', 'POST', { id: newId, ...formValues }, 'create_repair');
                Swal.fire({ icon: 'success', title: 'Job Created', timer: 1000, showConfirmButton: false });
            }
            app.renderRepairs();
        }
    },

    updateRepairStatus: async (id) => {
        const repair = await db.repairs.get(id);
        const { value: status } = await Swal.fire({
            title: 'Update Status',
            input: 'select',
            inputOptions: {
                'Pending': 'Pending',
                'In Progress': 'In Progress',
                'Completed': 'Completed',
                'Delivered': 'Delivered'
            },
            inputValue: repair.status,
            showCancelButton: true
        });

        if (status) {
            await db.repairs.update(id, { status });
            const updated = await db.repairs.get(id);
            app.apiCall(`/api/repairs/${id}`, 'PUT', updated, 'update_repair', id);
            app.renderRepairs();
        }
    },

    deleteRepair: async (id) => {
        if (await Swal.fire({ title: 'Delete job?', icon: 'warning', showCancelButton: true }).then(r => r.isConfirmed)) {
            await db.repairs.delete(id);
            app.apiCall(`/api/repairs/${id}`, 'DELETE', null, 'delete_repair', id);
            app.renderRepairs();
        }
    },

    // --- REPORTS ---
    renderReports: async (displayDate = new Date().toISOString().split('T')[0]) => {
        // Fetch data
        const sales = await db.sales.where('date').startsWith(displayDate).toArray();
        const expenses = await db.expenses.where('date').startsWith(displayDate).toArray();

        // Calculate Metrics
        let metrics = {
            revenue: 0,
            cogs: 0, // Cost of Goods Sold
            grossProfit: 0,
            expenseTotal: 0,
            netProfit: 0
        };

        sales.forEach(sale => {
            metrics.revenue += sale.total;
            let saleCost = 0;
            if (sale.items && Array.isArray(sale.items)) {
                saleCost = sale.items.reduce((acc, item) => acc + ((item.cost || 0) * item.qty), 0);
            }
            metrics.cogs += saleCost;
        });

        expenses.forEach(exp => metrics.expenseTotal += exp.amount);

        metrics.grossProfit = metrics.revenue - metrics.cogs;
        metrics.netProfit = metrics.grossProfit - metrics.expenseTotal;

        const html = `
             <div class="bg-white rounded-2xl shadow-sm border border-slate-100 p-6 fade-in space-y-8 h-full flex flex-col">
                 <div class="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
                     <h2 class="text-2xl font-bold text-slate-800">Financial Reports</h2>
                     <div class="flex items-center gap-2">
                        <label class="text-sm font-medium text-slate-500">Date:</label>
                        <input type="date" value="${displayDate}" onchange="app.renderReports(this.value)" class="border border-slate-200 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-violet-500">
                     </div>
                 </div>
                 
                 <div class="grid grid-cols-1 md:grid-cols-4 gap-4">
                     <div class="p-4 bg-emerald-50 rounded-xl border border-emerald-100 hover:shadow-md transition-shadow">
                         <p class="text-xs text-emerald-600 font-bold uppercase tracking-wider mb-1">Total Revenue</p>
                         <h3 class="text-2xl font-bold text-emerald-800">LKR ${metrics.revenue.toFixed(2)}</h3>
                         <p class="text-[10px] text-emerald-600 mt-1">${sales.length} Sales</p>
                     </div>
                     <div class="p-4 bg-blue-50 rounded-xl border border-blue-100 hover:shadow-md transition-shadow">
                         <p class="text-xs text-blue-600 font-bold uppercase tracking-wider mb-1">Gross Profit</p>
                         <h3 class="text-2xl font-bold text-blue-800">LKR ${metrics.grossProfit.toFixed(2)}</h3>
                         <p class="text-[10px] text-blue-600 mt-1">Revenue - Cost</p>
                     </div>
                      <div class="p-4 bg-red-50 rounded-xl border border-red-100 hover:shadow-md transition-shadow">
                         <p class="text-xs text-red-600 font-bold uppercase tracking-wider mb-1">Total Expenses</p>
                         <h3 class="text-2xl font-bold text-red-800">LKR ${metrics.expenseTotal.toFixed(2)}</h3>
                         <p class="text-[10px] text-red-600 mt-1">${expenses.length} Records</p>
                     </div>
                      <div class="p-4 bg-indigo-50 rounded-xl border border-indigo-100 hover:shadow-md transition-shadow">
                         <p class="text-xs text-indigo-600 font-bold uppercase tracking-wider mb-1">Net Profit</p>
                         <h3 class="text-2xl font-bold text-indigo-800">LKR ${metrics.netProfit.toFixed(2)}</h3>
                         <p class="text-[10px] text-indigo-600 mt-1">Gross - Expenses</p>
                     </div>
                 </div>

                <div class="flex-1 overflow-y-auto grid grid-cols-1 lg:grid-cols-2 gap-6 pt-4 border-t border-slate-100">
                    <div>
                        <h3 class="text-lg font-bold text-slate-700 mb-4 flex items-center gap-2"><i class="fa-solid fa-receipt text-slate-400"></i> Expenses Log</h3>
                        <div class="overflow-x-auto rounded-lg border border-slate-200">
                             <table class="w-full text-left text-sm">
                                <thead class="bg-slate-50 text-xs uppercase font-bold text-slate-500">
                                    <tr><th class="px-4 py-3">Cat</th><th class="px-4 py-3">Desc</th><th class="px-4 py-3 text-right">Amt</th></tr>
                                </thead>
                                <tbody class="divide-y divide-slate-100 bg-white">
                                    ${expenses.length === 0 ? '<tr><td colspan="3" class="px-4 py-4 text-center text-slate-400 text-xs">No expenses for this date</td></tr>' : ''}
                                    ${expenses.map(e => `
                                        <tr>
                                            <td class="px-4 py-2"><span class="bg-slate-100 px-2 py-0.5 rounded text-[10px] uppercase font-bold text-slate-500">${e.category}</span></td>
                                            <td class="px-4 py-2 text-slate-700">${e.description}</td>
                                            <td class="px-4 py-2 text-right font-bold text-red-600">${e.amount.toFixed(2)}</td>
                                        </tr>
                                    `).join('')}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>
        `;
        document.getElementById('app-content').innerHTML = html;
    },

    exportData: async () => {
        window.location.href = '/api/backup/export';
    },

    // --- CREDIT BOOK (NAYA POTHA) ---
    renderCredits: async () => {
        const creditors = await db.creditors.toArray();
        const html = `
            <div class="bg-white rounded-2xl shadow-sm border border-slate-100 p-6 fade-in h-full flex flex-col">
                <div class="flex justify-between items-center mb-6">
                    <div>
                        <h2 class="text-2xl font-bold text-slate-800">ණය පොත (Credit Book)</h2>
                        <p class="text-sm text-slate-500">ණයකරුවන් සහ සැපයුම්කරුවන් කළමනාකරණය</p>
                    </div>
                    <button onclick="app.openCreditorModal()" class="bg-violet-600 hover:bg-violet-700 text-white px-6 py-2 rounded-lg font-medium shadow-lg shadow-violet-200 transition-all flex items-center">
                        <i class="fa-solid fa-plus mr-2"></i> අලුත් අයෙක් එක් කරන්න (Add Person)
                    </button>
                </div>

                <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 overflow-y-auto p-1">
                    ${creditors.length === 0 ?
                `<div class="col-span-full flex flex-col items-center justify-center p-12 text-slate-400 bg-slate-50 rounded-xl border border-dashed border-slate-200">
                            <i class="fa-solid fa-book text-4xl mb-3 opacity-50"></i>
                            <p>පැහැදිලි වාර්තා නොමැත (No records found).</p>
                        </div>` : ''}
                    
                    ${creditors.map(c => `
                        <div class="bg-white border border-slate-200 rounded-xl p-5 hover:shadow-md transition-shadow relative overflow-hidden group">
                            <div class="absolute top-0 right-0 w-24 h-24 bg-gradient-to-br ${c.amount < 0 ? 'from-green-50 to-emerald-100' : 'from-red-50 to-rose-100'} rounded-bl-full -mr-8 -mt-8 opacity-50"></div>
                            
                            <div class="relative z-10">
                                <h3 class="text-xl font-bold text-slate-800 mb-0.5">${c.name}</h3>
                                <p class="text-[10px] text-violet-600 font-black uppercase tracking-widest mb-4">${c.contact || 'No Contact'}</p>
                                
                                <div class="bg-slate-50 rounded-lg p-4 mb-4 text-center border border-slate-100 shadow-inner">
                                    <p class="text-[10px] text-slate-400 font-black uppercase mb-1 tracking-tighter">වත්මන් ශේෂය (Current Balance)</p>
                                    <p class="text-2xl font-black ${c.amount < 0 ? 'text-emerald-600' : 'text-red-600'}">
                                        LKR ${Math.abs(c.amount).toFixed(2)}
                                        <span class="text-[10px] font-bold block mt-1 uppercase tracking-widest opacity-80">
                                            ${c.amount < 0 ? 'සැපයුම්කරුට ගෙවිය යුතුයි (To Pay)' : 'අපට ලැබිය යුතුයි (To Collect)'}
                                        </span>
                                    </p>
                                </div>

                                <div class="grid grid-cols-2 gap-3">
                                    <button onclick="app.updateCreditorAmount(${c.id}, -1)" class="py-2.5 px-3 bg-emerald-600 text-white hover:bg-emerald-700 rounded-xl text-xs font-black transition-all shadow-md flex flex-col items-center justify-center gap-1">
                                        <i class="fa-solid fa-hand-holding-dollar text-lg"></i>
                                        <span>මුදල් ලැබුණා (Paid)</span>
                                    </button>
                                    <button onclick="app.updateCreditorAmount(${c.id}, 1)" class="py-2.5 px-3 bg-red-600 text-white hover:bg-red-700 rounded-xl text-xs font-black transition-all shadow-md flex flex-col items-center justify-center gap-1">
                                        <i class="fa-solid fa-file-invoice text-lg"></i>
                                        <span>ණයට ගත්තා (Credit)</span>
                                    </button>
                                </div>
                                <div class="absolute top-4 right-4 opacity-0 group-hover:opacity-100 transition-opacity">
                                    <button onclick="app.deleteCreditor(${c.id})" class="text-slate-300 hover:text-red-500 transition-colors"><i class="fa-solid fa-trash"></i></button>
                                </div>
                            </div>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
        document.getElementById('app-content').innerHTML = html;
    },

    openCreditorModal: async (defaultType = 'receivable') => {
        const { value: formValues } = await Swal.fire({
            title: defaultType === 'receivable' ? 'නව ගනුදෙනුකරුවෙකු ලියාපදිංචි කිරීම' : 'නව ණය වාර්තාවක්',
            html: `
                <div class="space-y-4 text-left">
                    <div>
                        <label class="block text-xs font-bold text-slate-500 mb-1">නම (Name)</label>
                        <input id="cred-name" class="swal2-input m-0 w-full" placeholder="e.g. Kamal Perera">
                    </div>
                    <div>
                        <label class="block text-xs font-bold text-slate-500 mb-1">දුරකථන අංකය (Contact Number)</label>
                        <input id="cred-contact" class="swal2-input m-0 w-full" placeholder="07x xxxxxxx">
                    </div>
                    <div>
                        <label class="block text-xs font-bold text-slate-500 mb-1">ආරම්භක ශේෂය (Initial Amount - LKR)</label>
                        <input id="cred-amount" type="number" class="swal2-input m-0 w-full" placeholder="0.00" value="0">
                    </div>
                    <div>
                        <label class="block text-xs font-bold text-slate-500 mb-1">වර්ගය (Record Type)</label>
                        <select id="cred-type" class="swal2-input m-0 w-full">
                            <option value="receivable" ${defaultType === 'receivable' ? 'selected' : ''}>අපට ලැබිය යුතු (Customer / Debtor)</option>
                            <option value="payable" ${defaultType === 'payable' ? 'selected' : ''}>අප ගෙවිය යුතු (Supplier / Creditor)</option>
                        </select>
                    </div>
                </div>
            `,
            focusConfirm: false,
            showCancelButton: true,
            confirmButtonText: 'වාර්තාව සුරකින්න (Save Record)',
            confirmButtonColor: '#7c3aed',
            preConfirm: () => {
                const name = document.getElementById('cred-name').value;
                const contact = document.getElementById('cred-contact').value;
                let amount = parseFloat(document.getElementById('cred-amount').value) || 0;
                const type = document.getElementById('cred-type').value;

                if (type === 'payable') amount = -Math.abs(amount);
                else amount = Math.abs(amount);

                if (!name) {
                    Swal.showValidationMessage('නම ඇතුළත් කිරීම අනිවාර්යයි');
                    return false;
                }
                return { name, contact, amount, type, lastUpdated: new Date().toISOString() };
            }
        });

        if (formValues) {
            const newId = await db.creditors.add(formValues);
            app.apiCall('/api/creditors', 'POST', { id: newId, ...formValues }, 'create_creditor');
            
            // Re-render current view
            const activeNav = document.querySelector('nav a.bg-violet-600')?.innerText?.toLowerCase() || '';
            if (activeNav.includes('pos')) {
                app.renderPOS();
            } else {
                app.renderCredits();
            }
            
            Swal.fire({ icon: 'success', title: 'Record Added', timer: 1000, showConfirmButton: false });
        }
    },

    updateCreditorAmount: async (id, multiplier) => {
        const creditor = await db.creditors.get(id);
        const isDebtor = creditor.amount >= 0; // True if they owe us (Customer)
        
        let title = '';
        if (multiplier > 0) {
            title = isDebtor ? 'ණයට ලබාදීම (Add New Debt)' : 'ණය ගැනීම වැඩි කිරීම (Increase Payable)';
        } else {
            title = isDebtor ? 'මුදල් ලැබීම / පියවීම (Record Payment)' : 'ණය පියවීම (Settle Payable)';
        }

        const { value: amount } = await Swal.fire({
            title: title,
            html: `
                <div class="text-left mb-2">
                    <label class="text-[10px] font-black text-slate-400 uppercase tracking-widest pl-1">මුදල (Amount in LKR)</label>
                    <input id="swal-amount" type="number" class="swal2-input !mt-1 !w-full" placeholder="0.00">
                </div>
            `,
            showCancelButton: true,
            confirmButtonText: 'Record Update',
            confirmButtonColor: multiplier > 0 ? '#dc2626' : '#059669',
            preConfirm: () => {
                const val = parseFloat(document.getElementById('swal-amount').value);
                if (!val || val <= 0) {
                    Swal.showValidationMessage('Please enter a valid amount');
                    return false;
                }
                return val;
            }
        });

        if (amount) {
            const numericAmount = parseFloat(amount);
            let newAmount = creditor.amount;

            if (multiplier > 0) { // Increase debt/payable
                if (isDebtor) newAmount += numericAmount; // They owe us more
                else newAmount -= numericAmount; // we owe supplier more (more negative)
            } else { // Settle/Pay
                if (isDebtor) newAmount -= numericAmount; // They paid us (debt decreases)
                else newAmount += numericAmount; // we paid supplier (debt decreases)
            }

            const updatedObj = { amount: newAmount, lastUpdated: new Date().toISOString() };
            await db.creditors.update(id, updatedObj);
            const fullUpdated = await db.creditors.get(id);
            app.apiCall(`/api/creditors/${id}`, 'PUT', fullUpdated, 'update_creditor', id);

            app.renderCredits();
            Swal.fire({ icon: 'success', title: 'ශේෂය යාවත්කාලීන කරන ලදී (Balance Updated)', timer: 1500, showConfirmButton: false, toast: true, position: 'top-end' });
        }
    },

    deleteCreditor: async (id) => {
        if ((await Swal.fire({ title: 'Are you sure?', icon: 'warning', showCancelButton: true })).isConfirmed) {
            await db.creditors.delete(id);
            app.apiCall(`/api/creditors/${id}`, 'DELETE', null, 'delete_creditor', id);
            app.renderCredits();
        }
    },

    openExpenseModal: async () => {
        const { value: formValues } = await Swal.fire({
            title: 'Log Expense',
            html: `
                <div class="space-y-3 text-left">
                    <input id="exp-desc" class="swal2-input m-0 w-full" placeholder="Description (e.g. Electricity Bill)">
                    <input id="exp-cat" class="swal2-input m-0 w-full" list="exp-cats" placeholder="Category">
                    <datalist id="exp-cats">
                        <option value="Utilities">
                        <option value="Rent">
                        <option value="Supplies">
                        <option value="Salary">
                    </datalist>
                    <input id="exp-amount" type="number" class="swal2-input m-0 w-full" placeholder="Amount">
                </div>
            `,
            showCancelButton: true,
            preConfirm: () => {
                return {
                    description: document.getElementById('exp-desc').value,
                    category: document.getElementById('exp-cat').value,
                    amount: parseFloat(document.getElementById('exp-amount').value) || 0,
                    date: new Date().toISOString()
                }
            }
        });

        if (formValues) {
            const newId = await db.expenses.add(formValues);
            app.apiCall('/api/expenses', 'POST', { id: newId, ...formValues }, 'create_expense');
            Swal.fire({ icon: 'success', title: 'Expense Added', timer: 1000, showConfirmButton: false });
        }
    },

    deleteExpense: async (id) => {
        if ((await Swal.fire({ title: 'Delete expense?', icon: 'warning', showCancelButton: true })).isConfirmed) {
            await db.expenses.delete(id);
            app.apiCall(`/api/expenses/${id}`, 'DELETE', null, 'delete_expense', id);
            app.renderExpenses();
        }
    },

    // --- SALES HISTORY ---
    renderSalesHistory: async () => {
        const sales = await db.sales.orderBy('date').reverse().toArray();
        const html = `
             <div class="bg-white rounded-2xl shadow-sm border border-slate-100 p-6 fade-in">
                 <h2 class="text-2xl font-bold text-slate-800 mb-6">Sales History</h2>
                 <div class="overflow-x-auto">
                    <table class="w-full text-left text-sm">
                        <thead class="bg-slate-50 text-xs uppercase font-bold text-slate-500">
                            <tr>
                                <th class="px-6 py-4">Date</th>
                                <th class="px-6 py-4">Receipt ID</th>
                                <th class="px-6 py-4">Items</th>
                                <th class="px-6 py-4">Payment</th>
                                <th class="px-6 py-4 text-right">Total</th>
                                <th class="px-6 py-4 text-center">Action</th>
                            </tr>
                        </thead>
                         <tbody class="divide-y divide-slate-100">
                             ${sales.map(s => `
                                <tr class="hover:bg-slate-50">
                                    <td class="px-6 py-4">${new Date(s.date).toLocaleString()}</td>
                                    <td class="px-6 py-4 text-slate-400">#${s.id}</td>
                                    <td class="px-6 py-4 text-xs text-slate-600">${s.items.map(i => `${i.qty}x ${i.name}`).join(', ')}</td>
                                    <td class="px-6 py-4 badge"><span class="bg-slate-100 px-2 py-1 rounded text-xs">${s.paymentMethod}</span></td>
                                    <td class="px-6 py-4 text-right font-bold text-emerald-600">LKR ${s.total.toFixed(2)}</td>
                                    <td class="px-6 py-4 text-center">
                                        <button onclick="app.printReceipt(${s.id})" class="text-violet-600 hover:text-violet-800 transition-colors p-2" title="Print Receipt">
                                            <i class="fa-solid fa-print"></i>
                                        </button>
                                    </td>
                                </tr>
                             `).join('')}
                         </tbody>
                    </table>
                 </div>
             </div>
        `;
    document.getElementById('app-content').innerHTML = html;
},

    // --- REPORTING ACTIONS ---
    renderExpenses: async () => {
        // Just reusing reports for now or a specific expense view
        // Let's redirect to reports as they contain expenses
        app.renderReports();
    },

        // --- UTILITY BILLS ---
    renderUtilityBills: async () => {
        const html = `
            <div class="max-w-4xl mx-auto fade-in">
                <div class="bg-white rounded-3xl shadow-xl border border-slate-200 overflow-hidden">
                    <div class="bg-gradient-to-r from-emerald-600 to-teal-600 p-8 text-white">
                        <h2 class="text-3xl font-black mb-2 flex items-center gap-3">
                            <i class="fa-solid fa-bolt-lightning"></i> Utility Bill Payment
                        </h2>
                        <p class="text-emerald-50 text-sm opacity-90">Pay Electricity (CEB), Water (NWSDB) or Telecom bills instantly.</p>
                    </div>

                    <div class="p-10">
                        <div class="mb-10">
                            <label class="block text-sm font-black text-slate-700 mb-6 uppercase tracking-widest text-center">Tap to Add a Bill</label>
                            <div class="grid grid-cols-2 md:grid-cols-5 gap-4">
                                <button onclick="app.addUtilityRow('CEB')" class="p-8 border-2 border-slate-100 rounded-[2rem] flex flex-col items-center gap-4 hover:border-yellow-400 hover:bg-yellow-50 hover:shadow-lg transition-all font-black text-slate-700 group overflow-hidden">
                                    <img src="http://slcgdxb.com/wp-content/uploads/2021/07/CEB-Logo.jpg" class="w-24 h-24 object-contain rounded-lg group-hover:scale-125 transition-transform transform scale-110">
                                    <span class="text-xl">CEB</span>
                                </button>
                                <button onclick="app.addUtilityRow('Water')" class="p-8 border-2 border-slate-100 rounded-[2rem] flex flex-col items-center gap-4 hover:border-blue-400 hover:bg-blue-50 hover:shadow-lg transition-all font-black text-slate-700 group overflow-hidden">
                                    <img src="https://www.waterboard.lk/wp-content/uploads/2022/11/Water-Board-Logo.png" class="w-24 h-24 object-contain rounded-lg group-hover:scale-125 transition-transform transform scale-110">
                                    <span class="text-xl">Water</span>
                                </button>
                                <button onclick="app.addUtilityRow('Walawa')" class="p-8 border-2 border-slate-100 rounded-[2rem] flex flex-col items-center gap-4 hover:border-orange-400 hover:bg-orange-50 hover:shadow-lg transition-all font-black text-slate-700 group">
                                    <i class="fa-solid fa-seedling text-5xl text-orange-500 group-hover:scale-110 transition-transform"></i>
                                    <span class="text-lg text-center leading-tight">Walawa<br>CBO</span>
                                </button>
                                <button onclick="app.addUtilityRow('Other')" class="p-8 border-2 border-slate-100 rounded-[2rem] flex flex-col items-center gap-4 hover:border-slate-400 hover:bg-slate-50 hover:shadow-lg transition-all font-black text-slate-700 group">
                                    <i class="fa-solid fa-plus text-5xl text-slate-400 group-hover:scale-110 transition-transform"></i>
                                    <span class="text-lg">Other</span>
                                </button>
                            </div>
                        </div>

                        <div id="utility-rows-container" class="space-y-4">
                            <!-- Rows will be added here -->
                        </div>

                        <div id="utility-link-container" class="mt-8 hidden">
                             <label class="block text-sm font-bold text-slate-700 mb-2 uppercase tracking-wide">Official Payment Portal</label>
                             <a id="utility-official-link" href="#" target="_blank" class="flex items-center justify-between p-4 bg-blue-50 text-blue-700 rounded-2xl border border-blue-100 hover:bg-blue-100 transition-all group max-w-md">
                                <div class="flex items-center gap-3">
                                    <i class="fa-solid fa-earth-americas text-xl"></i>
                                    <span class="font-bold text-sm">Pay on Official Site</span>
                                </div>
                                <i class="fa-solid fa-arrow-up-right-from-square opacity-50 group-hover:opacity-100 transition-opacity"></i>
                             </a>
                        </div>

                        <div class="mt-10 flex justify-end gap-4 border-t border-slate-100 pt-8">
                             <button onclick="app.navigate('dashboard')" class="px-8 py-4 bg-slate-100 text-slate-600 font-bold border border-slate-200 rounded-2xl hover:bg-slate-200 transition-all">Cancel</button>
                             <button onclick="app.processUtilityPayment()" class="px-10 py-4 bg-emerald-600 text-white font-black rounded-2xl shadow-xl shadow-emerald-200 hover:bg-emerald-700 transform hover:scale-105 active:scale-95 transition-all flex items-center gap-3 text-lg">
                                <i class="fa-solid fa-print"></i> Process & Print Receipt
                             </button>
                        </div>
                    </div>
                </div>
            </div>
        `;
        document.getElementById('app-content').innerHTML = html;
        // Start with one CEB row
        app.addUtilityRow('CEB');
    },

    addUtilityRow: (type = 'CEB') => {
        const container = document.getElementById('utility-rows-container');
        const row = document.createElement('div');
        
        const typeIcons = {
            'CEB': '<img src="http://slcgdxb.com/wp-content/uploads/2021/07/CEB-Logo.jpg" class="w-14 h-14 object-contain rounded-md transform scale-125">',
            'Water': '<img src="https://www.waterboard.lk/wp-content/uploads/2022/11/Water-Board-Logo.png" class="w-14 h-14 object-contain rounded-md transform scale-125">',
            'Walawa': '<i class="fa-solid fa-seedling text-orange-500 text-3xl"></i>',
            'Other': '<i class="fa-solid fa-plus text-slate-400 text-3xl"></i>'
        };

        row.className = 'utility-row grid grid-cols-1 md:grid-cols-12 gap-3 bg-white p-5 rounded-2xl border border-slate-200 relative shadow-sm fade-in mb-4';
        row.innerHTML = `
            <div class="md:col-span-1 flex items-center justify-center">
                ${typeIcons[type]}
                <input type="hidden" class="util-type" value="${type}">
            </div>
            <div class="md:col-span-3">
                <label class="block text-[10px] font-bold text-slate-400 mb-1 uppercase tracking-widest">Account No.</label>
                <input type="text" class="util-acc w-full px-4 py-2.5 rounded-xl border border-slate-200 focus:ring-2 focus:ring-emerald-500 font-bold" placeholder="Acc. No">
            </div>
            <div class="md:col-span-3">
                <label class="block text-[10px] font-bold text-slate-400 mb-1 uppercase tracking-widest">Reference No.</label>
                <input type="text" class="util-ref w-full px-4 py-2.5 rounded-xl border border-slate-200 focus:ring-2 focus:ring-emerald-500" placeholder="Ref. No">
            </div>
            ${type === 'Other' ? `
            <div class="md:col-span-2">
                <label class="block text-[10px] font-bold text-slate-400 mb-1 uppercase tracking-widest">Bill Name</label>
                <input type="text" class="util-other-name w-full px-4 py-2.5 rounded-xl border border-slate-200 focus:ring-2 focus:ring-emerald-500" placeholder="Bill Name">
            </div>
            ` : ''}
            <div class="md:col-span-2">
                <label class="block text-[10px] font-bold text-slate-400 mb-1 uppercase tracking-widest">Amount</label>
                <input type="number" class="util-amount w-full px-4 py-2.5 rounded-xl border border-slate-200 focus:ring-2 focus:ring-emerald-500 font-bold text-emerald-600" placeholder="0.00" oninput="app.updateRowServiceCharge(this)">
                <span class="util-charge-label text-[10px] font-extrabold text-slate-400 block mt-1 tracking-tight">Charge: LKR 0.00</span>
            </div>
            <div class="md:col-span-1 flex items-end justify-center pb-1">
                <button onclick="this.closest('.utility-row').remove(); app.checkUtilityLinks();" class="p-2.5 text-red-300 hover:text-red-500 transition-colors">
                    <i class="fa-solid fa-xmark text-lg"></i>
                </button>
            </div>
        `;
        container.appendChild(row);
        app.checkUtilityLinks();
    },

    checkUtilityLinks: () => {
        const types = Array.from(document.querySelectorAll('.util-type')).map(i => i.value);
        const linkContainer = document.getElementById('utility-link-container');
        const linkElem = document.getElementById('utility-official-link');
        const linkText = linkElem.querySelector('span');

        if (types.includes('CEB')) {
            linkContainer.classList.remove('hidden');
            linkElem.href = 'https://payment.ceb.lk//instantpay';
            linkText.innerText = 'Pay on CEB Official Site';
        } else if (types.includes('Water')) {
            linkContainer.classList.remove('hidden');
            linkElem.href = 'https://www.waterboard.lk/web/index.php?option=com_content&view=article&id=115&Itemid=158&lang=en';
            linkText.innerText = 'Pay on Water Board Portal';
        } else {
            linkContainer.classList.add('hidden');
        }
    },

    updateRowServiceCharge: (input) => {
        const row = input.closest('.utility-row');
        const amount = parseFloat(input.value);
        const label = row.querySelector('.util-charge-label');
        if (label) {
            const charge = app.calculateUtilityServiceCharge(amount);
            label.textContent = `Charge: LKR ${charge.toFixed(2)}`;
        }
    },

    calculateUtilityServiceCharge: (amount) => {
        if (isNaN(amount) || amount <= 0) return 0;
        if (amount <= 5000) return 30;
        if (amount <= 15000) return 40;
        return 50;
    },

    processUtilityPayment: async () => {
        const rows = document.querySelectorAll('.utility-row');
        let saleItems = [];
        let totalBillAmount = 0;
        let totalServiceCharge = 0;

        for (const row of rows) {
            const type = row.querySelector('.util-type').value;
            const accNo = row.querySelector('.util-acc').value;
            const refNo = row.querySelector('.util-ref').value;
            const billAmount = parseFloat(row.querySelector('.util-amount').value);
            const otherName = row.querySelector('.util-other-name')?.value || '';
            
            if (!accNo || isNaN(billAmount) || billAmount <= 0) {
                Swal.fire({ icon: 'error', title: 'Invalid Entry', text: 'Please enter Account Number and Amount.' });
                return;
            }

            const serviceChargePerBill = app.calculateUtilityServiceCharge(billAmount);

            saleItems.push({
                name: type === 'Other' ? (otherName || 'Utility Bill') : `${type} Bill Payment`,
                qty: 1,
                price: billAmount + serviceChargePerBill,
                cost: billAmount,
                type: 'service',
                utilityType: type,
                otherName: otherName,
                accNo: accNo,
                ref: refNo, // Storing reference as well
                billAmount: billAmount,
                serviceCharge: serviceChargePerBill
            });

            totalBillAmount += billAmount;
            totalServiceCharge += serviceChargePerBill;
        }

        const totalToPay = totalBillAmount + totalServiceCharge;

        const confirm = await Swal.fire({
            title: `Confirm ${rows.length} Payment(s)?`,
            html: `<div class="text-left space-y-2 p-2 bg-slate-50 rounded-xl border border-slate-100">
                <p><strong>Total Bills:</strong> ${rows.length}</p>
                <div class="border-t border-slate-200 mt-2 pt-2 space-y-1">
                    <p class="flex justify-between text-sm"><span>Total Bill Amount:</span> <span>LKR ${totalBillAmount.toFixed(2)}</span></p>
                    <p class="flex justify-between text-sm"><span>Total Service Charge:</span> <span>LKR ${totalServiceCharge.toFixed(2)}</span></p>
                    <p class="flex justify-between text-lg font-black text-emerald-700 border-t border-slate-200 pt-2"><span>Grand Total:</span> <span>LKR ${totalToPay.toFixed(2)}</span></p>
                </div>
            </div>`,
            icon: 'info',
            showCancelButton: true,
            confirmButtonText: 'Confirm & Pay',
            confirmButtonColor: '#059669'
        });

        if (confirm.isConfirmed) {
            const saleRecord = {
                date: new Date().toISOString(),
                items: saleItems,
                subTotal: totalToPay,
                discount: 0,
                total: totalToPay,
                paymentMethod: 'Cash',
                isUtility: true
            };
            const saleId = await db.sales.add(saleRecord);
            app.apiCall('/api/sales', 'POST', { id: saleId, ...saleRecord }, 'create_sale');

            await Swal.fire({ icon: 'success', title: 'Payments Successful', timer: 1500, showConfirmButton: false });
            app.printReceipt(saleId);
            app.navigate('dashboard');
        }
    },

    filterTable: (tableId, query) => {
            const rows = document.querySelectorAll(`#${tableId} tbody tr`);
            rows.forEach(row => {
                const text = row.innerText.toLowerCase();
                row.style.display = text.includes(query.toLowerCase()) ? '' : 'none';
            });
        },

            exportData: async () => {
                const data = {
                    items: await db.items.toArray(),
                    sales: await db.sales.toArray(),
                    repairs: await db.repairs.toArray(),
                    expenses: await db.expenses.toArray()
                };
                const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = `backup-${new Date().toISOString().split('T')[0]}.json`;
                a.click();
            },

    printReceipt: async (saleId) => {
        const sale = await db.sales.get(saleId);
        if (!sale) return;

        const shopDetails = {
            name: "Krishan Communication & Studio",
            address: "Hatharamanhandiya, Mapalassa, Sooriyawewa",
            phone: "076 928 1880 / 071 759 7335",
            email: "krishanpos@gmail.com",
            logo: "krishan_pos_logo_1775302997348.png"
        };

        const typeNames = {
            'CEB': 'ලංකා විදුලිබල මණ්ඩලය',
            'Water': 'ජාතික ජලසම්පාදන හා ජලාපවහන මණ්ඩලය',
            'Walawa': 'ඒකාබද්ධ වලව මව් නදී ප්‍රජාමූල සංවිධානය',
            'Other': 'වෙනත් බිල්පත් ගෙවීම්'
        };

        const saleDisplayName = sale.isUtility ? typeNames[sale.items[0].utilityType] || sale.items[0].utilityType : shopDetails.name;

        const printWindow = window.open('', '_blank', 'width=450,height=800');
        const itemsHTML = sale.items.map(item => `
            <div style="display:flex;justify-content:space-between;margin-bottom:2px;font-size:15px;">
                <div style="flex:1;padding-right:4px;">
                    <div style="font-weight:600;line-height:1.15;">${item.name}</div>
                    <div style="font-size:12px;font-weight:500;">${item.qty} x LKR ${item.price.toFixed(2)}</div>
                </div>
                <div style="font-weight:600;align-self:flex-end;white-space:nowrap;">LKR ${(item.qty*item.price).toFixed(2)}</div>
            </div>
        `).join('');

        const receiptHTML = `
            <!DOCTYPE html>
            <html>
            <head>
                <title>Bill #${saleId}</title>
                <style>
                    *{box-sizing:border-box;margin:0;padding:0;}
                    body{
                        font-family:system-ui,-apple-system,sans-serif;
                        font-size:15px;
                        line-height:1.15;
                        margin:0 auto;
                        padding:2px 5px;
                        width:80mm;
                        color:#000;
                        font-weight:400;
                    }
                    .sep{border-top:1.5px dashed #000;margin:3px 0;}
                    .gtotal{
                        font-size:20px;
                        font-weight:700;
                        border-top:2px solid #000;
                        border-bottom:3px double #000;
                        padding:3px 0;
                        display:flex;
                        justify-content:space-between;
                    }
                    .ubox{border:1.5px solid #000;padding:4px 6px;margin-bottom:3px;font-size:14px;font-weight:500;}
                    @page{margin:0;}
                    @media print{body{margin:0;padding:2px 3px;width:100%;}}
                </style>
            </head>
            <body>
                <!-- HEADER -->
                <div style="text-align:center;margin-bottom:2px;">
                    <div style="font-size:18px;font-weight:700;text-transform:uppercase;line-height:1.1;">${shopDetails.name}</div>
                    <div style="font-size:11px;font-weight:500;">Tel: ${shopDetails.phone}</div>
                </div>

                <div class="sep"></div>

                <!-- TITLE + META in one block -->
                <div style="font-size:13px;font-weight:500;">
                    <div style="text-align:center;font-size:14px;font-weight:700;text-transform:uppercase;">${sale.isUtility ? saleDisplayName : 'RECEIPT / INVOICE'}</div>
                    <div style="display:flex;justify-content:space-between;">
                        <span>#${saleId}</span>
                        <span>${new Date(sale.date).toLocaleDateString()} ${new Date(sale.date).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'})}</span>
                        <span>${sale.paymentMethod.toUpperCase()}</span>
                    </div>
                </div>

                <div class="sep"></div>

                <!-- UTILITY BILLS -->
                ${sale.isUtility ? sale.items.map(i => `
                <div class="ubox">
                    <div style="font-weight:700;text-decoration:underline;font-size:13px;">UTILITY PAYMENT</div>
                    <div>Type: ${i.utilityType === 'Other' ? (i.otherName||'Utility Bill') : (typeNames[i.utilityType]||i.utilityType)}</div>
                    <div>Acc: ${i.accNo}</div>
                    ${i.ref ? `<div>Ref: ${i.ref}</div>` : ''}
                    <div class="sep" style="margin:3px 0;"></div>
                    <div style="display:flex;justify-content:space-between;"><span>Bill Amt:</span><span>LKR ${i.billAmount.toFixed(2)}</span></div>
                    <div style="display:flex;justify-content:space-between;"><span>Charge:</span><span>LKR ${i.serviceCharge.toFixed(2)}</span></div>
                </div>
                `).join('') : ''}

                <!-- ITEMS -->
                <div>${itemsHTML}</div>

                <div class="sep"></div>

                <!-- TOTALS -->
                <div style="font-weight:500;">
                    ${sale.discount > 0 ? `
                    <div style="display:flex;justify-content:space-between;font-size:14px;"><span>SUBTOTAL:</span><span>LKR ${sale.subTotal.toFixed(2)}</span></div>
                    <div style="display:flex;justify-content:space-between;font-size:14px;"><span>DISCOUNT:</span><span>- LKR ${sale.discount.toFixed(2)}</span></div>
                    ` : ''}
                    <div class="gtotal"><span>TOTAL:</span><span>LKR ${sale.total.toFixed(2)}</span></div>
                </div>

                <div class="sep"></div>
                <div style="text-align:center;font-size:12px;font-weight:600;">THANK YOU!</div>

                <script>
                    window.onload = () => { 
                        window.print(); 
                    };
                </script>
            </body>
            </html>
        `;

        printWindow.document.write(receiptHTML);
        printWindow.document.close();
    },
};

// Start the app immediately or on DOM ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => app.init());
} else {
    app.init();
}
let html5QrCode;

// කැමරාව පෙන්වීමට
async function showCamera() {
    const cameraSection = document.getElementById('camera-section');
    cameraSection.classList.remove('hidden');

    if (!html5QrCode) {
        html5QrCode = new Html5Qrcode("reader");
    }

    const config = { fps: 15, qrbox: { width: 220, height: 150 } };

    html5QrCode.start({ facingMode: "environment" }, config, (decodedText) => {
        // බාර්කෝඩ් එකක් අහුවුණාම මේක වැඩ කරනවා
        Swal.fire({
            toast: true,
            position: 'top-end',
            icon: 'success',
            title: 'Scanned: ' + decodedText,
            showConfirmButton: false,
            timer: 2000
        });

        // මේ code එක POS එකේ search එකට auto දාන්න මේක පාවිච්චි කරන්න පුළුවන්
        // app.searchProduct(decodedText); 

    }).catch(err => console.error("Camera error:", err));
}

// කැමරාව නවත්වන්න
async function hideCamera() {
    if (html5QrCode) {
        await html5QrCode.stop();
    }
    document.getElementById('camera-section').classList.add('hidden');
}