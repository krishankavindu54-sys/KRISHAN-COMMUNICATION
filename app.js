
// Database Initialization
const db = new Dexie('KrishanPOS_DB');
db.version(1).stores({
    items: '++id, name, barcode, category, type',
    repairs: '++id, customerName, phoneModel, status, createdAt',
    sales: '++id, date, total, paymentMethod',
    expenses: '++id, date, category',
    creditors: '++id, name, amount, lastUpdated, type'
});

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

// App Logic
const app = {
    state: {
        cart: [],
        currentView: 'dashboard',
        posCategory: null,
        inventoryCategory: 'All',
        lastAddedCategory: 'General',
        scanner: null,
        modalScanner: null,
        selectedCreditor: null
    },

    currentUser: null,

    // --- SUPABASE DIRECT SYNC & API BRIDGE ---
    apiCall: async (tableOrPath, method = 'GET', data = null) => {
        if (typeof supabase === 'undefined') {
            console.warn('Supabase client is not initialized.');
            return null;
        }

        const tableName = tableOrPath.replace(/^\/?api\//, '').split('/')[0];

        try {
            if (method === 'GET') {
                const { data: resData, error } = await supabase.from(tableName).select('*');
                if (error) throw error;
                return resData;
            } else if (method === 'POST') {
                const { data: resData, error } = await supabase.from(tableName).insert([data]).select();
                if (error) throw error;
                return resData ? resData[0] : null;
            } else if (method === 'PUT') {
                const id = data.id;
                const updateData = { ...data };
                delete updateData.id;
                const { data: resData, error } = await supabase.from(tableName).update(updateData).eq('id', id).select();
                if (error) throw error;
                return resData ? resData[0] : null;
            } else if (method === 'DELETE') {
                const id = tableOrPath.split('/').pop();
                const { error } = await supabase.from(tableName).delete().eq('id', id);
                if (error) throw error;
                return { success: true };
            }
        } catch (err) {
            console.warn(`Supabase query failed for ${tableName} (${method}):`, err.message);
            return null;
        }
    },

    realtime: {
        socket: null,
        status: 'connected',
        syncMode: 'supabase',
        deviceCount: 1,
        lastSyncTime: null,
        pendingQueue: [],

        init: () => {
            app.realtime.setStatus('connected');
            app.syncWithBackend(false);
            app.realtime.startPollingFallback();
        },

        startKeepAlive: () => { },

        startPollingFallback: () => {
            setInterval(async () => {
                if (typeof supabase === 'undefined') return;
                try {
                    const { data: serverItems } = await supabase.from('items').select('*');
                    if (Array.isArray(serverItems) && serverItems.length > 0) {
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

                        if (hasChanges) {
                            if (app.state.currentView === 'pos') app.renderPOS();
                            if (app.state.currentView === 'products') app.renderInventory();
                            if (app.state.currentView === 'dashboard') app.renderDashboard();
                        }
                    }
                    app.realtime.lastSyncTime = new Date();
                    app.realtime.updateStatusUI();
                } catch (e) {
                    console.warn('Sync poll check:', e?.message);
                }
            }, 5000);
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

            widget.className = 'flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-emerald-50 dark:bg-emerald-950/40 border border-emerald-200 dark:border-emerald-800 text-emerald-700 dark:text-emerald-300 text-xs font-bold cursor-pointer transition-all hover:scale-105 shadow-sm';
            if (pulse) pulse.className = 'animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75';
            if (dot) dot.className = 'relative inline-flex rounded-full h-2 w-2 bg-emerald-500';
            if (text) text.textContent = 'Cloud Live';
            if (badge) {
                badge.textContent = 'Supabase';
                badge.className = 'px-1.5 py-0.2 rounded-full bg-emerald-200/60 dark:bg-emerald-800/60 text-[10px]';
            }
        },

        flushOfflineQueue: async () => { },
        queueOfflineMutation: () => { }
    },

    // Supabase සම්පූර්ණ Data Synchronization එක
    syncWithBackend: async (refreshView = false) => {
        if (typeof supabase === 'undefined') {
            console.warn('Supabase client load වී නොමැත.');
            return false;
        }

        try {
            const [itemsRes, salesRes, repairsRes, expensesRes, creditorsRes, bankRes, supRes, billsRes] = await Promise.all([
                supabase.from('items').select('*'),
                supabase.from('sales').select('*'),
                supabase.from('repairs').select('*'),
                supabase.from('expenses').select('*'),
                supabase.from('creditors').select('*'),
                supabase.from('bankTransactions').select('*'),
                supabase.from('suppliers').select('*'),
                supabase.from('purchaseBills').select('*')
            ]);

            if (itemsRes.data && itemsRes.data.length > 0) {
                await db.items.clear();
                await db.items.bulkPut(itemsRes.data);
            }
            if (salesRes.data && salesRes.data.length > 0) {
                await db.sales.clear();
                await db.sales.bulkPut(salesRes.data);
            }
            if (repairsRes.data && repairsRes.data.length > 0) {
                await db.repairs.clear();
                await db.repairs.bulkPut(repairsRes.data);
            }
            if (expensesRes.data && expensesRes.data.length > 0) {
                await db.expenses.clear();
                await db.expenses.bulkPut(expensesRes.data);
            }
            if (creditorsRes.data && creditorsRes.data.length > 0) {
                await db.creditors.clear();
                await db.creditors.bulkPut(creditorsRes.data);
            }
            if (bankRes.data && bankRes.data.length > 0) {
                await db.bankTransactions.clear();
                await db.bankTransactions.bulkPut(bankRes.data);
            }
            if (supRes.data && supRes.data.length > 0) {
                await db.suppliers.clear();
                await db.suppliers.bulkPut(supRes.data);
            }
            if (billsRes.data && billsRes.data.length > 0) {
                await db.purchaseBills.clear();
                await db.purchaseBills.bulkPut(billsRes.data);
            }

            app.realtime.lastSyncTime = new Date();
            app.realtime.setStatus('connected');

            if (refreshView && app.state.currentView) {
                app.navigate(app.state.currentView);
            }
            return true;
        } catch (err) {
            console.error('Supabase sync error:', err.message);
            return false;
        }
    },

    triggerManualSync: async () => {
        const icon = document.getElementById('manual-sync-icon');
        if (icon) icon.classList.add('fa-spin');
        try {
            await app.syncWithBackend(true);
            Swal.fire({
                toast: true,
                position: 'top-end',
                icon: 'success',
                title: 'Data Synced via Supabase!',
                timer: 1500,
                showConfirmButton: false
            });
        } catch (e) {
            Swal.fire({
                toast: true,
                position: 'top-end',
                icon: 'error',
                title: 'Sync failed: ' + e.message,
                timer: 2000,
                showConfirmButton: false
            });
        } finally {
            if (icon) icon.classList.remove('fa-spin');
        }
    },

    init: async () => {
        try {
            app.updateDateTime();
            setInterval(app.updateDateTime, 1000);
            app.initTheme();

            app.realtime.init();

            const isAuth = await app.checkAuth();
            if (!isAuth) {
                app.showLoginOverlay();
                return;
            }

            await app.syncWithBackend(false);
            app.updateShopProfileHeader();
            app.navigate('dashboard');
        } catch (e) {
            console.error('App init error:', e);
            try { app.navigate('dashboard'); } catch (err) { }
        }
    },

    checkAuth: async () => {
        const saved = localStorage.getItem('pos_current_user');
        if (saved) {
            try {
                app.currentUser = JSON.parse(saved);
                app.updateUserHeader();
                return true;
            } catch (err) { }
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
            localStorage.removeItem('pos_current_user');
            localStorage.removeItem('pos_token');
            app.currentUser = null;
            window.location.href = 'login.html';
        }
    },

    initTheme: () => {
        const isDark = localStorage.getItem('krishan_pos_theme') === 'dark';
        if (isDark) {
            document.documentElement.classList.add('dark');
        } else {
            document.documentElement.classList.remove('dark');
        }
    },

    toggleDarkMode: () => {
        const html = document.documentElement;
        if (html.classList.contains('dark')) {
            html.classList.remove('dark');
            localStorage.setItem('krishan_pos_theme', 'light');
        } else {
            html.classList.add('dark');
            localStorage.setItem('krishan_pos_theme', 'dark');
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
    },

    updateShopProfileHeader: () => {
        const profile = app.getShopProfile();
        const shopLabel = document.getElementById('shop-profile-name');
        if (shopLabel) shopLabel.textContent = profile.shopName;
    },

    updateDateTime: () => {
        const now = new Date();
        const timeEl = document.getElementById('current-time');
        const dateEl = document.getElementById('current-date');
        if (timeEl) timeEl.textContent = now.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
        if (dateEl) dateEl.textContent = now.toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' });
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
    }