# 🏪 Krishan Communication & Studio - POS System

A modern, responsive, full-featured Point of Sale (POS) and retail management system built for **Krishan Communication & Studio**.

---

## ✨ Features

- 🛒 **Point of Sale (Billing):** Touch-friendly category grid, barcode scanning, custom item pricing, discounts, cash/card/credit payment options.
- 📦 **Inventory & Service Management:** Track products with low-stock alerts, manage non-inventory services (photocopies, passport photos, etc.).
- 📱 **Repair Tracker (Job Cards):** Track phone & electronic repair jobs from intake to delivery.
- 💳 **Credit Book (Creditors):** Track customer debts and supplier payables with easy settlement logs.
- 🏦 **Bank & Reload Transactions:** Track cash-in vs digital wallet / bank transfer transactions with commission calculation.
- 👥 **User Roles & Security:** Admin and Cashier roles with secure authentication.
- 📊 **Reports & Analytics:** Daily, monthly sales, revenue breakdown, expense tracking, and automated profit calculator.
- 💾 **Dual-Storage Engine:** Works with **SQLite backend** (Node.js/Express) and **Dexie.js IndexedDB** for 100% offline standalone capability.

---

## 🔑 Default Login Credentials

| Role | Username | Password |
| :--- | :--- | :--- |
| **Admin** | `admin` | `admin123` |
| **Cashier** | `cashier` | `cashier123` |

---

## 🚀 How to Run Locally

### Option 1: One-Click Launcher (Windows)
Double-click **`Start-POS.bat`** in this folder. It will start the server and open the app in your browser at `http://krishanpos.local` or `http://localhost:3000`.

### Option 2: Using Node.js Terminal
1. Open a terminal in this project folder:
```bash
npm install
npm start
```
2. Open your browser and navigate to:
```
http://localhost:3000
```

### Option 3: Custom Shop Domain Setup (Optional)
Right-click **`Setup-Shop-Domain.bat`** and click **Run as administrator**.
You can then open the POS anytime using:
- `http://krishanpos.local`
- `http://krishan.pos`

---

## 🌐 Deploy to GitHub Pages / Static Hosting

This POS system is built to run standalone in any modern browser without needing a Node.js server!

1. Push this repository to GitHub.
2. In GitHub, go to **Settings** > **Pages**.
3. Under **Branch**, select `main` (or `master`) and `/ (root)` folder. Click **Save**.
4. Your POS system will be live at `https://<your-username>.github.io/<repo-name>/index.html`!

---

## 📁 Project Structure

```
├── index.html              # Main POS Dashboard and billing application
├── login.html              # Secure user authentication interface
├── app.js                  # Frontend logic & Dexie.js offline DB
├── server.js               # Express.js REST API & SQLite Server
├── database.js             # SQLite / JSON database storage engine
├── package.json            # Node.js dependencies & scripts
├── Start-POS.bat           # 1-Click Windows server startup
├── Setup-Shop-Domain.bat   # 1-Click Windows domain setup
├── .gitignore              # Git ignore configuration
└── README.md               # Project documentation
```

---

## 📄 License
MIT License - Krishan Communication & Studio &copy; 2026.
