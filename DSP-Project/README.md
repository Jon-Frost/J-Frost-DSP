# DataForge — Interactive Dashboard Builder

A full-stack web application built with Flask, SQLite, and Plotly for uploading datasets, building interactive multi-chart dashboards, and chatting with an AI assistant about your data.

![Python](https://img.shields.io/badge/Python-3.9+-blue)
![Flask](https://img.shields.io/badge/Flask-3.0-green)
![Plotly](https://img.shields.io/badge/Plotly-5.18-purple)

---

## Features

### Authentication & Sessions
- User registration and login with password hashing
- Persistent sessions via Flask session management
- Per-user data isolation — each user sees only their own datasets and dashboards

### File Upload
- Drag-and-drop or click-to-browse file uploader
- Supports **CSV**, **Excel** (.xlsx/.xls), **JSON**, and **TSV** formats
- Automatic column type detection (numeric, categorical, datetime)
- File size limit: 50MB

### Interactive Dashboard Builder
- **11 chart types**: Bar, Line, Scatter, Pie, Histogram, Box, Area, Heatmap, Violin, Sunburst, Treemap
- Configurable axes, color grouping, and aggregation (sum, mean, count, min, max, median)
- **Resizable grid layout**: Switch between 1, 2, or 3 column layouts
- Toggle individual charts to full-width
- All charts are interactive (zoom, pan, hover tooltips) via Plotly
- Save dashboards to the database and reload them later

### Data Exploration
- Live data preview table (first 30 rows)
- Per-column statistics: min/max/mean for numeric, unique/top values for categorical
- Null count tracking

### AI Data Assistant (Gemini Integration)
- Chat with Google's Gemini 2.0 Flash model about your dataset
- AI sees your column statistics and sample data for context
- Ask for insights, trends, patterns, or chart suggestions
- AI can suggest charts that you can add to your dashboard with one click
- Persistent chat history per dataset

### Export
- Download dashboards as standalone HTML files with embedded Plotly charts
- Exported files work offline — no server needed

---

## Quick Start

### 1. Install dependencies

```bash
cd dashboard_app
pip install -r requirements.txt
```

### 2. Run the application

```bash
python app.py
```

The app will start at **http://localhost:5000**

### 3. Create an account

Open your browser, go to `http://localhost:5000`, and register a new account.

### 4. Configure Gemini API (optional)

To use the AI chat assistant:

1. Go to **Settings** (gear icon in the navbar)
2. Get a free API key from [Google AI Studio](https://aistudio.google.com/apikey)
3. Paste the key and click Save

The key is stored in your browser's localStorage and sent only to Google's API.

---

## Project Structure

```
dashboard_app/
├── app.py                  # Main Flask application
├── requirements.txt        # Python dependencies
├── dashboard.db           # SQLite database (auto-created)
├── static/
│   └── uploads/           # Uploaded dataset files
└── templates/
    ├── base.html          # Base layout with navbar & styles
    ├── login.html         # Login page
    ├── register.html      # Registration page
    ├── dashboard.html     # Home page — datasets & dashboards
    ├── builder.html       # Dashboard builder with charts & AI chat
    ├── settings.html      # API key configuration
    └── export.html        # Standalone export template
```

---

## Tech Stack

| Component       | Technology                     |
|-----------------|--------------------------------|
| Backend         | Python Flask                   |
| Database        | SQLite                         |
| Charts          | Plotly.js + Plotly Express     |
| Data Processing | Pandas                         |
| AI Assistant    | Google Gemini 2.0 Flash API    |
| Auth            | Flask sessions + SHA-256       |
| Frontend        | Vanilla JS + Custom CSS        |

---

## API Endpoints

| Endpoint                             | Method | Description                    |
|--------------------------------------|--------|--------------------------------|
| `/upload`                            | POST   | Upload a dataset file          |
| `/api/dataset/<id>/preview`          | GET    | Get dataset preview & stats    |
| `/api/dataset/<id>/columns`          | GET    | Get column metadata            |
| `/api/dataset/<id>/delete`           | POST   | Delete a dataset               |
| `/api/chart`                         | POST   | Generate a Plotly chart        |
| `/api/dashboard/save`                | POST   | Save dashboard configuration   |
| `/api/dashboard/<id>`                | GET    | Load a saved dashboard         |
| `/api/dashboard/<id>/delete`         | POST   | Delete a dashboard             |
| `/api/dashboard/<id>/export`         | GET    | Export dashboard as HTML       |
| `/api/chat`                          | POST   | Send message to Gemini AI      |
| `/api/chat/history/<dataset_id>`     | GET    | Get chat history               |
| `/api/chat/clear/<dataset_id>`       | POST   | Clear chat history             |

---

## License

MIT
