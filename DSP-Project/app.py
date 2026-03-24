import os
import json
import uuid
import sqlite3
import hashlib
import secrets
import pandas as pd
from functools import wraps
from datetime import datetime
from flask import (
    Flask, render_template, request, redirect, url_for,
    session, flash, jsonify, send_file, g
)
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
import io
import base64
import numpy as np
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(__file__)
load_dotenv(os.path.join(BASE_DIR, '.env'), override=True)

app = Flask(__name__)
app.secret_key = secrets.token_hex(32)
app.config['UPLOAD_FOLDER'] = os.path.join(BASE_DIR, 'static', 'uploads')
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max
app.config['GEMINI_API_KEY'] = os.getenv('GEMINI_API_KEY', '').strip().strip('"').strip("'")
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls', 'json', 'tsv'}

DATABASE = os.path.join(BASE_DIR, 'dashboard.db')

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# ─── Database ───────────────────────────────────────────────────────────────

def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
    return g.db

@app.teardown_appcontext
def close_db(exception):
    db = g.pop('db', None)
    if db is not None:
        db.close()

def init_db():
    db = sqlite3.connect(DATABASE)
    db.executescript('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS datasets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            filename TEXT NOT NULL,
            original_name TEXT NOT NULL,
            file_type TEXT NOT NULL,
            row_count INTEGER,
            col_count INTEGER,
            columns_json TEXT,
            uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS dashboards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            dataset_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            config_json TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (dataset_id) REFERENCES datasets(id)
        );
        CREATE TABLE IF NOT EXISTS chat_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            dataset_id INTEGER NOT NULL,
            role TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
    ''')
    db.close()

init_db()

def hash_password(password):
    return generate_password_hash(password)

def check_password(password, password_hash):
    return check_password_hash(password_hash, password)

# ─── Auth decorator ─────────────────────────────────────────────────────────

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please log in first.', 'warning')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def infer_columns_info(df):
    # Record column info for quick processing
    columns_info = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        if 'int' in dtype or 'float' in dtype:
            col_type = 'numeric'
        elif 'datetime' in dtype:
            col_type = 'datetime'
        else:
            col_type = 'categorical'
        columns_info.append({'name': col, 'type': col_type, 'dtype': dtype})
    return columns_info

def build_dataset_highlights(df, columns):
    # Generate highlight bullets for a dataset.
    highlights = []
    row_count = len(df)
    col_count = len(df.columns)
    total_cells = row_count * col_count if row_count and col_count else 0
    null_count = int(df.isnull().sum().sum()) if total_cells else 0
    completeness = ((total_cells - null_count) / total_cells * 100) if total_cells else 0

    highlights.append(
        f"{row_count:,} rows across {col_count} columns with {completeness:.1f}% complete values."
    )

    numeric_cols = [c['name'] for c in columns if c.get('type') == 'numeric' and c.get('name') in df.columns]
    categorical_cols = [c['name'] for c in columns if c.get('type') != 'numeric' and c.get('name') in df.columns]
    highlights.append(
        f"Contains {len(numeric_cols)} numeric columns and {len(categorical_cols)} categorical/text columns."
    )

    if numeric_cols:
        spread_col = max(
            numeric_cols,
            key=lambda col: float(df[col].std()) if pd.notna(df[col].std()) else float('-inf')
        )
        spread_val = df[spread_col].std()
        if pd.notna(spread_val):
            highlights.append(
                f"{spread_col} shows the widest variation with a standard deviation of {float(spread_val):.2f}."
            )

    if categorical_cols:
        best_cat = None
        best_share = -1
        best_mode = None
        for col in categorical_cols:
            mode = df[col].mode(dropna=True)
            if mode.empty:
                continue
            top_value = mode.iloc[0]
            share = float((df[col] == top_value).mean())
            if share > best_share:
                best_share = share
                best_cat = col
                best_mode = top_value
        if best_cat is not None:
            highlights.append(
                f"{best_cat} is led by '{best_mode}' appearing in {best_share * 100:.1f}% of rows."
            )

    if null_count:
        missing_col = df.isnull().sum().sort_values(ascending=False).index[0]
        missing_val = int(df[missing_col].isnull().sum())
        if missing_val > 0:
            highlights.append(
                f"Most missing values are in {missing_col} with {missing_val:,} empty entries."
            )

    return highlights[:4]

# ─── Helper: load dataset ──────────────────────────────────────────────────

def load_dataset(dataset_row):
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], dataset_row['filename'])
    ft = dataset_row['file_type']
    if ft == 'csv':
        return pd.read_csv(filepath)
    elif ft in ('xlsx', 'xls'):
        return pd.read_excel(filepath)
    elif ft == 'json':
        return pd.read_json(filepath)
    elif ft == 'tsv':
        return pd.read_csv(filepath, sep='\t')
    return None

# ─── Routes: Auth ───────────────────────────────────────────────────────────

@app.route('/')
def index():
    if 'user_id' in session:
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        db = get_db()
        user = db.execute('SELECT * FROM users WHERE username = ?', (username,)).fetchone()
        if user and check_password(password, user['password_hash']):
            session['user_id'] = user['id']
            session['username'] = user['username']
            return redirect(url_for('dashboard'))
        flash('Invalid credentials.', 'error')
    return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        email = request.form.get('email', '').strip()
        password = request.form.get('password', '')
        confirm = request.form.get('confirm_password', '')
        if not username or not email or not password:
            flash('All fields are required.', 'error')
        elif password != confirm:
            flash('Passwords do not match.', 'error')
        elif len(password) < 6:
            flash('Password must be at least 6 characters.', 'error')
        else:
            db = get_db()
            try:
                db.execute(
                    'INSERT INTO users (username, email, password_hash) VALUES (?, ?, ?)',
                    (username, email, hash_password(password))
                )
                db.commit()
                flash('Account created! Please log in.', 'success')
                return redirect(url_for('login'))
            except sqlite3.IntegrityError:
                flash('Username or email already exists.', 'error')
    return render_template('register.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

# ─── Routes: Dashboard ─────────────────────────────────────────────────────

@app.route('/dashboard')
@login_required
def dashboard():
    db = get_db()
    datasets = db.execute(
        'SELECT * FROM datasets WHERE user_id = ? ORDER BY uploaded_at DESC',
        (session['user_id'],)
    ).fetchall()
    dashboards = db.execute(
        'SELECT d.*, ds.original_name as dataset_name FROM dashboards d '
        'JOIN datasets ds ON d.dataset_id = ds.id '
        'WHERE d.user_id = ? ORDER BY d.updated_at DESC',
        (session['user_id'],)
    ).fetchall()
    return render_template('dashboard.html', datasets=datasets, dashboards=dashboards)

# ─── Routes: File Upload ───────────────────────────────────────────────────

@app.route('/upload', methods=['POST'])
@login_required
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file selected'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    if not allowed_file(file.filename):
        return jsonify({'error': f'File type not allowed. Use: {", ".join(ALLOWED_EXTENSIONS)}'}), 400

    original_name = secure_filename(file.filename)
    ext = original_name.rsplit('.', 1)[1].lower()
    unique_name = f"{uuid.uuid4().hex}_{original_name}"
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], unique_name)
    file.save(filepath)

    try:
        if ext == 'csv':
            df = pd.read_csv(filepath)
        elif ext in ('xlsx', 'xls'):
            df = pd.read_excel(filepath)
        elif ext == 'json':
            df = pd.read_json(filepath)
        elif ext == 'tsv':
            df = pd.read_csv(filepath, sep='\t')
        else:
            df = pd.DataFrame()

        columns_info = infer_columns_info(df)

        db = get_db()
        db.execute(
            'INSERT INTO datasets (user_id, filename, original_name, file_type, row_count, col_count, columns_json) '
            'VALUES (?, ?, ?, ?, ?, ?, ?)',
            (session['user_id'], unique_name, original_name, ext, len(df), len(df.columns), json.dumps(columns_info))
        )
        db.commit()
        return jsonify({
            'success': True,
            'message': f'Uploaded {original_name} ({len(df)} rows, {len(df.columns)} columns)'
        })
    except Exception as e:
        os.remove(filepath)
        return jsonify({'error': f'Error processing file: {str(e)}'}), 400

# ─── Routes: Dataset API ───────────────────────────────────────────────────

@app.route('/api/dataset/<int:dataset_id>/preview')
@login_required
def dataset_preview(dataset_id):
    db = get_db()
    dataset = db.execute(
        'SELECT * FROM datasets WHERE id = ? AND user_id = ?',
        (dataset_id, session['user_id'])
    ).fetchone()
    if not dataset:
        return jsonify({'error': 'Dataset not found'}), 404
    df = load_dataset(dataset)
    if df is None:
        return jsonify({'error': 'Could not load dataset'}), 500
    columns = json.loads(dataset['columns_json'])
    preview = df.head(50).to_dict(orient='records')
    stats = {}
    for col_info in columns:
        col = col_info['name']
        if col_info['type'] == 'numeric':
            stats[col] = {
                'min': float(df[col].min()) if pd.notna(df[col].min()) else None,
                'max': float(df[col].max()) if pd.notna(df[col].max()) else None,
                'mean': float(df[col].mean()) if pd.notna(df[col].mean()) else None,
                'nulls': int(df[col].isnull().sum())
            }
        else:
            stats[col] = {
                'unique': int(df[col].nunique()),
                'top': str(df[col].mode().iloc[0]) if len(df[col].mode()) > 0 else None,
                'nulls': int(df[col].isnull().sum())
            }
    highlights = build_dataset_highlights(df, columns)
    return jsonify({
        'columns': columns,
        'preview': preview,
        'stats': stats,
        'highlights': highlights,
        'row_count': dataset['row_count'],
        'col_count': dataset['col_count']
    })

@app.route('/api/dataset/<int:dataset_id>/columns')
@login_required
def dataset_columns(dataset_id):
    db = get_db()
    dataset = db.execute(
        'SELECT columns_json FROM datasets WHERE id = ? AND user_id = ?',
        (dataset_id, session['user_id'])
    ).fetchone()
    if not dataset:
        return jsonify({'error': 'Not found'}), 404
    return jsonify(json.loads(dataset['columns_json']))

@app.route('/api/dataset/<int:dataset_id>/clean', methods=['POST'])
@login_required
def clean_dataset(dataset_id):
    db = get_db()
    dataset = db.execute(
        'SELECT * FROM datasets WHERE id = ? AND user_id = ?',
        (dataset_id, session['user_id'])
    ).fetchone()
    if not dataset:
        return jsonify({'error': 'Dataset not found'}), 404

    df = load_dataset(dataset)
    if df is None:
        return jsonify({'error': 'Could not load dataset'}), 500

    cleaned_df = df.copy()
    numeric_cols = cleaned_df.select_dtypes(include='number').columns.tolist()
    filled_cells = 0
    cleaned_columns = 0

    for col in numeric_cols:
        missing_count = int(cleaned_df[col].isnull().sum())
        if missing_count == 0:
            continue

        mean_value = cleaned_df[col].mean()
        if pd.isna(mean_value):
            continue

        cleaned_df[col] = cleaned_df[col].fillna(mean_value)
        filled_cells += missing_count
        cleaned_columns += 1

    if filled_cells == 0:
        return jsonify({'error': 'No numeric missing values were found to clean.'}), 400

    original_base = os.path.splitext(dataset['original_name'])[0]
    cleaned_name = f"{original_base}_cleaned.csv"
    unique_name = f"{uuid.uuid4().hex}_{secure_filename(cleaned_name)}"
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], unique_name)
    cleaned_df.to_csv(filepath, index=False)

    columns_info = infer_columns_info(cleaned_df)
    cursor = db.execute(
        'INSERT INTO datasets (user_id, filename, original_name, file_type, row_count, col_count, columns_json) '
        'VALUES (?, ?, ?, ?, ?, ?, ?)',
        (
            session['user_id'],
            unique_name,
            cleaned_name,
            'csv',
            len(cleaned_df),
            len(cleaned_df.columns),
            json.dumps(columns_info)
        )
    )
    db.commit()

    return jsonify({
        'success': True,
        'dataset_id': cursor.lastrowid,
        'message': f'Created cleaned dataset with {filled_cells:,} missing values filled across {cleaned_columns} numeric columns.'
    })

# ─── Routes: Chart Generation ──────────────────────────────────────────────

COLORS = ['#5b6ef5','#8b5cf6','#34d399','#fbbf24','#f87171','#38bdf8','#fb923c','#a78bfa','#4ade80','#f472b6']

def safe_list(series):
    """Convert pandas series to JSON-safe list."""
    return [None if pd.isna(v) else (int(v) if isinstance(v, (np.integer,)) else (float(v) if isinstance(v, (np.floating,)) else v)) for v in series]

def axis_value_label(column_name, agg=None, default='Value'):
    """Build a human-friendly axis label for chart values."""
    if agg == 'count':
        return 'Count'
    if column_name and agg:
        return f'{agg.title()} of {column_name}'
    return column_name or default

def apply_axis_titles(layout, chart_type, x_col=None, y_col=None, color_col=None, agg=None):
    """Apply axis titles for cartesian Plotly charts based on selected columns."""
    if chart_type in ('bar', 'line', 'scatter', 'area'):
        layout['xaxis']['title'] = {'text': x_col or ''}
        layout['yaxis']['title'] = {'text': axis_value_label(y_col, agg if chart_type == 'bar' else None)}
    elif chart_type == 'histogram':
        layout['xaxis']['title'] = {'text': x_col or ''}
        layout['yaxis']['title'] = {'text': 'Count'}
    elif chart_type in ('box', 'violin'):
        if x_col:
            layout['xaxis']['title'] = {'text': x_col}
        elif color_col:
            layout['xaxis']['title'] = {'text': color_col}
        else:
            layout['xaxis']['title'] = {'text': ''}

        if y_col:
            layout['yaxis']['title'] = {'text': y_col}
        elif x_col:
            layout['yaxis']['title'] = {'text': 'Value'}
    elif chart_type == 'heatmap':
        layout['xaxis']['title'] = {'text': 'Columns'}
        layout['yaxis']['title'] = {'text': 'Columns'}

def apply_axis_style_defaults(layout):
    """Make axes easier to read and less likely to clip labels."""
    for axis_name in ('xaxis', 'yaxis'):
        axis = layout.get(axis_name)
        if not axis:
            continue

        axis.update({
            'showgrid': True,
            'nticks': 12,
            'automargin': True,
            'ticks': 'outside',
            'ticklen': 6,
            'tickcolor': '#9bb9c7',
            'gridwidth': 1,
            'title': {
                **axis.get('title', {}),
                'standoff': 14,
            },
        })

@app.route('/api/chart', methods=['POST'])
@login_required
def generate_chart():
    data = request.json
    dataset_id = data.get('dataset_id')
    chart_type = data.get('chart_type', 'bar')
    x_col = data.get('x')
    y_col = data.get('y')
    color_col = data.get('color')
    agg = data.get('aggregation', 'sum')
    title = data.get('title', '')

    db = get_db()
    dataset = db.execute(
        'SELECT * FROM datasets WHERE id = ? AND user_id = ?',
        (dataset_id, session['user_id'])
    ).fetchone()
    if not dataset:
        return jsonify({'error': 'Dataset not found'}), 404

    df = load_dataset(dataset)
    if df is None:
        return jsonify({'error': 'Could not load data'}), 500

    try:
        traces = []
        layout = {
            'title': {'text': title or f'{chart_type.title()} Chart', 'font': {'size': 14}},
            'paper_bgcolor': 'rgba(0,0,0,0)',
            'plot_bgcolor': 'rgba(0,0,0,0)',
            'font': {'family': 'JetBrains Mono, monospace', 'size': 12, 'color': '#c8cad0'},
            'margin': {'l': 75, 'r': 30, 't': 60, 'b': 80},
            'xaxis': {'gridcolor': '#2a2d38', 'zerolinecolor': '#2a2d38'},
            'yaxis': {'gridcolor': '#2a2d38', 'zerolinecolor': '#2a2d38'},
            'legend': {'font': {'size': 11}},
            'colorway': COLORS,
        }

        if chart_type in ('bar', 'line', 'scatter', 'area'):
            if color_col and color_col != '':
                groups = df[color_col].dropna().unique()
                for i, grp in enumerate(groups):
                    sub = df[df[color_col] == grp]
                    if agg and y_col and chart_type == 'bar':
                        sub = sub.groupby(x_col)[y_col].agg(agg).reset_index()
                    trace = {
                        'x': safe_list(sub[x_col]),
                        'y': safe_list(sub[y_col]) if y_col else None,
                        'name': str(grp),
                        'marker': {'color': COLORS[i % len(COLORS)]},
                    }
                    if chart_type == 'bar':
                        trace['type'] = 'bar'
                    elif chart_type == 'line':
                        trace['type'] = 'scatter'
                        trace['mode'] = 'lines+markers'
                    elif chart_type == 'scatter':
                        trace['type'] = 'scatter'
                        trace['mode'] = 'markers'
                    elif chart_type == 'area':
                        trace['type'] = 'scatter'
                        trace['mode'] = 'lines'
                        trace['fill'] = 'tozeroy'
                    traces.append(trace)
                if chart_type == 'bar':
                    layout['barmode'] = 'group'
            else:
                work_df = df
                if agg and y_col and chart_type == 'bar':
                    work_df = df.groupby(x_col)[y_col].agg(agg).reset_index()
                trace = {
                    'x': safe_list(work_df[x_col]),
                    'y': safe_list(work_df[y_col]) if y_col else None,
                    'marker': {'color': COLORS[0]},
                }
                if chart_type == 'bar':
                    trace['type'] = 'bar'
                elif chart_type == 'line':
                    trace['type'] = 'scatter'
                    trace['mode'] = 'lines+markers'
                elif chart_type == 'scatter':
                    trace['type'] = 'scatter'
                    trace['mode'] = 'markers'
                elif chart_type == 'area':
                    trace['type'] = 'scatter'
                    trace['mode'] = 'lines'
                    trace['fill'] = 'tozeroy'
                traces.append(trace)

        elif chart_type == 'pie':
            work_df = df
            if agg and y_col:
                work_df = df.groupby(x_col)[y_col].agg(agg).reset_index()
            traces.append({
                'type': 'pie',
                'labels': safe_list(work_df[x_col]),
                'values': safe_list(work_df[y_col]) if y_col else None,
                'marker': {'colors': COLORS},
                'textfont': {'color': '#e8eaef'},
            })

        elif chart_type == 'histogram':
            traces.append({
                'type': 'histogram',
                'x': safe_list(df[x_col]),
                'marker': {'color': COLORS[0]},
            })

        elif chart_type == 'box':
            if color_col and color_col != '':
                groups = df[color_col].dropna().unique()
                for i, grp in enumerate(groups):
                    sub = df[df[color_col] == grp]
                    traces.append({
                        'type': 'box',
                        'y': safe_list(sub[y_col]) if y_col else safe_list(sub[x_col]),
                        'name': str(grp),
                        'marker': {'color': COLORS[i % len(COLORS)]},
                    })
            else:
                traces.append({
                    'type': 'box',
                    'x': safe_list(df[x_col]) if x_col else None,
                    'y': safe_list(df[y_col]) if y_col else None,
                    'marker': {'color': COLORS[0]},
                })

        elif chart_type == 'violin':
            if color_col and color_col != '':
                groups = df[color_col].dropna().unique()
                for i, grp in enumerate(groups):
                    sub = df[df[color_col] == grp]
                    traces.append({
                        'type': 'violin',
                        'y': safe_list(sub[y_col]) if y_col else safe_list(sub[x_col]),
                        'name': str(grp),
                        'marker': {'color': COLORS[i % len(COLORS)]},
                        'box': {'visible': True},
                        'meanline': {'visible': True},
                    })
            else:
                traces.append({
                    'type': 'violin',
                    'x': safe_list(df[x_col]) if x_col else None,
                    'y': safe_list(df[y_col]) if y_col else None,
                    'marker': {'color': COLORS[0]},
                    'box': {'visible': True},
                    'meanline': {'visible': True},
                })

        elif chart_type == 'heatmap':
            numeric_cols = df.select_dtypes(include='number').columns.tolist()
            corr = df[numeric_cols].corr()
            traces.append({
                'type': 'heatmap',
                'z': corr.values.tolist(),
                'x': list(corr.columns),
                'y': list(corr.columns),
                'colorscale': 'RdBu',
                'zmid': 0,
            })
            layout['title']['text'] = title or 'Correlation Heatmap'

        elif chart_type == 'sunburst':
            labels, parents, values = [], [], []
            if y_col:
                grouped = df.groupby(x_col)[y_col].sum()
                for cat, val in grouped.items():
                    labels.append(str(cat))
                    parents.append('')
                    values.append(float(val))
            traces.append({
                'type': 'sunburst',
                'labels': labels,
                'parents': parents,
                'values': values,
                'marker': {'colors': COLORS},
            })

        elif chart_type == 'treemap':
            labels, parents, values = [], [], []
            if y_col:
                grouped = df.groupby(x_col)[y_col].sum()
                for cat, val in grouped.items():
                    labels.append(str(cat))
                    parents.append('')
                    values.append(float(val))
            traces.append({
                'type': 'treemap',
                'labels': labels,
                'parents': parents,
                'values': values,
                'marker': {'colors': COLORS},
            })

        else:
            traces.append({
                'type': 'bar',
                'x': safe_list(df[x_col]),
                'y': safe_list(df[y_col]) if y_col else None,
                'marker': {'color': COLORS[0]},
            })

        apply_axis_titles(layout, chart_type, x_col, y_col, color_col, agg)
        apply_axis_style_defaults(layout)

        return jsonify({'chart': {'data': traces, 'layout': layout}})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

# ─── Routes: Save / Load Dashboard ─────────────────────────────────────────

@app.route('/api/dashboard/save', methods=['POST'])
@login_required
def save_dashboard():
    data = request.json
    name = data.get('name', 'Untitled Dashboard')
    dataset_id = data.get('dataset_id')
    config = data.get('config', {})
    dashboard_id = data.get('dashboard_id')

    db = get_db()
    if dashboard_id:
        db.execute(
            'UPDATE dashboards SET name=?, config_json=?, updated_at=CURRENT_TIMESTAMP WHERE id=? AND user_id=?',
            (name, json.dumps(config), dashboard_id, session['user_id'])
        )
    else:
        cursor = db.execute(
            'INSERT INTO dashboards (user_id, dataset_id, name, config_json) VALUES (?, ?, ?, ?)',
            (session['user_id'], dataset_id, name, json.dumps(config))
        )
        dashboard_id = cursor.lastrowid
    db.commit()
    return jsonify({'success': True, 'dashboard_id': dashboard_id})

@app.route('/api/dashboard/<int:dashboard_id>')
@login_required
def get_dashboard(dashboard_id):
    db = get_db()
    d = db.execute(
        'SELECT * FROM dashboards WHERE id = ? AND user_id = ?',
        (dashboard_id, session['user_id'])
    ).fetchone()
    if not d:
        return jsonify({'error': 'Not found'}), 404
    return jsonify({
        'id': d['id'],
        'name': d['name'],
        'dataset_id': d['dataset_id'],
        'config': json.loads(d['config_json'])
    })

@app.route('/api/dashboard/<int:dashboard_id>/delete', methods=['POST'])
@login_required
def delete_dashboard(dashboard_id):
    db = get_db()
    db.execute('DELETE FROM dashboards WHERE id = ? AND user_id = ?', (dashboard_id, session['user_id']))
    db.commit()
    return jsonify({'success': True})

@app.route('/api/dataset/<int:dataset_id>/delete', methods=['POST'])
@login_required
def delete_dataset(dataset_id):
    db = get_db()
    dataset = db.execute('SELECT * FROM datasets WHERE id = ? AND user_id = ?', (dataset_id, session['user_id'])).fetchone()
    if dataset:
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], dataset['filename'])
        if os.path.exists(filepath):
            os.remove(filepath)
        db.execute('DELETE FROM dashboards WHERE dataset_id = ?', (dataset_id,))
        db.execute('DELETE FROM datasets WHERE id = ?', (dataset_id,))
        db.commit()
    return jsonify({'success': True})

# ─── Routes: Export Dashboard as HTML ───────────────────────────────────────

@app.route('/api/dashboard/<int:dashboard_id>/export')
@login_required
def export_dashboard(dashboard_id):
    db = get_db()
    d = db.execute(
        'SELECT * FROM dashboards WHERE id = ? AND user_id = ?',
        (dashboard_id, session['user_id'])
    ).fetchone()
    if not d:
        return jsonify({'error': 'Not found'}), 404

    config = json.loads(d['config_json'])
    dataset = db.execute('SELECT * FROM datasets WHERE id = ?', (d['dataset_id'],)).fetchone()
    df = load_dataset(dataset) if dataset else None

    # Generate chart JSON data for each chart config
    charts_json = []
    if df is not None:
        for chart_cfg in config.get('charts', []):
            try:
                chart_data = generate_chart_json(df, chart_cfg)
                if chart_data:
                    charts_json.append(chart_data)
            except:
                pass

    html = render_template('export.html', dashboard_name=d['name'], charts_json=json.dumps(charts_json))
    buf = io.BytesIO(html.encode('utf-8'))
    buf.seek(0)
    return send_file(buf, mimetype='text/html', as_attachment=True,
                     download_name=f"{d['name'].replace(' ', '_')}_dashboard.html")

def generate_chart_json(df, cfg):
    """Generate Plotly.js JSON (data+layout) from a chart config dict."""
    chart_type = cfg.get('chart_type', 'bar')
    x_col = cfg.get('x')
    y_col = cfg.get('y')
    color_col = cfg.get('color')
    agg = cfg.get('aggregation', 'sum')
    title = cfg.get('title', '')

    layout = {
        'title': {'text': title or f'{chart_type.title()} Chart'},
        'paper_bgcolor': '#181a21',
        'plot_bgcolor': '#181a21',
        'font': {'color': '#c8cad0', 'size': 12},
        'margin': {'l': 75, 'r': 30, 't': 60, 'b': 80},
        'xaxis': {'gridcolor': '#2a2d38'},
        'yaxis': {'gridcolor': '#2a2d38'},
        'colorway': COLORS,
    }
    traces = []

    if chart_type in ('bar', 'line', 'scatter', 'area'):
        work_df = df
        if agg and y_col and chart_type == 'bar':
            work_df = df.groupby(x_col)[y_col].agg(agg).reset_index()
        trace = {'x': safe_list(work_df[x_col]), 'y': safe_list(work_df[y_col]) if y_col else None, 'marker': {'color': COLORS[0]}}
        if chart_type == 'bar': trace['type'] = 'bar'
        elif chart_type == 'line': trace['type'] = 'scatter'; trace['mode'] = 'lines+markers'
        elif chart_type == 'scatter': trace['type'] = 'scatter'; trace['mode'] = 'markers'
        elif chart_type == 'area': trace['type'] = 'scatter'; trace['mode'] = 'lines'; trace['fill'] = 'tozeroy'
        traces.append(trace)
    elif chart_type == 'pie':
        work_df = df.groupby(x_col)[y_col].agg(agg).reset_index() if agg and y_col else df
        traces.append({'type': 'pie', 'labels': safe_list(work_df[x_col]), 'values': safe_list(work_df[y_col]) if y_col else None})
    elif chart_type == 'histogram':
        traces.append({'type': 'histogram', 'x': safe_list(df[x_col]), 'marker': {'color': COLORS[0]}})
    elif chart_type == 'box':
        traces.append({'type': 'box', 'x': safe_list(df[x_col]), 'y': safe_list(df[y_col]) if y_col else None, 'marker': {'color': COLORS[0]}})
    elif chart_type == 'heatmap':
        numeric_cols = df.select_dtypes(include='number').columns.tolist()
        corr = df[numeric_cols].corr()
        traces.append({'type': 'heatmap', 'z': corr.values.tolist(), 'x': list(corr.columns), 'y': list(corr.columns), 'colorscale': 'RdBu', 'zmid': 0})
    else:
        traces.append({'type': 'bar', 'x': safe_list(df[x_col]), 'y': safe_list(df[y_col]) if y_col else None, 'marker': {'color': COLORS[0]}})

    apply_axis_titles(layout, chart_type, x_col, y_col, color_col, agg)
    apply_axis_style_defaults(layout)

    return {'data': traces, 'layout': layout}

# ─── Routes: Gemini AI Chat ────────────────────────────────────────────────

@app.route('/api/chat', methods=['POST'])
@login_required
def chat_with_ai():
    data = request.json
    dataset_id = data.get('dataset_id')
    user_message = data.get('message', '')
    current_charts = data.get('charts', [])
    api_key = app.config.get('GEMINI_API_KEY', '')

    if not api_key:
        return jsonify({'error': 'Gemini API key is not configured on the server.'}), 400

    db = get_db()
    dataset = db.execute(
        'SELECT * FROM datasets WHERE id = ? AND user_id = ?',
        (dataset_id, session['user_id'])
    ).fetchone()
    if not dataset:
        return jsonify({'error': 'Dataset not found'}), 404

    df = load_dataset(dataset)
    if df is None:
        return jsonify({'error': 'Could not load dataset'}), 500

    # Build context about the dataset
    columns_info = json.loads(dataset['columns_json'])
    sample = df.head(5).to_string()
    stats_lines = []
    for col_info in columns_info:
        col = col_info['name']
        if col_info['type'] == 'numeric':
            stats_lines.append(f"  {col}: min={df[col].min()}, max={df[col].max()}, mean={df[col].mean():.2f}, nulls={df[col].isnull().sum()}")
        else:
            stats_lines.append(f"  {col}: {df[col].nunique()} unique values, top='{df[col].mode().iloc[0] if len(df[col].mode()) > 0 else 'N/A'}', nulls={df[col].isnull().sum()}")
    stats_text = '\n'.join(stats_lines)

    chart_lines = []
    if isinstance(current_charts, list):
        for idx, chart in enumerate(current_charts, start=1):
            if not isinstance(chart, dict):
                continue
            ctype = chart.get('chart_type', 'unknown')
            x_axis = chart.get('x') or 'N/A'
            y_axis = chart.get('y') or 'N/A'
            color = chart.get('color') or 'None'
            agg = chart.get('aggregation') or 'N/A'
            ctitle = chart.get('title') or 'Untitled'
            chart_lines.append(
                f"  {idx}. type={ctype}, title={ctitle}, x={x_axis}, y={y_axis}, color={color}, aggregation={agg}"
            )
    chart_context = '\n'.join(chart_lines) if chart_lines else '  No charts currently on the dashboard.'

    system_prompt = f"""You are a friendly data assistant speaking to non-technical users. The user has a dataset called '{dataset['original_name']}' with {dataset['row_count']} rows and {dataset['col_count']} columns.

Columns and statistics:
{stats_text}

Sample data (first 5 rows):
{sample}

Current dashboard charts:
{chart_context}

Answer questions about this dataset clearly, simply, and concisely.
Do not use technical jargon unless absolutely necessary.
If you must use a technical term, explain it in plain English immediately.
Speak as if you are helping a beginner understand their data.
Focus on practical meaning, simple patterns, and what the result suggests in everyday language.
Avoid sounding academic, statistical, or overly formal.

If the user asks for a chart suggestion, respond with a JSON block like:
{{"chart_suggestion": {{"chart_type": "bar", "x": "column_name", "y": "column_name", "color": "optional_column", "title": "Chart Title"}}}}

When giving suggestions, take into account the existing dashboard charts to avoid duplicates and improve variety.

Provide insights, patterns, and actionable analysis."""

    # Save user message
    db.execute(
        'INSERT INTO chat_history (user_id, dataset_id, role, message) VALUES (?, ?, ?, ?)',
        (session['user_id'], dataset_id, 'user', user_message)
    )
    db.commit()

    # Get chat history for context
    history = db.execute(
        'SELECT role, message FROM chat_history WHERE user_id = ? AND dataset_id = ? ORDER BY created_at DESC LIMIT 10',
        (session['user_id'], dataset_id)
    ).fetchall()
    history = list(reversed(history))

    try:
        import urllib.request
        import urllib.error

        contents = []
        for h in history:
            role = 'user' if h['role'] == 'user' else 'model'
            contents.append({'role': role, 'parts': [{'text': h['message']}]})

        payload = json.dumps({
            'system_instruction': {'parts': [{'text': system_prompt}]},
            'contents': contents,
            'generationConfig': {
                'temperature': 0.7,
                'maxOutputTokens': 2048
            }
        }).encode('utf-8')

        url = f'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}'
        req = urllib.request.Request(url, data=payload, headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode('utf-8'))

        ai_text = result['candidates'][0]['content']['parts'][0]['text']

        # Save AI response
        db.execute(
            'INSERT INTO chat_history (user_id, dataset_id, role, message) VALUES (?, ?, ?, ?)',
            (session['user_id'], dataset_id, 'assistant', ai_text)
        )
        db.commit()

        # Check for chart suggestion in response
        chart_suggestion = None
        if '"chart_suggestion"' in ai_text:
            try:
                import re
                json_match = re.search(r'\{[^{}]*"chart_suggestion"[^{}]*\{[^{}]*\}[^{}]*\}', ai_text)
                if json_match:
                    chart_suggestion = json.loads(json_match.group())['chart_suggestion']
            except:
                pass

        return jsonify({
            'response': ai_text,
            'chart_suggestion': chart_suggestion
        })
    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8') if e.fp else str(e)
        return jsonify({'error': f'Gemini API error: {error_body}'}), 500
    except Exception as e:
        return jsonify({'error': f'Error: {str(e)}'}), 500

@app.route('/api/chat/history/<int:dataset_id>')
@login_required
def chat_history(dataset_id):
    db = get_db()
    history = db.execute(
        'SELECT role, message, created_at FROM chat_history WHERE user_id = ? AND dataset_id = ? ORDER BY created_at',
        (session['user_id'], dataset_id)
    ).fetchall()
    return jsonify([{'role': h['role'], 'message': h['message'], 'time': h['created_at']} for h in history])

@app.route('/api/chat/clear/<int:dataset_id>', methods=['POST'])
@login_required
def clear_chat(dataset_id):
    db = get_db()
    db.execute('DELETE FROM chat_history WHERE user_id = ? AND dataset_id = ?', (session['user_id'], dataset_id))
    db.commit()
    return jsonify({'success': True})

# ─── Builder page ──────────────────────────────────────────────────────────

@app.route('/builder')
@login_required
def builder():
    dataset_id = request.args.get('dataset_id')
    dashboard_id = request.args.get('dashboard_id')
    db = get_db()
    datasets = db.execute(
        'SELECT * FROM datasets WHERE user_id = ? ORDER BY uploaded_at DESC',
        (session['user_id'],)
    ).fetchall()
    return render_template('builder.html',
                           datasets=datasets,
                           selected_dataset_id=dataset_id,
                           selected_dashboard_id=dashboard_id)

# ─── Settings page ─────────────────────────────────────────────────────────

@app.route('/settings')
@login_required
def settings():
    return render_template('settings.html', gemini_configured=bool(app.config.get('GEMINI_API_KEY')))

if __name__ == '__main__':
    app.run(debug=True, port=5000)
