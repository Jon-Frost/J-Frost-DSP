
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
from xml.sax.saxutils import escape

# APP CONFIGURATION — FLASK INSTANCE, PATHS, UPLOAD LIMITS, AND ENV VARS

BASE_DIR = os.path.dirname(__file__)
load_dotenv(os.path.join(BASE_DIR, '.env'), override=True)

app = Flask(__name__)
app.secret_key = secrets.token_hex(32)
app.config['UPLOAD_FOLDER'] = os.path.join(BASE_DIR, 'static', 'uploads')
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB UPLOAD SIZE LIMIT
app.config['GEMINI_API_KEY'] = os.getenv('GEMINI_API_KEY', '').strip().strip('"').strip("'")
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls', 'json', 'tsv'}

DATABASE = os.path.join(BASE_DIR, 'dashboard.db')

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# DATABASE — SQLITE CONNECTION MANAGEMENT AND SCHEMA INITIALISATION

def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
    return g.db

@app.teardown_appcontext
def close_db(exception):
    # AUTOMATICALLY CLOSE THE DB CONNECTION WHEN THE REQUEST ENDS
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

# CREATE TABLES ON STARTUP IF THEY DO NOT ALREADY EXIST
init_db()

# PASSWORD HELPERS — HASH AND VERIFY USER PASSWORDS USING WERKZEUG

def hash_password(password):
    return generate_password_hash(password)

def check_password(password, password_hash):
    return check_password_hash(password_hash, password)

# AUTH DECORATOR AND FILE VALIDATION — PROTECT ROUTES AND CHECK UPLOADS


def login_required(f):
    # DECORATOR THAT REDIRECTS UNAUTHENTICATED USERS TO THE LOGIN PAGE
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please log in first.', 'warning')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated

def allowed_file(filename):
    # RETURN TRUE IF THE FILE EXTENSION IS IN THE ALLOWED SET
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# COLUMN INFERENCE — DETECT NUMERIC, DATETIME, AND CATEGORICAL COLUMNS

def infer_columns_info(df):
    # ITERATE EVERY COLUMN AND CLASSIFY IT AS NUMERIC, DATETIME, OR CATEGORICAL
    columns_info = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        if 'int' in dtype or 'float' in dtype:
            col_type = 'numeric'
        elif 'datetime' in dtype:
            col_type = 'datetime'
        elif dtype == 'object':
            # TRY PARSING A SAMPLE OF OBJECT COLUMNS TO DETECT DATE-LIKE STRINGS
            sample = df[col].dropna().head(20)
            if len(sample) > 0:
                try:
                    pd.to_datetime(sample)
                    col_type = 'datetime'
                    # CONVERT THE ENTIRE COLUMN TO PROPER DATETIME DTYPE IN-PLACE
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    dtype = str(df[col].dtype)
                except (ValueError, TypeError):
                    col_type = 'categorical'
            else:
                col_type = 'categorical'
        else:
            col_type = 'categorical'
        columns_info.append({'name': col, 'type': col_type, 'dtype': dtype})
    return columns_info

# DATASET HIGHLIGHTS — AUTO-GENERATE KEY INSIGHT BULLETS FOR THE SIDEBAR

def build_dataset_highlights(df, columns):
    # BUILD A LIST OF UP TO 4 HUMAN-READABLE HIGHLIGHT STRINGS ABOUT THE DATA
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

# DATASET LOADER — READ A FILE FROM DISK AND AUTO-CONVERT DATE COLUMNS

def load_dataset(dataset_row):
    # LOAD THE CORRECT FILE FORMAT BASED ON THE STORED FILE_TYPE
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], dataset_row['filename'])
    ft = dataset_row['file_type']
    if ft == 'csv':
        df = pd.read_csv(filepath)
    elif ft in ('xlsx', 'xls'):
        df = pd.read_excel(filepath)
    elif ft == 'json':
        df = pd.read_json(filepath)
    elif ft == 'tsv':
        df = pd.read_csv(filepath, sep='\t')
    else:
        return None
    # AUTO-CONVERT DATE-LIKE STRING COLUMNS TO PROPER DATETIME OBJECTS
    for col in df.columns:
        if df[col].dtype == 'object':
            sample = df[col].dropna().head(20)
            if len(sample) > 0:
                try:
                    pd.to_datetime(sample)
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                except (ValueError, TypeError):
                    pass
    return df

# ROUTES: AUTHENTICATION — LOGIN, REGISTER, LOGOUT, AND ROOT REDIRECT

@app.route('/')
def index():
    # REDIRECT LOGGED-IN USERS TO DASHBOARD, OTHERS TO LOGIN
    if 'user_id' in session:
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    # VALIDATE CREDENTIALS ON POST; SET SESSION AND REDIRECT ON SUCCESS
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
    # VALIDATE FORM FIELDS, HASH PASSWORD, AND INSERT NEW USER ROW
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
    # CLEAR THE SESSION AND SEND THE USER BACK TO THE LOGIN PAGE
    session.clear()
    return redirect(url_for('login'))

# ROUTES: DASHBOARD — MAIN LANDING PAGE WITH DATASETS AND SAVED DASHBOARDS

@app.route('/dashboard')
@login_required
def dashboard():
    # FETCH ALL USER DATASETS AND DASHBOARDS, THEN RENDER THE HOME PAGE
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

# Routes: File Upload

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

# Routes: Dataset API

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
    # RE-INFER COLUMN TYPES FROM THE LIVE DATAFRAME (HANDLES DATE AUTO-DETECTION)
    columns = infer_columns_info(df)
    # CONVERT DATETIME COLUMNS TO ISO STRINGS FOR SAFE JSON SERIALIZATION
    preview_df = df.head(50).copy()
    for col_info in columns:
        if col_info['type'] == 'datetime' and col_info['name'] in preview_df.columns:
            preview_df[col_info['name']] = preview_df[col_info['name']].dt.strftime('%Y-%m-%d').fillna('')
    preview = preview_df.to_dict(orient='records')
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
    # RETURN THE STORED COLUMN METADATA JSON FOR A GIVEN DATASET
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
    # FILL MISSING NUMERIC VALUES WITH COLUMN MEANS AND SAVE AS A NEW DATASET
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

# Routes: Chart Generation ──────────────────────────────────────────────

COLORS = ['#5b6ef5','#8b5cf6','#34d399','#fbbf24','#f87171','#38bdf8','#fb923c','#a78bfa','#4ade80','#f472b6']

# Forecasting Helper 

def generate_forecast(df, x_col, y_col, periods=30):
    """
    RETURNS (FORECAST_DATES, PREDICTED, CONF_LOWER, CONF_UPPER) OR RAISES
    A VALUEERROR WITH A USER-FRIENDLY MESSAGE ON FAILURE.
    """
    work = df[[x_col, y_col]].copy()
    work[x_col] = pd.to_datetime(work[x_col], errors='coerce')
    work = work.dropna(subset=[x_col, y_col])

    # ENFORCE MINIMUM DATA POINTS FOR A MEANINGFUL FORECAST
    if len(work) < 10:
        raise ValueError(
            'Not enough historical data to generate a forecast. '
            'At least 10 data points are required.'
        )

    work = work.sort_values(x_col)
    work = work.set_index(x_col)

    # RESAMPLE TO A UNIFORM FREQUENCY AND FILL GAPS (HANDLES WEEKENDS / MISSING DAYS)
    freq = pd.infer_freq(work.index)
    if freq is None:
        freq = 'D'
    work = work.resample(freq).sum()
    work = work.ffill().fillna(0)

    series = work[y_col].astype(float)

    predicted = None
    conf_lower = None
    conf_upper = None

    # PRIMARY MODEL: HOLT-WINTERS EXPONENTIAL SMOOTHING (CAPTURES TREND WELL)
    from statsmodels.tsa.holtwinters import ExponentialSmoothing
    try:
        hw_model = ExponentialSmoothing(
            series,
            trend='add',          
            seasonal=None,        
            damped_trend=True,    
        ).fit(optimized=True)

        predicted = hw_model.forecast(periods)

        # BUILD APPROXIMATE 95% CONFIDENCE INTERVAL FROM RESIDUAL STD
        residuals = series - hw_model.fittedvalues
        residual_std = float(residuals.std())
        conf_lower = predicted - 1.96 * residual_std
        conf_upper = predicted + 1.96 * residual_std
    except Exception:
        pass

    # FALLBACK MODEL: ARIMA IF HOLT-WINTERS FAILED TO FIT
    if predicted is None:
        from statsmodels.tsa.arima.model import ARIMA
        try:
            model = ARIMA(series, order=(2, 1, 2))
            fitted = model.fit()
        except Exception:
            try:
                model = ARIMA(series, order=(1, 1, 1))
                fitted = model.fit()
            except Exception as inner:
                raise ValueError(
                    f'Unable to fit forecasting model: {str(inner)}'
                )
        forecast_result = fitted.get_forecast(steps=periods)
        predicted = forecast_result.predicted_mean
        ci = forecast_result.conf_int(alpha=0.05)
        conf_lower = ci.iloc[:, 0]
        conf_upper = ci.iloc[:, 1]

    # CONVERT INDEX AND VALUES TO JSON-SAFE LISTS
    forecast_dates = [d.isoformat() for d in predicted.index]
    predicted_vals = [None if pd.isna(v) else float(v) for v in predicted]
    lower_vals = [None if pd.isna(v) else float(v) for v in conf_lower]
    upper_vals = [None if pd.isna(v) else float(v) for v in conf_upper]

    return forecast_dates, predicted_vals, lower_vals, upper_vals


def safe_list(series):
    
    return [None if pd.isna(v) else (int(v) if isinstance(v, (np.integer,)) else (float(v) if isinstance(v, (np.floating,)) else v)) for v in series]

def axis_value_label(column_name, agg=None, default='Value'):
    
    if agg == 'count':
        return 'Count'
    if column_name and agg:
        return f'{agg.title()} of {column_name}'
    return column_name or default

def apply_axis_titles(layout, chart_type, x_col=None, y_col=None, color_col=None, agg=None):
    
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

# Routes: Chart API — Build Plotly Traces + Layout from User Selections

@app.route('/api/chart', methods=['POST'])
@login_required
def generate_chart():
    # EXTRACT CHART PARAMETERS FROM THE JSON REQUEST BODY
    data = request.json
    dataset_id = data.get('dataset_id')
    chart_type = data.get('chart_type', 'bar')
    x_col = data.get('x')
    y_col = data.get('y')
    color_col = data.get('color')
    agg = data.get('aggregation', 'sum')
    title = data.get('title', '')
    forecast_enabled = data.get('forecast', False)
    forecast_periods = int(data.get('forecast_periods', 30))

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
        # DEFAULT DARK-THEMED PLOTLY LAYOUT SHARED BY ALL CHART TYPES
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

        # BAR / LINE / SCATTER / AREA — CARTESIAN TRACES WITH OPTIONAL COLOUR GROUPING
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

        # PIE — AGGREGATE SLICES BY CATEGORY
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

        # HISTOGRAM — FREQUENCY DISTRIBUTION OF A SINGLE COLUMN
        elif chart_type == 'histogram':
            traces.append({
                'type': 'histogram',
                'x': safe_list(df[x_col]),
                'marker': {'color': COLORS[0]},
            })

        # BOX PLOT — STATISTICAL DISTRIBUTION WITH OPTIONAL COLOUR GROUPING
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

        # VIOLIN — DENSITY-BASED DISTRIBUTION WITH OPTIONAL COLOUR GROUPING
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

        # HEATMAP — CORRELATION MATRIX OF ALL NUMERIC COLUMNS
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

        # SUNBURST — HIERARCHICAL BREAKDOWN OF Y GROUPED BY X
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

        # TREEMAP — HIERARCHICAL BREAKDOWN OF Y GROUPED BY X (RECTANGULAR)
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

        # DEFAULT FALLBACK — RENDER AS BAR IF CHART TYPE IS UNRECOGNISED
        else:
            traces.append({
                'type': 'bar',
                'x': safe_list(df[x_col]),
                'y': safe_list(df[y_col]) if y_col else None,
                'marker': {'color': COLORS[0]},
            })

        # APPLY AXIS TITLES AND CONSISTENT STYLING TO ALL CHART TYPES
        apply_axis_titles(layout, chart_type, x_col, y_col, color_col, agg)
        apply_axis_style_defaults(layout)

        
        # FORECASTING — APPEND PREDICTION TRACES IF ENABLED ON LINE/AREA
        
        forecast_meta = None
        if forecast_enabled and chart_type in ('line', 'area'):
            try:
                # BUILD THE SAME AGGREGATED SERIES THE CHART USES
                if agg and y_col:
                    agg_df = df.groupby(x_col)[y_col].agg(agg).reset_index()
                else:
                    agg_df = df[[x_col, y_col]].copy()

                dates, predicted, lower, upper = generate_forecast(
                    agg_df, x_col, y_col, periods=forecast_periods
                )

                # FORECAST LINE (DASHED GOLD TRACE)
                traces.append({
                    'type': 'scatter',
                    'mode': 'lines',
                    'x': dates,
                    'y': predicted,
                    'name': 'Forecast',
                    'line': {'color': '#fbbf24', 'dash': 'dot', 'width': 2},
                    'showlegend': True,
                })

                # CONFIDENCE-INTERVAL UPPER BOUND (INVISIBLE LINE)
                traces.append({
                    'type': 'scatter',
                    'mode': 'lines',
                    'x': dates,
                    'y': upper,
                    'name': 'Upper CI',
                    'line': {'width': 0},
                    'showlegend': False,
                })

                # CONFIDENCE-INTERVAL LOWER BOUND (SHADED FILL BETWEEN BOUNDS)
                traces.append({
                    'type': 'scatter',
                    'mode': 'lines',
                    'x': dates,
                    'y': lower,
                    'name': 'Lower CI',
                    'line': {'width': 0},
                    'fill': 'tonexty',
                    'fillcolor': 'rgba(251, 191, 36, 0.15)',
                    'showlegend': False,
                })

                forecast_meta = {
                    'periods': forecast_periods,
                    'message': f'Forecast generated for the next {forecast_periods} periods.'
                }

            except ValueError as ve:
                forecast_meta = {'error': str(ve)}
            except Exception as fe:
                forecast_meta = {'error': f'Forecasting failed: {str(fe)}'}

        # ASSEMBLE AND RETURN THE FINAL JSON RESPONSE
        response = {'chart': {'data': traces, 'layout': layout}}
        if forecast_meta is not None:
            response['forecast'] = forecast_meta
        return jsonify(response)
    except Exception as e:
        return jsonify({'error': str(e)}), 400

# Routes: Save / Load Dashboard — Persist and Retrieve Dashboard Configurations

@app.route('/api/dashboard/save', methods=['POST'])
@login_required
def save_dashboard():
    # INSERT A NEW DASHBOARD OR UPDATE AN EXISTING ONE BY ID
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
    # FETCH A SINGLE DASHBOARD BY ID AND RETURN ITS CONFIG JSON
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
    # PERMANENTLY DELETE A DASHBOARD RECORD
    db = get_db()
    db.execute('DELETE FROM dashboards WHERE id = ? AND user_id = ?', (dashboard_id, session['user_id']))
    db.commit()
    return jsonify({'success': True})

@app.route('/api/dataset/<int:dataset_id>/delete', methods=['POST'])
@login_required
def delete_dataset(dataset_id):
    # DELETE THE DATASET FILE FROM DISK, REMOVE LINKED DASHBOARDS, AND DELETE THE DB RECORD
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

# Routes: Export Dashboard as HTML

@app.route('/api/dashboard/<int:dashboard_id>/export')
@login_required
def export_dashboard(dashboard_id):
    # BUILD A DOWNLOADABLE PDF VERSION OF THE SAVED DASHBOARD
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

    # GENERATE CHART JSON DATA FOR EACH CHART CONFIG
    charts_json = []
    if df is not None:
        for chart_cfg in config.get('charts', []):
            try:
                # RE-CREATE PLOTLY JSON FOR EACH SAVED CHART CONFIG
                chart_data = generate_chart_json(df, chart_cfg)
                if chart_data:
                    charts_json.append(chart_data)
            except:
                pass

    pdf_bytes = generate_dashboard_pdf_bytes(d['name'], charts_json, config.get('charts', []))
    buf = io.BytesIO(pdf_bytes)
    buf.seek(0)
    return send_file(
        buf,
        mimetype='application/pdf',
        as_attachment=True,
        download_name=f"{d['name'].replace(' ', '_')}_dashboard.pdf"
    )

# Routes: Chart JSON Helper — Generate Plotly Data+Layout for Export / Reuse

def generate_chart_json(df, cfg):
    
    chart_type = cfg.get('chart_type', 'bar')
    x_col = cfg.get('x')
    y_col = cfg.get('y')
    color_col = cfg.get('color')
    agg = cfg.get('aggregation', 'sum')
    title = cfg.get('title', '')

    # DARK-THEMED EXPORT LAYOUT WITH MATCHING COLOUR PALETTE
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


# PDF EXPORT HELPERS — BUILD A DASHBOARD PDF USING CHART IMAGES OR A TEXT FALLBACK

def _escape_pdf_text(value):
    
    return str(value).replace('\\', '\\\\').replace('(', '\\(').replace(')', '\\)')


def build_simple_pdf_bytes(title, lines):
   
    all_lines = [title, f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}', ''] + list(lines)
    lines_per_page = 40
    pages = [all_lines[i:i + lines_per_page] for i in range(0, len(all_lines), lines_per_page)] or [[title]]

    objects = []

    def add_object(content):
        objects.append(content)
        return len(objects)

    font_obj = add_object('<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>')
    page_obj_ids = []

    for page_lines in pages:
        text_commands = ['BT', '/F1 14 Tf', '50 790 Td']
        first_line = True
        for line in page_lines:
            safe_line = _escape_pdf_text(line)
            if first_line:
                text_commands.append(f'({safe_line}) Tj')
                first_line = False
            else:
                text_commands.append('0 -18 Td')
                text_commands.append(f'({safe_line}) Tj')
        text_commands.append('ET')
        stream = '\n'.join(text_commands)
        content_obj = add_object(f'<< /Length {len(stream.encode("utf-8"))} >>\nstream\n{stream}\nendstream')
        page_obj_ids.append((content_obj, None))

    pages_kids = []
    pages_placeholder_index = add_object('')

    for idx, (content_obj, _) in enumerate(page_obj_ids):
        page_obj_id = add_object(
            f'<< /Type /Page /Parent {pages_placeholder_index} 0 R /MediaBox [0 0 612 842] '
            f'/Resources << /Font << /F1 {font_obj} 0 R >> >> /Contents {content_obj} 0 R >>'
        )
        page_obj_ids[idx] = (content_obj, page_obj_id)
        pages_kids.append(f'{page_obj_id} 0 R')

    objects[pages_placeholder_index - 1] = (
        f'<< /Type /Pages /Kids [{" ".join(pages_kids)}] /Count {len(pages_kids)} >>'
    )
    catalog_obj = add_object(f'<< /Type /Catalog /Pages {pages_placeholder_index} 0 R >>')

    pdf = io.BytesIO()
    pdf.write(b'%PDF-1.4\n')
    offsets = [0]
    for index, obj in enumerate(objects, start=1):
        offsets.append(pdf.tell())
        pdf.write(f'{index} 0 obj\n'.encode('utf-8'))
        pdf.write(obj.encode('utf-8'))
        pdf.write(b'\nendobj\n')

    xref_offset = pdf.tell()
    pdf.write(f'xref\n0 {len(objects) + 1}\n'.encode('utf-8'))
    pdf.write(b'0000000000 65535 f \n')
    for offset in offsets[1:]:
        pdf.write(f'{offset:010d} 00000 n \n'.encode('utf-8'))

    trailer = (
        f'trailer\n<< /Size {len(objects) + 1} /Root {catalog_obj} 0 R >>\n'
        f'startxref\n{xref_offset}\n%%EOF'
    )
    pdf.write(trailer.encode('utf-8'))
    return pdf.getvalue()


def generate_dashboard_pdf_bytes(dashboard_name, charts_json, chart_configs):
    
    fallback_lines = []
    for index, chart_cfg in enumerate(chart_configs, start=1):
        chart_title = chart_cfg.get('title') or f"{chart_cfg.get('chart_type', 'chart').title()} Chart"
        fallback_lines.append(f'{index}. {chart_title}')

    if not charts_json:
        fallback_lines.append('No charts were available to export for this dashboard.')

    try:
        import plotly.graph_objects as go
        from reportlab.lib.pagesizes import landscape, A4
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib.units import inch
        from reportlab.platypus import Image, PageBreak, Paragraph, SimpleDocTemplate, Spacer

        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=landscape(A4),
            leftMargin=36,
            rightMargin=36,
            topMargin=36,
            bottomMargin=36,
        )
        styles = getSampleStyleSheet()
        story = [
            Paragraph(escape(dashboard_name), styles['Title']),
            Spacer(1, 0.2 * inch),
            Paragraph(f'Generated on {escape(datetime.now().strftime("%Y-%m-%d %H:%M"))}', styles['Normal']),
            Spacer(1, 0.3 * inch),
        ]

        if not charts_json:
            story.append(Paragraph('No charts were available to export.', styles['Normal']))

        for index, chart_json in enumerate(charts_json, start=1):
            chart_cfg = chart_configs[index - 1] if index - 1 < len(chart_configs) else {}
            chart_title = chart_cfg.get('title') or f"{chart_cfg.get('chart_type', 'chart').title()} Chart"
            story.append(Paragraph(f'{index}. {escape(chart_title)}', styles['Heading2']))
            story.append(Spacer(1, 0.15 * inch))

            fig = go.Figure(data=chart_json.get('data', []), layout=chart_json.get('layout', {}))
            fig.update_layout(width=1200, height=700)
            image_bytes = fig.to_image(format='png', width=1200, height=700, scale=2)

            story.append(Image(io.BytesIO(image_bytes), width=10.5 * inch, height=6.1 * inch))
            if index < len(charts_json):
                story.append(PageBreak())

        doc.build(story)
        return buffer.getvalue()
    except Exception:
        return build_simple_pdf_bytes(dashboard_name, fallback_lines)

# Routes: Gemini AI Chat — AI-Powered Data Analysis and Chart Suggestions

@app.route('/api/chat', methods=['POST'])
@login_required
def chat_with_ai():
    # PARSE THE USER MESSAGE, DATASET ID, AND CURRENT CHART CONTEXT
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

    # BUILD CONTEXT ABOUT THE DATASET (COLUMN STATS AND SAMPLE ROWS FOR THE PROMPT)
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

    # SAVE USER MESSAGE TO CHAT HISTORY TABLE
    db.execute(
        'INSERT INTO chat_history (user_id, dataset_id, role, message) VALUES (?, ?, ?, ?)',
        (session['user_id'], dataset_id, 'user', user_message)
    )
    db.commit()

    # RETRIEVE LAST 10 MESSAGES FOR CONVERSATIONAL CONTEXT
    history = db.execute(
        'SELECT role, message FROM chat_history WHERE user_id = ? AND dataset_id = ? ORDER BY created_at DESC LIMIT 10',
        (session['user_id'], dataset_id)
    ).fetchall()
    history = list(reversed(history))

    try:
        import urllib.request
        import urllib.error

        # CONVERT CHAT HISTORY INTO GEMINI API CONTENT FORMAT
        contents = []
        for h in history:
            role = 'user' if h['role'] == 'user' else 'model'
            contents.append({'role': role, 'parts': [{'text': h['message']}]})

        # SEND REQUEST TO GEMINI 2.0 FLASH VIA REST API
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

        # PERSIST THE AI RESPONSE IN CHAT HISTORY
        db.execute(
            'INSERT INTO chat_history (user_id, dataset_id, role, message) VALUES (?, ?, ?, ?)',
            (session['user_id'], dataset_id, 'assistant', ai_text)
        )
        db.commit()

        # DETECT JSON CHART SUGGESTIONS EMBEDDED IN THE AI RESPONSE
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

# Routes: Chat History — Retrieve and Clear Conversation Logs

@app.route('/api/chat/history/<int:dataset_id>')
@login_required
def chat_history(dataset_id):
    # RETURN ALL CHAT MESSAGES FOR A GIVEN DATASET, ORDERED CHRONOLOGICALLY
    db = get_db()
    history = db.execute(
        'SELECT role, message, created_at FROM chat_history WHERE user_id = ? AND dataset_id = ? ORDER BY created_at',
        (session['user_id'], dataset_id)
    ).fetchall()
    return jsonify([{'role': h['role'], 'message': h['message'], 'time': h['created_at']} for h in history])

@app.route('/api/chat/clear/<int:dataset_id>', methods=['POST'])
@login_required
def clear_chat(dataset_id):
    # DELETE ALL CHAT HISTORY FOR THIS USER + DATASET COMBINATION
    db = get_db()
    db.execute('DELETE FROM chat_history WHERE user_id = ? AND dataset_id = ?', (session['user_id'], dataset_id))
    db.commit()
    return jsonify({'success': True})

# Builder page

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

# Settings page

@app.route('/settings')
@login_required
def settings():
    return render_template('settings.html', gemini_configured=bool(app.config.get('GEMINI_API_KEY')))

if __name__ == '__main__':
    app.run(debug=True, port=5000)
