# ══════════════════════════════════════════════════════════════════════════════
# IMPORTS — STANDARD LIBRARY, PANDAS, AND APPLICATION MODULE
# ══════════════════════════════════════════════════════════════════════════════

import io
import json
import os
import shutil
import sqlite3
import tempfile
import unittest

import pandas as pd

import app as app_module



# TEST CLASS — UNIT TESTS FOR THE DATAFORGE APPLICATION



class DataForgeAppTests(unittest.TestCase):

    
    # SETUP AND TEARDOWN — CREATE TEMP DIR, DB, AND TEST CLIENT BEFORE EACH TEST
    

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="dataforge_tests_")
        self.upload_dir = os.path.join(self.tmpdir, "uploads")
        os.makedirs(self.upload_dir, exist_ok=True)
        self.db_path = os.path.join(self.tmpdir, "test_dashboard.db")

        app_module.DATABASE = self.db_path
        app_module.app.config.update(
            TESTING=True,
            SECRET_KEY="test-secret-key",
            UPLOAD_FOLDER=self.upload_dir,
            GEMINI_API_KEY="",
        )

        with app_module.app.app_context():
            app_module.init_db()

        self.client = app_module.app.test_client()
        self.user_id = self._create_user("alice", "alice@example.com", "password123")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    # HELPER METHODS — CONNECT TO DB, CREATE USERS, LOGIN, AND SEED DATASETS

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _create_user(self, username, email, password):
        conn = self._connect()
        cur = conn.execute(
            "INSERT INTO users (username, email, password_hash) VALUES (?, ?, ?)",
            (username, email, app_module.hash_password(password)),
        )
        conn.commit()
        user_id = cur.lastrowid
        conn.close()
        return user_id

    def _login_session(self, user_id=None, username="alice"):
        # SET SESSION USER_ID SO PROTECTED ENDPOINTS ALLOW ACCESS
        with self.client.session_transaction() as sess:
            sess["user_id"] = user_id or self.user_id
            sess["username"] = username

    def _create_dataset_record(self, df, original_name="sample.csv", ext="csv"):
        # WRITE A DATAFRAME TO DISK AND INSERT A MATCHING DB ROW FOR TESTING
        filename = f"dataset_{original_name}"
        filepath = os.path.join(self.upload_dir, filename)

        if ext == "csv":
            df.to_csv(filepath, index=False)
        elif ext == "json":
            df.to_json(filepath, orient="records")
        elif ext == "tsv":
            df.to_csv(filepath, sep="\t", index=False)
        else:
            raise ValueError("Unsupported test extension")

        columns_info = app_module.infer_columns_info(df)

        conn = self._connect()
        cur = conn.execute(
            """
            INSERT INTO datasets (user_id, filename, original_name, file_type, row_count, col_count, columns_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self.user_id,
                filename,
                original_name,
                ext,
                len(df),
                len(df.columns),
                json.dumps(columns_info),
            ),
        )
        conn.commit()
        dataset_id = cur.lastrowid
        conn.close()
        return dataset_id

    # TEST: HELPERS — PASSWORD HASHING, FILE VALIDATION, INFERENCE, SAFE_LIST

    def test_helpers_basic_behavior(self):
        password_hash = app_module.hash_password("abc123")
        self.assertTrue(app_module.check_password("abc123", password_hash))
        self.assertFalse(app_module.check_password("wrong-password", password_hash))
        self.assertTrue(app_module.allowed_file("data.csv"))
        self.assertFalse(app_module.allowed_file("data.exe"))

        df = pd.DataFrame(
            {
                "sales": [100, 200, None, 150],
                "region": ["West", "East", "West", "West"],
            }
        )
        columns = app_module.infer_columns_info(df)
        highlights = app_module.build_dataset_highlights(df, columns)
        self.assertGreaterEqual(len(highlights), 2)

        series_json = app_module.safe_list(pd.Series([1, 2.5, None]))
        self.assertEqual(series_json, [1, 2.5, None])
        self.assertEqual(app_module.axis_value_label("sales", "sum"), "Sum of sales")
        self.assertEqual(app_module.axis_value_label(None, "count"), "Count")

    # ROUTE PROTECTION — UNAUTHENTICATED USERS ARE REDIRECTED TO LOGIN
    

    def test_index_and_protected_redirects(self):
        res = self.client.get("/")
        self.assertEqual(res.status_code, 302)
        self.assertIn("/login", res.location)

        res = self.client.get("/dashboard")
        self.assertEqual(res.status_code, 302)
        self.assertIn("/login", res.location)

    # AUTH FLOW — REGISTER, LOGIN, AND LOGOUT ROUND-TRIP

    def test_register_login_logout_flow(self):
        res = self.client.post(
            "/register",
            data={
                "username": "bob",
                "email": "bob@example.com",
                "password": "password123",
                "confirm_password": "password123",
            },
            follow_redirects=False,
        )
        self.assertEqual(res.status_code, 302)
        self.assertIn("/login", res.location)

        res = self.client.post(
            "/login",
            data={"username": "bob", "password": "password123"},
            follow_redirects=False,
        )
        self.assertEqual(res.status_code, 302)
        self.assertIn("/dashboard", res.location)

        res = self.client.get("/logout", follow_redirects=False)
        self.assertEqual(res.status_code, 302)
        self.assertIn("/login", res.location)

    # UPLOAD, PREVIEW, AND COLUMNS — FILE UPLOAD END-TO-END

    def test_upload_and_preview_and_columns(self):
        self._login_session()
        payload = io.BytesIO(b"category,amount\nA,10\nB,20\nA,30\n")
        res = self.client.post(
            "/upload",
            data={"file": (payload, "sales.csv")},
            content_type="multipart/form-data",
        )
        self.assertEqual(res.status_code, 200)
        body = res.get_json()
        self.assertTrue(body["success"])

        conn = self._connect()
        dataset = conn.execute("SELECT id FROM datasets WHERE user_id = ?", (self.user_id,)).fetchone()
        conn.close()
        dataset_id = dataset["id"]

        preview_res = self.client.get(f"/api/dataset/{dataset_id}/preview")
        self.assertEqual(preview_res.status_code, 200)
        preview_body = preview_res.get_json()
        self.assertIn("columns", preview_body)
        self.assertIn("preview", preview_body)
        self.assertIn("stats", preview_body)
        self.assertIn("highlights", preview_body)

        columns_res = self.client.get(f"/api/dataset/{dataset_id}/columns")
        self.assertEqual(columns_res.status_code, 200)
        self.assertIsInstance(columns_res.get_json(), list)

    # DATA CLEANING — VERIFY CLEANED DATASET IS CREATED CORRECTLY

    def test_clean_dataset_creates_new_dataset(self):
        self._login_session()
        df = pd.DataFrame(
            {
                "amount": [10.0, None, 30.0],
                "region": ["West", "East", "West"],
            }
        )
        dataset_id = self._create_dataset_record(df, original_name="dirty.csv", ext="csv")

        res = self.client.post(f"/api/dataset/{dataset_id}/clean")
        self.assertEqual(res.status_code, 200)
        body = res.get_json()
        self.assertTrue(body["success"])
        self.assertIn("dataset_id", body)

        conn = self._connect()
        cleaned_row = conn.execute("SELECT * FROM datasets WHERE id = ?", (body["dataset_id"],)).fetchone()
        conn.close()
        self.assertIsNotNone(cleaned_row)
        self.assertTrue(os.path.exists(os.path.join(self.upload_dir, cleaned_row["filename"])))

    # CHART GENERATION — API CHART ENDPOINT AND EXPORT HELPER

    def test_chart_generation_and_chart_json(self):
        self._login_session()
        df = pd.DataFrame(
            {
                "category": ["A", "B", "A", "C"],
                "value": [10, 20, 30, 15],
                "cost": [3, 4, 9, 6],
            }
        )
        dataset_id = self._create_dataset_record(df, original_name="chart.csv", ext="csv")

        # /API/CHART IS THE PRIMARY CHART ENTRYPOINT
        res = self.client.post(
            "/api/chart",
            json={
                "dataset_id": dataset_id,
                "chart_type": "bar",
                "x": "category",
                "y": "value",
                "aggregation": "sum",
                "title": "Sales by Category",
            },
        )
        self.assertEqual(res.status_code, 200)
        body = res.get_json()
        self.assertIn("chart", body)
        self.assertIn("data", body["chart"])

        export_chart = app_module.generate_chart_json(
            df,
            {
                "chart_type": "heatmap",
                "x": "category",
                "y": "value",
                "title": "Correlation",
            },
        )
        self.assertIn("data", export_chart)
        self.assertIn("layout", export_chart)

    # DASHBOARD CRUD — SAVE, GET, EXPORT, AND DELETE

    def test_dashboard_save_get_delete_and_export(self):
        self._login_session()
        df = pd.DataFrame({"category": ["A", "B"], "value": [1, 2]})
        dataset_id = self._create_dataset_record(df, original_name="dash.csv", ext="csv")

        save_res = self.client.post(
            "/api/dashboard/save",
            json={
                "name": "Exec Dashboard",
                "dataset_id": dataset_id,
                "config": {
                    "charts": [
                        {
                            "chart_type": "bar",
                            "x": "category",
                            "y": "value",
                            "title": "Bar View",
                        }
                    ]
                },
            },
        )
        self.assertEqual(save_res.status_code, 200)
        dashboard_id = save_res.get_json()["dashboard_id"]

        get_res = self.client.get(f"/api/dashboard/{dashboard_id}")
        self.assertEqual(get_res.status_code, 200)
        self.assertEqual(get_res.get_json()["name"], "Exec Dashboard")

        export_res = self.client.get(f"/api/dashboard/{dashboard_id}/export")
        self.assertEqual(export_res.status_code, 200)
        self.assertIn("application/pdf", export_res.content_type)
        self.assertTrue(export_res.data.startswith(b"%PDF"))

        del_res = self.client.post(f"/api/dashboard/{dashboard_id}/delete")
        self.assertEqual(del_res.status_code, 200)
        self.assertTrue(del_res.get_json()["success"])

    # DATASET DELETE — VERIFY FILE AND LINKED DASHBOARDS ARE REMOVED

    def test_dataset_delete_cleans_related_dashboards(self):
        self._login_session()
        df = pd.DataFrame({"category": ["A", "B"], "value": [2, 3]})
        dataset_id = self._create_dataset_record(df, original_name="to_delete.csv", ext="csv")

        save_res = self.client.post(
            "/api/dashboard/save",
            json={
                "name": "Temp Dash",
                "dataset_id": dataset_id,
                "config": {"charts": []},
            },
        )
        self.assertEqual(save_res.status_code, 200)

        del_res = self.client.post(f"/api/dataset/{dataset_id}/delete")
        self.assertEqual(del_res.status_code, 200)
        self.assertTrue(del_res.get_json()["success"])

        conn = self._connect()
        ds = conn.execute("SELECT * FROM datasets WHERE id = ?", (dataset_id,)).fetchone()
        dashboards = conn.execute("SELECT * FROM dashboards WHERE dataset_id = ?", (dataset_id,)).fetchall()
        conn.close()
        self.assertIsNone(ds)
        self.assertEqual(len(dashboards), 0)

    # CHAT — API KEY GUARD, HISTORY RETRIEVAL, AND CLEAR

    def test_chat_history_and_clear_and_no_api_key_guard(self):
        self._login_session()
        df = pd.DataFrame({"category": ["A", "B"], "value": [5, 7]})
        dataset_id = self._create_dataset_record(df, original_name="chat.csv", ext="csv")

        res = self.client.post(
            "/api/chat",
            json={"dataset_id": dataset_id, "message": "What stands out?", "charts": []},
        )
        self.assertEqual(res.status_code, 400)
        self.assertIn("Gemini API key", res.get_json()["error"])

        conn = self._connect()
        conn.execute(
            "INSERT INTO chat_history (user_id, dataset_id, role, message) VALUES (?, ?, ?, ?)",
            (self.user_id, dataset_id, "user", "hello"),
        )
        conn.execute(
            "INSERT INTO chat_history (user_id, dataset_id, role, message) VALUES (?, ?, ?, ?)",
            (self.user_id, dataset_id, "assistant", "hi"),
        )
        conn.commit()
        conn.close()

        history_res = self.client.get(f"/api/chat/history/{dataset_id}")
        self.assertEqual(history_res.status_code, 200)
        self.assertEqual(len(history_res.get_json()), 2)

        clear_res = self.client.post(f"/api/chat/clear/{dataset_id}")
        self.assertEqual(clear_res.status_code, 200)
        self.assertTrue(clear_res.get_json()["success"])

    # BUILDER AND SETTINGS PAGES — CONFIRM PAGES RENDER SUCCESSFULLY

    def test_builder_and_settings_pages(self):
        self._login_session()
        builder_res = self.client.get("/builder")
        settings_res = self.client.get("/settings")
        self.assertEqual(builder_res.status_code, 200)
        self.assertEqual(settings_res.status_code, 200)




if __name__ == "__main__":
    unittest.main(verbosity=2)