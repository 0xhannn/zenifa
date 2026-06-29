import os
import sqlite3
import uuid
import json
import httpx
from datetime import datetime
from io import BytesIO
from pathlib import Path

from fastapi import FastAPI, Request, Form, File, UploadFile, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / '.env')
except Exception:
    pass

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

_R2_CLIENT = None
def _r2():
    global _R2_CLIENT
    if _R2_CLIENT is None:
        _R2_CLIENT = boto3.client(
            's3',
            endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            aws_access_key_id=os.environ['R2_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['R2_SECRET_ACCESS_KEY'],
            config=Config(signature_version='s3v4'),
            region_name='auto',
        )
    return _R2_CLIENT

def _upload_pdf_to_r2(file: UploadFile) -> str:
    """Stream PDF upload to R2, return public URL."""
    name = datetime.now().strftime('%Y%m%d%H%M%S') + '_' + uuid.uuid4().hex[:8] + '.pdf'
    _r2().upload_fileobj(
        file.file,
        os.environ['R2_BUCKET'],
        name,
        ExtraArgs={'ContentType': 'application/pdf', 'ContentDisposition': 'inline'},
    )
    return f"{os.environ['R2_PUBLIC_BASE'].rstrip('/')}/{name}"

def _extract_remote_pdfs(urls: list[str]) -> list[str]:
    """Filter list to R2-hosted PDF URLs only."""
    base = os.environ.get('R2_PUBLIC_BASE', '').rstrip('/')
    if not base:
        return []
    return [u for u in (urls or []) if isinstance(u, str) and u.startswith(base + '/') and u.lower().endswith('.pdf')]

def _delete_r2_objects(urls: list[str]):
    """Best-effort batch delete of R2 objects by public URL. Silently no-ops if R2 not configured."""
    if not urls:
        return
    base = os.environ.get('R2_PUBLIC_BASE', '').rstrip('/')
    bucket = os.environ.get('R2_BUCKET')
    if not base or not bucket:
        return
    objects = []
    for url in urls:
        if not url.startswith(base + '/'):
            continue
        key = url[len(base) + 1:]
        # Safety: no traversal, no empty, must be simple filename (no slashes past first segment allowed)
        if not key or '..' in key or '/' in key:
            continue
        objects.append({'Key': key})
    if not objects:
        return
    try:
        _r2().delete_objects(Bucket=bucket, Delete={'Objects': objects, 'Quiet': True})
    except Exception as e:
        print('R2 delete failed:', e)


def _delete_removed_files(old_urls: list[str], new_urls: list[str]):
    """Diff old vs new: delete anything in old but not in new from R2 (URL) or local fs."""
    new_set = set(new_urls or [])
    removed = [u for u in (old_urls or []) if u and u not in new_set]
    if not removed:
        return
    # 1. R2 URLs → batch delete
    r2_urls = [u for u in removed if u.startswith('http://') or u.startswith('https://')]
    if r2_urls:
        try:
            _delete_r2_objects(r2_urls)
        except Exception as e:
            print('R2 delete error:', e)
    # 2. Local paths (relative like uploads/tasks/xxx.webp) → unlink
    for u in removed:
        if u.startswith(('http://', 'https://')):
            continue
        # Strip leading slash, resolve to absolute
        rel = u.lstrip('/')
        if not rel or rel.startswith('static/') or '..' in rel:
            continue
        full = os.path.join(BASE_DIR, rel)
        try:
            if os.path.isfile(full):
                os.remove(full)
                print(f'Deleted local file: {full}')
        except Exception as e:
            print(f'Local delete failed for {u}: {e}')

def _compress_image(file_path: str, max_width: int = 1600, quality: int = 82):
    """In-place: resize + convert to .webp beside original. Removes original if ext differs. Returns new rel-path or None."""
    try:
        from PIL import Image
        if not os.path.exists(file_path):
            return None
        img = Image.open(file_path)
        if img.format not in ('JPEG', 'PNG', 'WEBP', 'GIF'):
            return None
        if img.width > max_width:
            ratio = max_width / img.width
            img = img.resize((max_width, int(img.height * ratio)), Image.LANCZOS)
        base, _ = os.path.splitext(file_path)
        new_path = base + '.webp'
        if img.mode in ('RGBA', 'LA', 'P'):
            img.save(new_path, 'WEBP', quality=quality, method=6)
        else:
            img.convert('RGB').save(new_path, 'WEBP', quality=quality, method=6)
        if os.path.abspath(new_path) != os.path.abspath(file_path):
            os.remove(file_path)
        return new_path
    except Exception as e:
        print(f'compress_image failed for {file_path}:', e)
        return None

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Image, Paragraph, Spacer, Table, TableStyle, HRFlowable
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT

app = FastAPI()

from starlette.middleware.sessions import SessionMiddleware
import secrets as _secrets
ADMIN_PIN = os.environ.get('ZENIFA_PIN', 'zenifa2026')
APP_VERSION = "v1.2"

@app.get("/__health__", include_in_schema=False)
def __health__():
    return {"status": "ok", "version": APP_VERSION, "ts": datetime.utcnow().isoformat() + "Z"}
SESSION_SECRET = os.environ.get('ZENIFA_SESSION_SECRET', _secrets.token_hex(32))
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET, max_age=60*60*8, same_site='strict', https_only=False)

def _pin_ok(request: Request) -> bool:
    return bool(request.session.get('pin_ok'))

def _pin_guard(request: Request):
    # Return RedirectResponse if NOT authed; None if authed. Use for POST mutators.
    if not _pin_ok(request):
        return RedirectResponse(url="/pin", status_code=303)
    return None

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOADS_DIR = os.path.join(BASE_DIR, "uploads")
DATA_DIR = os.path.join(BASE_DIR, "data")
PDF_WORK_DIR = os.path.join(BASE_DIR, "pdf_work")
os.makedirs(UPLOADS_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(PDF_WORK_DIR, exist_ok=True)

app.mount("/uploads", StaticFiles(directory=UPLOADS_DIR), name="uploads")
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "static"))

# Force HTML no-cache so latest JS always served
from starlette.middleware.base import BaseHTTPMiddleware
class NoCacheHTML(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        resp = await call_next(request)
        ct = resp.headers.get('content-type', '')
        if 'text/html' in ct:
            resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
            resp.headers['Pragma'] = 'no-cache'
        return resp

app.add_middleware(NoCacheHTML)

DB_PATH = os.path.join(DATA_DIR, "shoes.db")

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _save_task_images(files) -> list:
    """Route uploads by type with parallel R2 uploads for PDFs.
    Images: local + auto-compress to webp (sequential, fast).
    PDFs: parallel upload to R2 via ThreadPoolExecutor (4-8 workers).
    """
    files = [f for f in (files or []) if f and f.filename]
    if not files:
        return []
    tasks_dir = os.path.join(UPLOADS_DIR, "tasks")
    os.makedirs(tasks_dir, exist_ok=True)
    img_exts = {'.jpg', '.jpeg', '.png', '.webp', '.gif'}
    r2_ready = os.environ.get('R2_ACCOUNT_ID') and os.environ.get('R2_BUCKET')

    # Split work: PDFs go to parallel pool, images stay sequential (CPU-bound compress)
    pdf_files = [f for f in files if os.path.splitext(f.filename)[1].lower() == '.pdf']
    img_files = [f for f in files if f not in pdf_files]

    saved = []

    # --- images: sequential (compress + write) ---
    for f in img_files:
        ext = os.path.splitext(f.filename)[1].lower()
        if ext in img_exts:
            name = datetime.now().strftime('%Y%m%d%H%M%S') + '_' + uuid.uuid4().hex[:8] + ext
            path = os.path.join(tasks_dir, name)
            with open(path, 'wb') as out:
                out.write(f.file.read())
            compressed = _compress_image(path)
            if compressed:
                saved.append(('uploads/tasks/' + os.path.basename(compressed), f.filename))
            else:
                saved.append((f'uploads/tasks/{name}', f.filename))
        else:
            # Unknown type: store as-is
            name = datetime.now().strftime('%Y%m%d%H%M%S') + '_' + uuid.uuid4().hex[:8] + ext
            path = os.path.join(tasks_dir, name)
            with open(path, 'wb') as out:
                out.write(f.file.read())
            saved.append((f'uploads/tasks/{name}', f.filename))

    # --- PDFs: parallel upload to R2 (or local fallback) ---
    if pdf_files:
        if r2_ready:
            from concurrent.futures import ThreadPoolExecutor, as_completed
            def _upload_one(f):
                try:
                    return ('r2', _upload_pdf_to_r2(f))
                except Exception as e:
                    print(f'R2 upload failed for {f.filename}, fallback to local: {e}')
                    name = datetime.now().strftime('%Y%m%d%H%M%S') + '_' + uuid.uuid4().hex[:8] + '.pdf'
                    path = os.path.join(tasks_dir, name)
                    with open(path, 'wb') as out:
                        out.write(f.file.read())
                    return ('local', f'uploads/tasks/{name}', f.filename)

            with ThreadPoolExecutor(max_workers=min(8, len(pdf_files))) as ex:
                futures = [ex.submit(_upload_one, f) for f in pdf_files]
                for fut in as_completed(futures):
                    kind, url, orig_name = fut.result()
                    saved.append((url, orig_name))
                    print(f'PDF uploaded ({kind}): {url}')
        else:
            # R2 not configured: sequential local fallback
            for f in pdf_files:
                name = datetime.now().strftime('%Y%m%d%H%M%S') + '_' + uuid.uuid4().hex[:8] + '.pdf'
                path = os.path.join(tasks_dir, name)
                with open(path, 'wb') as out:
                    out.write(f.file.read())
                saved.append((f'uploads/tasks/{name}', f.filename))

    return saved

def _parse_existing_images(raw: str) -> list:
    if not raw:
        return []
    try:
        data = json.loads(raw)
        return [x for x in data if isinstance(x, str)]
    except Exception:
        return []

def _parse_existing_names(raw: str) -> list:
    if not raw:
        return []
    try:
        data = json.loads(raw)
        return [x for x in data if isinstance(x, str)]
    except Exception:
        return []

def init_db():
    conn = get_db()
    conn.execute('''
        CREATE TABLE IF NOT EXISTS designs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model_name TEXT NOT NULL,
            material TEXT,
            color TEXT,
            status TEXT DEFAULT 'draft',
            image_path TEXT,
            created_at TEXT
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS pdf_drafts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            title TEXT NOT NULL,
            notes TEXT,
            images_json TEXT,
            created_at TEXT
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            category TEXT,
            link TEXT,
            status TEXT DEFAULT 'draft',
            notes TEXT,
            result TEXT,
            result_files TEXT,
            result_files_names TEXT,
            created_at TEXT
        )
    ''')
    # Migrations: drop source, ensure result/result_files columns
    task_cols = {r[1] for r in conn.execute('PRAGMA table_info(tasks)').fetchall()}
    if 'source' in task_cols:
        conn.execute('ALTER TABLE tasks RENAME TO _tasks_old')
        conn.execute('''
            CREATE TABLE tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                category TEXT,
                link TEXT,
                status TEXT DEFAULT 'draft',
                notes TEXT,
                result TEXT,
                result_files TEXT,
                created_at TEXT
            )
        ''')
        conn.execute('''
            INSERT INTO tasks (id, title, category, link, status, notes, created_at)
            SELECT id, title, category, link, status, notes, created_at FROM _tasks_old
        ''')
        conn.execute('DROP TABLE _tasks_old')
    task_cols = {r[1] for r in conn.execute('PRAGMA table_info(tasks)').fetchall()}
    if 'result_images' in task_cols and 'result_files' not in task_cols:
        conn.execute('ALTER TABLE tasks RENAME COLUMN result_images TO result_files')
    elif 'result_files' not in task_cols and 'result' in task_cols:
        conn.execute('ALTER TABLE tasks ADD COLUMN result_files TEXT')
    if 'result_files_names' not in task_cols:
        conn.execute('ALTER TABLE tasks ADD COLUMN result_files_names TEXT')
    # Seed initial tasks from JOBDESK (only if empty)
    count = conn.execute('SELECT COUNT(*) AS n FROM tasks').fetchone()['n']
    if count == 0:
        seed_tasks = [
            ('Edit sepatu bola nomor 2 (jahitan/embossan/belakang)', 'Edit', 'https://drive.google.com/drive/folders/1auwkyP3RMJyDgmy384fWHqzBsSAZYBbd?usp=sharing', 'draft', 'Detail: jahitan, embossan, bagian belakang'),
            ('Foto produk sepatu futsal specs reborn anak', 'Foto Produk', '', 'draft', ''),
            ('Foto produk sepatu running zenifa afterspark', 'Foto Produk', '', 'draft', ''),
            ('Tambah warna sepatu running ZNC', 'Edit Warna', 'https://drive.google.com/drive/folders/1wmRQakinPbdWugGp90n42FSUzlGV2BHL?usp=sharing', 'draft', ''),
            ('Edit warna phoenix man', 'Edit Warna', '', 'draft', ''),
            ('Edit motif mizuno fg', 'Edit Motif', '', 'draft', ''),
            ('Bantu edit baju', 'Edit', '', 'draft', ''),
            ('Riset tools berbayar', 'Riset', '', 'draft', ''),
            ('Laporan pelamar kerja 2 pelamar', 'Laporan', '', 'draft', '2 pelamar kerja'),
            ('Edit model bola mizuno', 'Edit Model', '', 'draft', ''),
            ('Edit model running pria model anta', 'Edit Model', '', 'draft', ''),
        ]
        now = datetime.now().strftime('%Y-%m-%d %H:%M')
        for t in seed_tasks:
            conn.execute(
                'INSERT INTO tasks (title, category, link, status, notes, created_at) VALUES (?,?,?,?,?,?)',
                (*t, now)
            )
    conn.commit()
    conn.close()

init_db()

# ── TASKS (Jobdesk) ────────────────────────────────────────────

@app.get("/tasks", response_class=HTMLResponse)
async def tasks_page(request: Request):
    # Unified view: same template for admin & public. Admin chrome gated by pin_ok.
    pin_ok = _pin_ok(request)
    conn = get_db()
    tasks = conn.execute('SELECT * FROM tasks ORDER BY id DESC').fetchall()
    conn.close()
    tasks_list = []
    for t in tasks:
        d = dict(t)
        try:
            d['result_files_list'] = json.loads(d.get('result_files') or '[]')
        except Exception:
            d['result_files_list'] = []
        try:
            d['result_files_names_list'] = json.loads(d.get('result_files_names') or '[]')
        except Exception:
            d['result_files_names_list'] = []
        tasks_list.append(d)
    return templates.TemplateResponse(
        request=request, name="index.html",
        context={"tasks": tasks_list, "menu": "tasks", "pin_ok": pin_ok, "app_version": APP_VERSION}
    )

@app.get("/task/{task_id}", response_class=HTMLResponse)
async def task_detail(task_id: int, request: Request):
    pin_ok = _pin_ok(request)
    conn = get_db()
    row = conn.execute('SELECT * FROM tasks WHERE id = ?', (task_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Task tidak ditemukan")
    t = dict(row)
    try:
        t['result_files_list'] = json.loads(t.get('result_files') or '[]')
    except Exception:
        t['result_files_list'] = []
    try:
        t['result_files_names_list'] = json.loads(t.get('result_files_names') or '[]')
    except Exception:
        t['result_files_names_list'] = []
    return templates.TemplateResponse(
        request=request, name="index.html",
        context={"tasks": [], "detail_task": t, "menu": "tasks", "pin_ok": pin_ok, "app_version": APP_VERSION}
    )

@app.get("/pin", response_class=HTMLResponse)
async def pin_form(request: Request):
    return templates.TemplateResponse(request=request, name="pin.html", context={"next": request.headers.get("referer", "/tasks"), "pin_ok": _pin_ok(request), "app_version": APP_VERSION})

@app.post("/pin")
async def pin_verify(request: Request, pin: str = Form(...), next: str = Form("/tasks")):
    if pin.strip() == ADMIN_PIN:
        request.session["pin_ok"] = True
        return RedirectResponse(url=next or "/tasks", status_code=303)
    return templates.TemplateResponse(request=request, name="pin.html", context={"next": next, "error": "PIN salah", "pin_ok": _pin_ok(request), "app_version": APP_VERSION}, status_code=401)

@app.post("/pin/logout")
async def pin_logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/pin", status_code=303)

@app.post("/tasks/add")
async def task_add(request: Request,
    title: str = Form(...),
    category: str = Form(""),
    link: str = Form(""),
    notes: str = Form(""),
    result: str = Form(""),
    status: str = Form("draft"),
    result_files: list[UploadFile] = File(default=[]),
    existing_images: str = Form(""),
    existing_images_names: str = Form(""),
    preuploaded_urls: str = Form(""),
    preuploaded_names: str = Form(""),
):
    guard = _pin_guard(request)
    if guard: return guard
    if not _pin_ok(request):
        return RedirectResponse(url="/pin?next=/add", status_code=303)
    saved = _save_task_images(result_files)
    keep = _parse_existing_images(existing_images)
    keep_names = _parse_existing_names(existing_images_names)
    # Parse pre-uploaded URLs from XHR parallel upload
    pre = []
    pre_names = []
    if preuploaded_urls:
        try:
            pre = [u for u in json.loads(preuploaded_urls) if u]
        except Exception:
            pre = []
    if preuploaded_names:
        try:
            pre_names = [n for n in json.loads(preuploaded_names) if n]
        except Exception:
            pre_names = []
    saved_urls = [u for u, _ in saved]
    saved_names = [n for _, n in saved]
    all_imgs = json.dumps(keep + pre + saved_urls)
    if keep_names and len(keep_names) >= len(keep):
        existing_names = keep_names[:len(keep)]
    else:
        existing_names = [os.path.basename(k) for k in keep]
    all_names = json.dumps(existing_names + pre_names + saved_names)
    conn = get_db()
    conn.execute(
        'INSERT INTO tasks (title, category, link, status, notes, result, result_files, result_files_names, created_at) VALUES (?,?,?,?,?,?,?,?,?)',
        (title.strip(), category.strip(), link.strip(), status,
         notes.strip(), result.strip(), all_imgs, all_names, datetime.now().strftime('%Y-%m-%d %H:%M'))
    )
    conn.commit()
    conn.close()
    return RedirectResponse(url="/tasks", status_code=303)

@app.post("/tasks/update/{task_id}")
async def task_update(request: Request, task_id: int,
    title: str = Form(...),
    category: str = Form(""),
    link: str = Form(""),
    notes: str = Form(""),
    result: str = Form(""),
    status: str = Form("draft"),
    result_files: list[UploadFile] = File(default=[]),
    existing_images: str = Form(""),
    existing_images_names: str = Form(""),
    preuploaded_urls: str = Form(""),
    preuploaded_names: str = Form(""),
):
    guard = _pin_guard(request)
    if guard: return guard
    if not _pin_ok(request):
        return RedirectResponse(url="/pin?next=/tasks", status_code=303)
    # v1.1: fetch old URLs BEFORE update so we can diff + delete removed files
    conn = get_db()
    row = conn.execute('SELECT result_files FROM tasks WHERE id=?', (task_id,)).fetchone()
    try:
        old_urls = json.loads(row['result_files'] or '[]') if row else []
    except Exception:
        old_urls = []

    saved = _save_task_images(result_files)
    keep = _parse_existing_images(existing_images)
    keep_names = _parse_existing_names(existing_images_names)
    pre = []
    pre_names = []
    if preuploaded_urls:
        try:
            pre = [u for u in json.loads(preuploaded_urls) if u]
        except Exception:
            pre = []
    if preuploaded_names:
        try:
            pre_names = [n for n in json.loads(preuploaded_names) if n]
        except Exception:
            pre_names = []
    saved_urls = [u for u, _ in saved]
    saved_names = [n for _, n in saved]
    all_imgs = json.dumps(keep + pre + saved_urls)
    if keep_names and len(keep_names) >= len(keep):
        existing_names = keep_names[:len(keep)]
    else:
        existing_names = [os.path.basename(k) for k in keep]
    all_names = json.dumps(existing_names + pre_names + saved_names)
    conn.execute(
        'UPDATE tasks SET title=?, category=?, link=?, status=?, notes=?, result=?, result_files=?, result_files_names=? WHERE id=?',
        (title.strip(), category.strip(), link.strip(), status,
         notes.strip(), result.strip(), all_imgs, all_names, task_id)
    )
    conn.commit()
    conn.close()

    # v1.1: cleanup removed files from R2 + local fs
    new_urls = keep + pre + saved_urls
    try:
        _delete_removed_files(old_urls, new_urls)
    except Exception as e:
        print(f'Cleanup error: {e}')

    return RedirectResponse(url="/tasks", status_code=303)


@app.post("/tasks/rename-file/{task_id}")
async def task_rename_file(request: Request, task_id: int, idx: int = Form(...), name: str = Form(...)):

    guard = _pin_guard(request)
    if guard: return guard
    if not _pin_ok(request):
        return JSONResponse({"ok": False, "error": "PIN required"}, status_code=401)
    """Rename display name of one file in result_files_names (parallel to result_files).
    Does NOT touch disk / R2 - display-only, like the rest of the name flow."""
    conn = get_db()
    row = conn.execute('SELECT result_files, result_files_names FROM tasks WHERE id = ?', (task_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, detail='task not found')
    try:
        urls = json.loads(row['result_files'] or '[]')
    except Exception:
        urls = []
    try:
        names = json.loads(row['result_files_names'] or '[]')
    except Exception:
        names = []
    if idx < 0 or idx >= len(urls):
        conn.close()
        raise HTTPException(400, detail=f'idx out of range (0..{len(urls)-1})')
    while len(names) < len(urls):
        names.append(os.path.basename(urls[len(names)]))
    # sanitize: strip slashes/backslashes, control chars, length cap
    raw = (name or '').strip()
    safe = ''.join(c for c in raw if c.isprintable() and c not in '/\\\\|?*<>:"')[:200]
    if not safe:
        safe = os.path.basename(urls[idx])
    names[idx] = safe
    conn.execute('UPDATE tasks SET result_files_names = ? WHERE id = ?', (json.dumps(names), task_id))
    conn.commit()
    conn.close()
    return {'ok': True, 'idx': idx, 'name': safe}


@app.post("/tasks/delete/{task_id}")
async def task_delete(request: Request, task_id: int):
    guard = _pin_guard(request)
    if guard: return guard
    if not _pin_ok(request):
        return RedirectResponse(url="/pin?next=/tasks", status_code=303)
    conn = get_db()
    row = conn.execute('SELECT result_files FROM tasks WHERE id = ?', (task_id,)).fetchone()
    conn.execute('DELETE FROM tasks WHERE id = ?', (task_id,))
    conn.commit()
    conn.close()
    if row and row['result_files']:
        try:
            urls = json.loads(row['result_files'])
        except Exception:
            urls = []
        _delete_r2_objects(_extract_remote_pdfs(urls))
    return RedirectResponse(url="/tasks", status_code=303)

@app.post("/tasks/bulk-delete")
async def task_bulk_delete(request: Request, ids: str = Form(...)):

    guard = _pin_guard(request)
    if guard: return guard
    if not _pin_ok(request):
        return RedirectResponse(url="/pin?next=/tasks", status_code=303)
    id_list = [int(x) for x in ids.split(",") if x.strip().isdigit()]
    if id_list:
        conn = get_db()
        placeholders = ",".join("?" * len(id_list))
        rows = conn.execute(f'SELECT result_files FROM tasks WHERE id IN ({placeholders})', id_list).fetchall()
        conn.execute(f'DELETE FROM tasks WHERE id IN ({placeholders})', id_list)
        conn.commit()
        conn.close()
        for row in rows:
            if row and row['result_files']:
                try:
                    urls = json.loads(row['result_files'])
                except Exception:
                    urls = []
                _delete_r2_objects(_extract_remote_pdfs(urls))
    return RedirectResponse(url="/tasks", status_code=303)

@app.post("/tasks/status/{task_id}")
async def task_quick_status(request: Request, task_id: int, status: str = Form(...), next: str = Form(default="/tasks")):

    guard = _pin_guard(request)
    if guard: return guard
    if status not in ('draft', 'proses', 'finish'):
        raise HTTPException(status_code=400, detail="bad status")
    conn = get_db()
    conn.execute('UPDATE tasks SET status=? WHERE id=?', (status, task_id))
    conn.commit()
    conn.close()
    # Only honor internal next paths (avoid open-redirect)
    if not next.startswith('/') or next.startswith('//'):
        next = '/tasks'
    return RedirectResponse(url=next, status_code=303)

# ── SINGLE-FILE UPLOAD ENDPOINT (parallel from browser) ──────

@app.get("/upload/check")
def upload_check(key: str = Query(...)):
    """Verify if R2 has a key. VPS-side check, used as fallback when browser HEAD is slow.
    Returns {ok, url, size, found}."""
    if not (os.environ.get('R2_ACCOUNT_ID') and os.environ.get('R2_BUCKET')):
        return {"ok": False, "found": False, "reason": "r2 not configured"}
    # Basic safety: reject keys with traversal
    if '..' in key or key.startswith('/'):
        return {"ok": False, "found": False, "reason": "bad key"}
    try:
        s3 = _r2()
        head = s3.head_object(Bucket=os.environ['R2_BUCKET'], Key=key)
        size = head.get('ContentLength', 0)
        return {"ok": True, "found": True, "url": f"{os.environ['R2_PUBLIC_BASE'].rstrip('/')}/{key}", "size": size}
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code', '')
        if code in ('404', 'NoSuchKey', 'NotFound'):
            return {"ok": True, "found": False, "url": f"{os.environ['R2_PUBLIC_BASE'].rstrip('/')}/{key}"}
        return {"ok": False, "found": False, "reason": str(e)}
    except Exception as e:
        return {"ok": False, "found": False, "reason": str(e)[:120]}

@app.get("/upload/presign")
async def upload_presign(filename: str, content_type: str = "application/octet-stream"):
    """Return a presigned PUT URL for direct-to-R2 browser upload.
    Skips VPS entirely. Requires R2 CORS configured to allow PUT from this origin.
    Falls back gracefully (frontend detects and uses /upload/single instead).
    """
    bucket = os.environ.get('R2_BUCKET')
    if not bucket:
        raise HTTPException(503, detail='R2 not configured')
    ext = os.path.splitext(filename)[1].lower() or '.bin'
    if ext not in ('.pdf', '.jpg', '.jpeg', '.png', '.webp', '.gif'):
        ext = '.bin'
    key = datetime.now().strftime('%Y%m%d%H%M%S') + '_' + uuid.uuid4().hex[:8] + ext
    try:
        url = _r2().generate_presigned_url(
            'put_object',
            Params={'Bucket': bucket, 'Key': key, 'ContentType': content_type},
            ExpiresIn=600,  # 10 min
            HttpMethod='PUT',
        )
        public_url = f"{os.environ['R2_PUBLIC_BASE'].rstrip('/')}/{key}"
        return {'ok': True, 'put_url': url, 'public_url': public_url, 'key': key}
    except Exception as e:
        raise HTTPException(500, detail=f'presign failed: {e}')


@app.post("/upload/telemetry")
async def upload_telemetry(req: Request):
    """Browser POSTs phase changes here so we can debug stuck uploads."""
    try:
        body = await req.json()
        msg = f"[upload-telemetry] {body}"
        print(msg, flush=True)
        with open('/tmp/zenifa_telemetry.log', 'a') as f:
            import json as _j, time as _t
            f.write(f"{_t.strftime('%H:%M:%S')} {_j.dumps(body)}\n")
        return {'ok': True}
    except Exception as e:
        return {'ok': False, 'err': str(e)}

@app.post("/upload/chunk/init")
async def upload_chunk_init(payload: dict):
    """Init chunked upload. Returns upload_id. Client then POSTs each chunk sequentially."""
    filename = payload.get('filename', 'unknown')
    content_type = payload.get('content_type', 'application/octet-stream')
    total_chunks = int(payload.get('total_chunks', 0))
    file_size = int(payload.get('file_size', 0))
    if total_chunks < 1 or total_chunks > 100 or file_size < 1 or file_size > 200 * 1024 * 1024:
        raise HTTPException(400, detail='bad params')
    upload_id = uuid.uuid4().hex
    upload_dir = f'/tmp/zenifa_chunks/{upload_id}'
    os.makedirs(upload_dir, exist_ok=True)
    with open(f'{upload_dir}/meta.json', 'w') as f:
        json.dump({'filename': filename, 'content_type': content_type, 'total_chunks': total_chunks, 'file_size': file_size, 'created': datetime.now().isoformat()}, f)
    return {'ok': True, 'upload_id': upload_id}

@app.post("/upload/chunk/{upload_id}")
async def upload_chunk_action(upload_id: str, request: Request, seq: int = Query(default=None), action: str = Query(default=None)):
    """Combined endpoint:
    - ?action=put&seq=N → store chunk binary
    - ?action=finalize → assemble + upload to R2, return URL
    - ?action=abort → cleanup
    """
    if not upload_id or not all(c in '0123456789abcdef' for c in upload_id):
        raise HTTPException(400, detail='bad upload_id')
    upload_dir = f'/tmp/zenifa_chunks/{upload_id}'
    if not os.path.isdir(upload_dir):
        raise HTTPException(404, detail='upload not found')

    if action == 'put':
        if seq is None or seq < 0:
            raise HTTPException(400, detail='seq required')
        with open(f'{upload_dir}/meta.json') as f:
            meta = json.load(f)
        if seq >= meta['total_chunks']:
            raise HTTPException(400, detail='seq out of range')
        chunk_path = f'{upload_dir}/{seq:04d}.part'
        with open(chunk_path, 'wb') as out:
            async for chunk in request.stream():
                out.write(chunk)
        return {'ok': True, 'seq': seq, 'size': os.path.getsize(chunk_path)}

    if action == 'finalize':
        with open(f'{upload_dir}/meta.json') as f:
            meta = json.load(f)
        parts = sorted([p for p in os.listdir(upload_dir) if p.endswith('.part')])
        if len(parts) != meta['total_chunks']:
            raise HTTPException(400, detail=f'incomplete: got {len(parts)} of {meta["total_chunks"]}')
        ext = os.path.splitext(meta['filename'])[1].lower()
        if ext == '.pdf' and os.environ.get('R2_ACCOUNT_ID') and os.environ.get('R2_BUCKET'):
            try:
                name = datetime.now().strftime('%Y%m%d%H%M%S') + '_' + uuid.uuid4().hex[:8] + '.pdf'
                bucket = os.environ['R2_BUCKET']
                # Concat chunks to assembled temp file, then upload via proven upload_fileobj path.
                # Disk usage: same as before (already in /tmp). Memory usage: bounded by 4MB chunks.
                assembled = f'{upload_dir}/_assembled'
                with open(assembled, 'wb') as out:
                    for p in parts:
                        ppath = f'{upload_dir}/{p}'
                        with open(ppath, 'rb') as fh:
                            while True:
                                buf = fh.read(1024 * 1024)
                                if not buf: break
                                out.write(buf)
                        os.remove(ppath)
                # Upload assembled file to R2
                with open(assembled, 'rb') as fh:
                    _r2().upload_fileobj(
                        fh,
                        bucket,
                        name,
                        ExtraArgs={'ContentType': 'application/pdf', 'ContentDisposition': 'inline'},
                    )
                os.remove(assembled)
                import shutil
                shutil.rmtree(upload_dir, ignore_errors=True)
                return {'ok': True, 'url': f"{os.environ['R2_PUBLIC_BASE'].rstrip('/')}/{name}", 'type': 'pdf', 'name': meta.get('filename')}
            except Exception as e:
                print(f'chunk finalize R2 fail: {e}')
                raise HTTPException(500, detail=f'R2 fail: {e}')
        else:
            tasks_dir = os.path.join(UPLOADS_DIR, 'tasks')
            os.makedirs(tasks_dir, exist_ok=True)
            safe_name = datetime.now().strftime('%Y%m%d%H%M%S') + '_' + uuid.uuid4().hex[:8] + ext
            final_path = os.path.join(tasks_dir, safe_name)
            with open(final_path, 'wb') as out:
                for p in parts:
                    ppath = f'{upload_dir}/{p}'
                    with open(ppath, 'rb') as fh:
                        while True:
                            buf = fh.read(1024 * 1024)
                            if not buf: break
                            out.write(buf)
            import shutil
            shutil.rmtree(upload_dir, ignore_errors=True)
            return {'ok': True, 'path': f'/uploads/tasks/{safe_name}', 'type': 'image' if ext in {'.jpg','.jpeg','.png','.webp','.gif'} else 'file', 'name': meta.get('filename')}

    if action == 'abort':
        import shutil
        shutil.rmtree(upload_dir, ignore_errors=True)
        return {'ok': True}

    raise HTTPException(400, detail='action required (put|finalize|abort)')

@app.post("/upload/single")
async def upload_single(file: UploadFile = File(...)):
    """Upload a single file via XHR. Returns JSON with final URL/path.
    - PDF: uploaded to R2, returns CDN URL
    - image: compressed to webp 1600px locally, returns /uploads/...webp path
    Designed for parallel client-side uploads (one XHR per file).
    """
    if not file or not file.filename:
        raise HTTPException(400, detail='no file')
    ext = os.path.splitext(file.filename)[1].lower()
    img_exts = {'.jpg', '.jpeg', '.png', '.webp', '.gif'}
    r2_ready = os.environ.get('R2_ACCOUNT_ID') and os.environ.get('R2_BUCKET')

    if ext == '.pdf' and r2_ready:
        try:
            url = _upload_pdf_to_r2(file)
            return {'ok': True, 'url': url, 'type': 'pdf', 'name': file.filename}
        except Exception as e:
            print(f'upload/single: R2 fail for {file.filename}: {e}, fallback local')

    # Local fallback (or image)
    tasks_dir = os.path.join(UPLOADS_DIR, 'tasks')
    os.makedirs(tasks_dir, exist_ok=True)
    if ext in img_exts:
        name = datetime.now().strftime('%Y%m%d%H%M%S') + '_' + uuid.uuid4().hex[:8] + ext
        path = os.path.join(tasks_dir, name)
        with open(path, 'wb') as out:
            out.write(await file.read())
        compressed = _compress_image(path)
        if compressed:
            return {'ok': True, 'url': 'uploads/tasks/' + os.path.basename(compressed), 'type': 'image', 'name': file.filename}
        return {'ok': True, 'url': f'uploads/tasks/{name}', 'type': 'image', 'name': file.filename}

    # Generic file (e.g. raw pdf without R2)
    name = datetime.now().strftime('%Y%m%d%H%M%S') + '_' + uuid.uuid4().hex[:8] + ext
    path = os.path.join(tasks_dir, name)
    with open(path, 'wb') as out:
        out.write(await file.read())
    return {'ok': True, 'url': f'uploads/tasks/{name}', 'type': ext.lstrip('.') or 'file', 'name': file.filename}


# ── WEB PAGES ────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(
        request=request, name="index.html", context={"menu": "welcome", "pin_ok": _pin_ok(request), "app_version": APP_VERSION}
    )

@app.get("/public/tasks", response_class=HTMLResponse)
async def public_tasks(request: Request):
    # Deprecated alias — redirects to unified /tasks (same view, no separate template).
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/tasks", status_code=301)

@app.get("/add", response_class=HTMLResponse)
async def add(request: Request):
    return templates.TemplateResponse(
        request=request, name="index.html", context={"menu": "home", "pin_ok": _pin_ok(request), "app_version": APP_VERSION}
    )

@app.get("/gallery", response_class=HTMLResponse)
async def gallery(request: Request):
    conn = get_db()
    designs = conn.execute(
        'SELECT * FROM designs WHERE status != ? ORDER BY id DESC', ('finish',)
    ).fetchall()
    conn.close()
    return templates.TemplateResponse(
        request=request, name="index.html",
        context={"designs": designs, "menu": "inwork", "pin_ok": _pin_ok(request), "app_version": APP_VERSION}
    )

@app.get("/inwork", response_class=HTMLResponse)
async def inwork(request: Request):
    conn = get_db()
    designs = conn.execute(
        'SELECT * FROM designs WHERE status != ? ORDER BY id DESC', ('finish',)
    ).fetchall()
    conn.close()
    return templates.TemplateResponse(
        request=request, name="index.html",
        context={"designs": designs, "menu": "inwork", "pin_ok": _pin_ok(request), "app_version": APP_VERSION}
    )

@app.get("/finished", response_class=HTMLResponse)
async def finished(request: Request):
    conn = get_db()
    designs = conn.execute(
        'SELECT * FROM designs WHERE status = ? ORDER BY id DESC', ('finish',)
    ).fetchall()
    conn.close()
    return templates.TemplateResponse(
        request=request, name="index.html",
        context={"designs": designs, "menu": "finished", "pin_ok": _pin_ok(request), "app_version": APP_VERSION}
    )

@app.post("/add")
async def add_design(
    model_name: str = Form(...),
    status: str = Form("draft"),
    image: UploadFile = File(None)
):
    image_path = ""
    if image and image.filename:
        file_ext = os.path.splitext(image.filename)[1].lower()  # '.jpg'
        filename = f"{datetime.now().strftime('%Y%m%d%H%M%S')}{file_ext}"
        abs_path = os.path.join(UPLOADS_DIR, filename)
        with open(abs_path, "wb") as buffer:
            buffer.write(await image.read())
        if file_ext in ('.jpg', '.jpeg', '.png', '.webp', '.gif'):
            compressed = _compress_image(abs_path)
            if compressed:
                image_path = 'uploads/' + os.path.basename(compressed)
            else:
                image_path = f'uploads/{filename}'
        else:
            image_path = f'uploads/{filename}'

    conn = get_db()
    cur = conn.execute(
        'INSERT INTO designs (model_name, status, image_path, created_at) VALUES (?, ?, ?, ?)',
        (model_name, status, image_path, datetime.now().strftime("%Y-%m-%d %H:%M"))
    )
    new_id = cur.lastrowid
    conn.commit()
    conn.close()
    return RedirectResponse(url="/inwork", status_code=303)

@app.get("/edit/{design_id}", response_class=HTMLResponse)
async def edit_page(request: Request, design_id: int):
    conn = get_db()
    design = conn.execute('SELECT * FROM designs WHERE id = ?', (design_id,)).fetchone()
    conn.close()
    if not design:
        return RedirectResponse(url="/gallery", status_code=303)
    return templates.TemplateResponse(
        request=request, name="index.html",
        context={"design": design, "menu": "edit", "pin_ok": _pin_ok(request), "app_version": APP_VERSION}
    )

@app.post("/update/{design_id}")
async def update_design(
    design_id: int,
    model_name: str = Form(...),
    status: str = Form("draft"),
    image: UploadFile = File(None)
):
    conn = get_db()
    current = conn.execute('SELECT image_path FROM designs WHERE id = ?', (design_id,)).fetchone()

    image_path = current['image_path']
    if image and image.filename:
        if current['image_path']:
            old_path = os.path.join(BASE_DIR, current['image_path'])
            if os.path.exists(old_path):
                os.remove(old_path)
        file_ext = os.path.splitext(image.filename)[1].lower()  # '.jpg'
        filename = f"{datetime.now().strftime('%Y%m%d%H%M%S')}{file_ext}"
        abs_path = os.path.join(UPLOADS_DIR, filename)
        with open(abs_path, "wb") as buffer:
            buffer.write(await image.read())
        if file_ext in ('.jpg', '.jpeg', '.png', '.webp', '.gif'):
            compressed = _compress_image(abs_path)
            if compressed:
                image_path = 'uploads/' + os.path.basename(compressed)
            else:
                image_path = f'uploads/{filename}'
        else:
            image_path = f'uploads/{filename}'

    conn.execute(
        'UPDATE designs SET model_name=?, status=?, image_path=? WHERE id=?',
        (model_name, status, image_path, design_id)
    )
    conn.commit()
    conn.close()
    return RedirectResponse(url="/gallery", status_code=303)

@app.post("/delete-image/{design_id}")
async def delete_image(design_id: int):
    conn = get_db()
    design = conn.execute('SELECT image_path FROM designs WHERE id = ?', (design_id,)).fetchone()
    if design and design['image_path']:
        full_path = os.path.join(BASE_DIR, design['image_path'])
        if os.path.exists(full_path):
            os.remove(full_path)
    conn.execute('DELETE FROM designs WHERE id = ?', (design_id,))
    conn.commit()
    conn.close()
    return {"ok": True}

@app.get("/delete/{design_id}")
async def delete_design(design_id: int):
    conn = get_db()
    design = conn.execute('SELECT image_path FROM designs WHERE id = ?', (design_id,)).fetchone()
    if design and design['image_path']:
        full_path = os.path.join(BASE_DIR, design['image_path'])
        if os.path.exists(full_path):
            os.remove(full_path)
    conn.execute('DELETE FROM designs WHERE id = ?', (design_id,))
    conn.commit()
    conn.close()
    return RedirectResponse(url="/gallery", status_code=303)


@app.post("/bulk-delete")
async def bulk_delete(ids: str = Form(...), redirect_to: str = Form("/inwork")):
    id_list = [int(x) for x in ids.split(",") if x.strip().isdigit()]
    if not id_list:
        return RedirectResponse(url=redirect_to, status_code=303)
    conn = get_db()
    placeholders = ",".join("?" * len(id_list))
    rows = conn.execute(f'SELECT id, image_path FROM designs WHERE id IN ({placeholders})', id_list).fetchall()
    for r in rows:
        if r['image_path']:
            full_path = os.path.join(BASE_DIR, r['image_path'])
            if os.path.exists(full_path):
                try: os.remove(full_path)
                except OSError: pass
    conn.execute(f'DELETE FROM designs WHERE id IN ({placeholders})', id_list)
    conn.commit()
    conn.close()
    return RedirectResponse(url=redirect_to, status_code=303)

# ── PDF BUILDER ────────────────────────────────────────────────

@app.get("/pdf", response_class=HTMLResponse)
async def pdf_builder(request: Request):
    conn = get_db()
    # Gallery picker: show all designs (any status)
    designs = conn.execute(
        'SELECT * FROM designs ORDER BY id DESC LIMIT 200'
    ).fetchall()
    conn.close()
    return templates.TemplateResponse(
        request=request, name="index.html",
        context={"menu": "pdf", "designs": designs, "pin_ok": _pin_ok(request), "app_version": APP_VERSION}
    )

@app.post("/pdf/preview")
async def pdf_preview(
    title: str = Form(...),
    notes: str = Form(""),
    selected_images: str = Form("")  # comma-separated image paths
):
    import json
    img_paths = []
    if selected_images:
        for p in selected_images.split(","):
            p = p.strip()
            if p:
                full = os.path.join(BASE_DIR, p)
                if os.path.exists(full):
                    img_paths.append(full)

    if not img_paths:
        raise HTTPException(status_code=400, detail="No valid images selected")

    buf = _build_pdf(title, notes, img_paths)
    return StreamingResponse(
        BytesIO(buf),
        media_type="application/pdf",
        headers={"Content-Disposition": f"inline; filename={title}.pdf"}
    )

@app.post("/pdf/save-draft")
async def pdf_save_draft(
    session_id: str = Form(...),
    title: str = Form(...),
    notes: str = Form(""),
    selected_images: str = Form("")
):
    import json
    conn = get_db()
    conn.execute(
        'INSERT INTO pdf_drafts (session_id, title, notes, images_json, created_at) VALUES (?, ?, ?, ?, ?)',
        (session_id, title, notes, json.dumps(selected_images.split(",") if selected_images else []),
         datetime.now().strftime("%Y-%m-%d %H:%M"))
    )
    conn.commit()
    conn.close()
    return {"ok": True, "session_id": session_id}

@app.get("/pdf/drafts", response_class=HTMLResponse)
async def pdf_drafts(request: Request):
    conn = get_db()
    drafts = conn.execute('SELECT * FROM pdf_drafts ORDER BY id DESC').fetchall()
    conn.close()
    return templates.TemplateResponse(
        request=request, name="index.html",
        context={"menu": "pdf_drafts", "drafts": drafts, "pin_ok": _pin_ok(request), "app_version": APP_VERSION}
    )

@app.get("/pdf/draft/{draft_id}/download")
async def pdf_draft_download(draft_id: int):
    import json
    conn = get_db()
    draft = conn.execute('SELECT * FROM pdf_drafts WHERE id = ?', (draft_id,)).fetchone()
    conn.close()
    if not draft:
        raise HTTPException(status_code=404, detail="Draft not found")

    img_paths = []
    for p in json.loads(draft['images_json'] or "[]"):
        p = p.strip()
        if p:
            full = os.path.join(BASE_DIR, p)
            if os.path.exists(full):
                img_paths.append(full)

    buf = _build_pdf(draft['title'], draft['notes'] or "", img_paths)
    safe_title = "".join(c for c in draft['title'] if c.isalnum() or c in " -_").strip() or "zenifa-pdf"
    return StreamingResponse(
        BytesIO(buf),
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename={safe_title}.pdf"}
    )

@app.post("/pdf/draft/{draft_id}/delete")
async def pdf_draft_delete(draft_id: int):
    conn = get_db()
    conn.execute('DELETE FROM pdf_drafts WHERE id = ?', (draft_id,))
    conn.commit()
    conn.close()
    return RedirectResponse(url="/pdf/drafts", status_code=303)


def _build_pdf(title: str, notes: str, img_paths: list) -> bytes:
    buf = BytesIO()

    PAGE_BG = colors.HexColor('#111111')
    FG = colors.HexColor('#ffffff')
    DIM = colors.HexColor('#a3a3a3')

    def _on_page(canvas, doc_):
        canvas.saveState()
        canvas.setFillColor(PAGE_BG)
        canvas.rect(0, 0, A4[0], A4[1], fill=1, stroke=0)
        canvas.restoreState()

    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=2*cm, bottomMargin=2*cm
    )

    styles = getSampleStyleSheet()
    brand_style = ParagraphStyle(
        'Brand',
        parent=styles['Normal'],
        fontSize=9,
        textColor=DIM,
        alignment=TA_CENTER,
        spaceAfter=4,
    )
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Title'],
        fontSize=20,
        textColor=FG,
        spaceAfter=6,
        alignment=TA_CENTER,
    )
    note_style = ParagraphStyle(
        'Note',
        parent=styles['Normal'],
        fontSize=10,
        textColor=DIM,
        leading=14,
    )
    caption_style = ParagraphStyle(
        'Caption',
        parent=styles['Normal'],
        fontSize=9,
        textColor=DIM,
        alignment=TA_CENTER,
    )

    story = []

    # Header
    story.append(Paragraph("ZENIFA DESIGN DEPARTMENT", brand_style))
    story.append(Paragraph(title, title_style))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor('#2a2a2a')))
    story.append(Spacer(1, 12))

    # Notes
    if notes.strip():
        story.append(Paragraph(f"<b>Notes:</b> {notes}", note_style))
        story.append(Spacer(1, 16))

    # Images
    for i, path in enumerate(img_paths, 1):
        try:
            img = Image(path, width=14*cm, height=10*cm)
            img.hAlign = 'CENTER'
            story.append(img)
            story.append(Paragraph(f"Halaman {i}", caption_style))
            story.append(Spacer(1, 16))
        except Exception:
            story.append(Paragraph(f"[Image {i}: could not load]", note_style))
            story.append(Spacer(1, 12))

    # Footer
    story.append(Spacer(1, 20))
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor('#2a2a2a')))
    story.append(Spacer(1, 4))
    story.append(Paragraph(
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')} · Zenifa Design Department",
        caption_style
    ))

    doc.build(story, onFirstPage=_on_page, onLaterPages=_on_page)
    return buf.getvalue()




@app.post("/upload/express")
async def upload_express_proxy(request: Request):
    """
    Same-origin proxy: frontend (HTTPS via Cloudflare) → FastAPI :8080 → Express :8081 → R2.
    Avoids mixed-content block (HTTPS page cannot fetch plain HTTP).
    Streams multipart through; returns the {url, size, success} JSON that Express emits.
    """
    import httpx
    try:
        body = await request.body()
        ct = request.headers.get("content-type", "")
        if not ct:
            return {"success": False, "error": "missing content-type"}
        async with httpx.AsyncClient(timeout=httpx.Timeout(310.0, connect=10.0)) as cli:
            r = await cli.post("http://127.0.0.1:8081/upload/r2", content=body, headers={"content-type": ct})
        try:
            return JSONResponse(content=r.json(), status_code=r.status_code)
        except Exception:
            return JSONResponse(content={"success": False, "error": f"express returned {r.status_code}: {r.text[:200]}"}, status_code=502)
    except httpx.ReadTimeout:
        return JSONResponse(content={"success": False, "error": "express timeout"}, status_code=504)
    except Exception as e:
        return JSONResponse(content={"success": False, "error": f"proxy: {e}"}, status_code=500)

@app.get("/welcome", response_class=HTMLResponse)
async def welcome(request: Request):
    return templates.TemplateResponse(
        request=request, name="index.html", context={"menu": "welcome", "pin_ok": _pin_ok(request), "app_version": APP_VERSION}
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
