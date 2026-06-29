#!/usr/bin/env python3
"""
One-shot migration: move existing local PDFs to R2 + compress old local images to webp.

Run once:
    python3 migrate_to_r2.py

What it does:
  1. Tasks DB: any result_files entry that is a local path ending in .pdf
     -> upload to R2, replace with public URL, delete local file
  2. Designs DB: image_path entries pointing to .jpg/.jpeg/.png > 200KB
     -> compress to webp 1600px, update DB path, delete original
  3. Orphan scan: files in uploads/tasks/ and uploads/ that no DB row references
     -> listed only (not deleted) so you can decide manually

Dry-run by default. Pass --apply to actually mutate.
"""
import json
import os
import sqlite3
import sys
from pathlib import Path

# Load .env so R2 creds are present
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / '.env')
except Exception:
    pass

import boto3
from botocore.config import Config

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / 'data' / 'shoes.db'
UPLOADS_DIR = BASE_DIR / 'uploads'

APPLY = '--apply' in sys.argv


def r2_client():
    return boto3.client(
        's3',
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ['R2_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['R2_SECRET_ACCESS_KEY'],
        config=Config(signature_version='s3v4'),
        region_name='auto',
    )


def upload_pdf(local_path: Path, bucket: str) -> str:
    import uuid
    from datetime import datetime
    name = datetime.now().strftime('%Y%m%d%H%M%S') + '_' + uuid.uuid4().hex[:8] + '.pdf'
    r2_client().upload_fileobj(
        open(local_path, 'rb'),
        bucket, name,
        ExtraArgs={'ContentType': 'application/pdf', 'ContentDisposition': 'inline'},
    )
    return f"{os.environ['R2_PUBLIC_BASE'].rstrip('/')}/{name}"


def compress_image(local_path: Path) -> Path | None:
    try:
        from PIL import Image
    except ImportError:
        print('  [WARN] Pillow missing — skip')
        return None
    try:
        img = Image.open(local_path)
        if img.format not in ('JPEG', 'PNG', 'WEBP', 'GIF'):
            return None
        if img.width > 1600:
            ratio = 1600 / img.width
            img = img.resize((1600, int(img.height * ratio)), Image.LANCZOS)
        new_path = local_path.with_suffix('.webp')
        if img.mode in ('RGBA', 'LA', 'P'):
            img.save(new_path, 'WEBP', quality=82, method=6)
        else:
            img.convert('RGB').save(new_path, 'WEBP', quality=82, method=6)
        if new_path != local_path:
            local_path.unlink()
        return new_path
    except Exception as e:
        print(f'  [ERR] compress {local_path.name}: {e}')
        return None


def main():
    bucket = os.environ.get('R2_BUCKET', '')
    r2_base = os.environ.get('R2_PUBLIC_BASE', '').rstrip('/')
    r2_ready = bool(os.environ.get('R2_ACCOUNT_ID') and bucket and r2_base)
    print(f'Mode: {"APPLY (mutating)" if APPLY else "DRY-RUN (no changes)"}')
    print(f'R2 ready: {r2_ready}')
    if not r2_ready:
        print('R2 not configured — PDF migration will be skipped.')
    print()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # === 1. Tasks: local PDFs -> R2 ===
    print('=== TASKS: migrate local PDFs to R2 ===')
    task_count = 0
    for row in conn.execute("SELECT id, result_files FROM tasks WHERE result_files IS NOT NULL"):
        try:
            urls = json.loads(row['result_files'])
        except Exception:
            continue
        changed = False
        new_urls = []
        for u in urls:
            if isinstance(u, str) and u.startswith('uploads/tasks/') and u.lower().endswith('.pdf'):
                local = BASE_DIR / u
                if not local.exists():
                    print(f'  task {row["id"]}: {local.name} missing on disk, skipping')
                    new_urls.append(u)
                    continue
                if not r2_ready:
                    print(f'  task {row["id"]}: {local.name} -> R2 SKIPPED (no creds)')
                    new_urls.append(u)
                    continue
                if APPLY:
                    try:
                        remote = upload_pdf(local, bucket)
                        local.unlink()
                        new_urls.append(remote)
                        changed = True
                        print(f'  task {row["id"]}: {local.name} -> {remote}')
                    except Exception as e:
                        print(f'  task {row["id"]}: upload FAILED for {local.name}: {e}')
                        new_urls.append(u)
                else:
                    print(f'  task {row["id"]}: would upload {local.name} ({local.stat().st_size} B)')
                    new_urls.append(u)
            else:
                new_urls.append(u)
        if changed:
            conn.execute('UPDATE tasks SET result_files=? WHERE id=?',
                         (json.dumps(new_urls), row['id']))
            task_count += 1
    print(f'Tasks updated: {task_count}')

    # === 2. Designs: compress old images ===
    print()
    print('=== DESIGNS: compress large images to webp ===')
    img_count = 0
    for row in conn.execute("SELECT id, image_path FROM designs WHERE image_path IS NOT NULL AND image_path != ''"):
        if not row['image_path'].startswith('uploads/'):
            continue
        local = BASE_DIR / row['image_path']
        if not local.exists():
            print(f'  design {row["id"]}: {local.name} missing on disk, skipping')
            continue
        if local.suffix.lower() not in ('.jpg', '.jpeg', '.png', '.gif'):
            continue
        size = local.stat().st_size
        if size < 200 * 1024:
            continue  # already small, skip
        if APPLY:
            new = compress_image(local)
            if new:
                rel = 'uploads/' + new.name
                conn.execute('UPDATE designs SET image_path=? WHERE id=?', (rel, row['id']))
                img_count += 1
                print(f'  design {row["id"]}: {local.name} ({size} B) -> {new.name} ({new.stat().st_size} B)')
        else:
            print(f'  design {row["id"]}: would compress {local.name} ({size} B)')
            img_count += 1
    print(f'Designs compressed: {img_count}')

    # === 3. Orphan scan ===
    print()
    print('=== ORPHAN SCAN (files not referenced by DB) ===')
    referenced = set()
    for row in conn.execute("SELECT result_files FROM tasks WHERE result_files IS NOT NULL"):
        try:
            for u in json.loads(row['result_files']):
                if isinstance(u, str) and u.startswith('uploads/'):
                    referenced.add(BASE_DIR / u)
        except Exception:
            pass
    for row in conn.execute("SELECT image_path FROM designs WHERE image_path IS NOT NULL"):
        if row['image_path'].startswith('uploads/'):
            referenced.add(BASE_DIR / row['image_path'])

    for sub in ('tasks', ''):
        scan_dir = UPLOADS_DIR / sub if sub else UPLOADS_DIR
        if not scan_dir.exists():
            continue
        for f in scan_dir.iterdir():
            if not f.is_file() or f.name.startswith('.'):
                continue
            if f in referenced:
                continue
            print(f'  orphan: {f.relative_to(BASE_DIR)} ({f.stat().st_size} B)')

    if APPLY:
        conn.commit()
        print()
        print('COMMITTED.')
    else:
        print()
        print('Dry-run only. Re-run with --apply to mutate.')
    conn.close()


if __name__ == '__main__':
    main()
