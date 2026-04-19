from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import threading
import os
import re
import sys

from database import init_db, get_db, DB_PATH, USE_PG, DATABASE_URL
from grid_utils import generate_grid, geocode_address
from scraper import run_scan_sync

app = Flask(__name__)
CORS(app)

# ── אתחול ──────────────────────────────────────────────────────────────────────

init_db()


def _mark_stuck_scans_on_startup():
    """
    בעליית השירות — סמן כל סריקה תקועה כ-error.
    לא מפעילים Chromium בעלייה כדי למנוע OOM על Render free tier (512MB).
    המשתמש יכול להפעיל סריקה חדשה ידנית.
    """
    try:
        db = get_db()
        stuck = db.execute(
            "SELECT id, status FROM scans WHERE status LIKE 'running:%%'"
        ).fetchall()

        for scan in stuck:
            scan_dict = dict(scan)
            scan_id = scan_dict['id']
            db.execute("UPDATE scans SET status='error' WHERE id=?", (scan_id,))
            print(f"⚠️ Scan #{scan_id} was stuck ({scan_dict['status']}) — marked as error on startup")

        if stuck:
            db.commit()
            print(f"✅ Marked {len(stuck)} stuck scan(s) as error")

        db.close()
    except Exception as e:
        print(f"⚠️ Startup scan cleanup failed: {e}")




# בעליית השירות — סמן סריקות תקועות כ-error (בלי להפעיל Chromium!)
_mark_stuck_scans_on_startup()

# ── דפים ───────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')

# ── API עסקים ──────────────────────────────────────────────────────────────────

@app.route('/api/businesses', methods=['GET'])
def get_businesses():
    db = get_db()
    if USE_PG:
        businesses = db.execute(
            'SELECT b.*, STRING_AGG(k.keyword, \'|||\') as keywords_str '
            'FROM businesses b '
            'LEFT JOIN keywords k ON k.business_id = b.id '
            'GROUP BY b.id ORDER BY b.created_at DESC'
        ).fetchall()
    else:
        businesses = db.execute(
            'SELECT b.*, GROUP_CONCAT(k.keyword, "|||") as keywords_str '
            'FROM businesses b '
            'LEFT JOIN keywords k ON k.business_id = b.id '
            'GROUP BY b.id ORDER BY b.created_at DESC'
        ).fetchall()
    result = []
    for b in businesses:
        d = dict(b)
        d['keywords'] = d.pop('keywords_str', '').split('|||') if d.get('keywords_str') else []
        result.append(d)
    db.close()
    return jsonify(result)


@app.route('/api/businesses', methods=['POST'])
def add_business():
    data = request.json
    name = data.get('name', '').strip()
    address = data.get('address', '').strip()
    keywords = [k.strip() for k in data.get('keywords', []) if k.strip()]

    # אם הועברו קורדינטות ישירות (מ-Maps URL)
    lat = data.get('lat')
    lng = data.get('lng')

    if not name:
        return jsonify({'error': 'שם הוא שדה חובה'}), 400

    if lat and lng:
        # יש קורדינטות מ-URL — כתובת לא חובה
        if not address:
            address = f'{float(lat):.4f}, {float(lng):.4f}'
    else:
        if not address:
            return jsonify({'error': 'נדרשת כתובת או קישור Google Maps'}), 400
        lat, lng = geocode_address(address)
        if not lat:
            return jsonify({'error': f'לא נמצאה כתובת: {address}'}), 400

    db = get_db()
    cursor = db.execute(
        'INSERT INTO businesses (name, address, lat, lng) VALUES (?, ?, ?, ?)',
        (name, address, lat, lng)
    )
    business_id = cursor.lastrowid
    for kw in keywords:
        db.execute('INSERT INTO keywords (business_id, keyword) VALUES (?, ?)',
                   (business_id, kw))
    db.commit()
    db.close()

    return jsonify({'id': business_id, 'lat': lat, 'lng': lng, 'name': name})


@app.route('/api/businesses/<int:bid>', methods=['DELETE'])
def delete_business(bid):
    db = get_db()
    db.execute('DELETE FROM businesses WHERE id=?', (bid,))
    db.commit()
    db.close()
    return jsonify({'ok': True})


@app.route('/api/businesses/<int:bid>/keywords', methods=['PUT'])
def update_keywords(bid):
    data = request.json
    keywords = [k.strip() for k in data.get('keywords', []) if k.strip()]
    db = get_db()
    db.execute('DELETE FROM keywords WHERE business_id=?', (bid,))
    for kw in keywords:
        db.execute('INSERT INTO keywords (business_id, keyword) VALUES (?, ?)', (bid, kw))
    db.commit()
    db.close()
    return jsonify({'ok': True})

# ── Geocode ────────────────────────────────────────────────────────────────────

@app.route('/api/geocode')
def geocode():
    address = request.args.get('address', '')
    lat, lng = geocode_address(address)
    if lat:
        return jsonify({'lat': lat, 'lng': lng})
    return jsonify({'error': 'Not found'}), 404

# ── API סריקות ─────────────────────────────────────────────────────────────────

@app.route('/api/scans', methods=['GET'])
def get_scans():
    bid = request.args.get('business_id')
    db = get_db()
    if bid:
        scans = db.execute(
            'SELECT s.*, b.name as business_name FROM scans s '
            'JOIN businesses b ON b.id=s.business_id '
            'WHERE s.business_id=? ORDER BY s.created_at DESC',
            (bid,)
        ).fetchall()
    else:
        scans = db.execute(
            'SELECT s.*, b.name as business_name FROM scans s '
            'JOIN businesses b ON b.id=s.business_id '
            'ORDER BY s.created_at DESC LIMIT 100'
        ).fetchall()
    db.close()
    return jsonify([dict(s) for s in scans])


@app.route('/api/scans', methods=['POST'])
def start_scan():
    data = request.json
    business_id = data.get('business_id')
    keyword = data.get('keyword', '').strip()
    grid_size = int(data.get('grid_size', 7))
    spacing_km = float(data.get('spacing_km', 1.0))

    if not business_id or not keyword:
        return jsonify({'error': 'חסר business_id או keyword'}), 400

    db = get_db()
    business = db.execute('SELECT * FROM businesses WHERE id=?', (business_id,)).fetchone()
    if not business:
        db.close()
        return jsonify({'error': 'עסק לא נמצא'}), 404

    # צור רשומת סריקה
    cursor = db.execute(
        'INSERT INTO scans (business_id, keyword, grid_size, spacing_km, status) VALUES (?,?,?,?,?)',
        (business_id, keyword, grid_size, spacing_km, 'running:0/0')
    )
    scan_id = cursor.lastrowid
    db.commit()

    # צור נקודות גריד
    grid_points = generate_grid(business['lat'], business['lng'], grid_size, spacing_km)

    # עדכן סטטוס
    db.execute("UPDATE scans SET status=? WHERE id=?",
               (f'running:0/{len(grid_points)}', scan_id))
    db.commit()
    db.close()

    # הרץ בthread נפרד
    t = threading.Thread(
        target=run_scan_sync,
        args=(scan_id, business['name'], keyword, grid_points, DB_PATH),
        daemon=True
    )
    t.start()

    return jsonify({'scan_id': scan_id, 'total_points': len(grid_points)})


@app.route('/api/scans/<int:scan_id>', methods=['GET'])
def get_scan(scan_id):
    db = get_db()
    scan = db.execute(
        'SELECT s.*, b.name as business_name, b.lat as b_lat, b.lng as b_lng '
        'FROM scans s JOIN businesses b ON b.id=s.business_id WHERE s.id=?',
        (scan_id,)
    ).fetchone()
    if not scan:
        db.close()
        return jsonify({'error': 'לא נמצא'}), 404
    db.close()
    return jsonify(dict(scan))


@app.route('/api/scans/<int:scan_id>/results', methods=['GET'])
def get_scan_results(scan_id):
    db = get_db()
    results = db.execute(
        'SELECT * FROM scan_results WHERE scan_id=? ORDER BY grid_row, grid_col',
        (scan_id,)
    ).fetchall()
    db.close()
    return jsonify([dict(r) for r in results])


@app.route('/api/scan-results/<int:result_id>/businesses', methods=['GET'])
def get_result_businesses(result_id):
    db = get_db()
    businesses = db.execute(
        'SELECT * FROM scan_result_businesses WHERE scan_result_id=? ORDER BY position',
        (result_id,)
    ).fetchall()
    db.close()
    return jsonify([dict(b) for b in businesses])


@app.route('/api/scans/<int:scan_id>/distribution', methods=['GET'])
def get_scan_distribution(scan_id):
    """מחזיר התפלגות דירוגים לסריקה"""
    db = get_db()
    results = db.execute(
        'SELECT rank FROM scan_results WHERE scan_id=?', (scan_id,)
    ).fetchall()
    db.close()

    dist = {
        'excellent': 0,  # 1-3
        'good': 0,       # 4-7
        'medium': 0,     # 8-10
        'weak': 0,       # 11-14
        'poor': 0,       # 15-17
        'not_found': 0,  # 18-20
        'total': len(results)
    }
    for r in results:
        rank = r['rank']
        if rank <= 3:    dist['excellent'] += 1
        elif rank <= 7:  dist['good'] += 1
        elif rank <= 10: dist['medium'] += 1
        elif rank <= 14: dist['weak'] += 1
        elif rank <= 17: dist['poor'] += 1
        else:            dist['not_found'] += 1

    return jsonify(dist)


@app.route('/api/scans/cleanup', methods=['POST'])
def cleanup_stuck_scans():
    """סימון כל הסריקות התקועות כ-error"""
    db = get_db()
    stuck = db.execute(
        "SELECT id, status FROM scans WHERE status LIKE 'running:%'"
    ).fetchall()
    cleaned = []
    for s in stuck:
        sd = dict(s)
        db.execute("UPDATE scans SET status='error' WHERE id=?", (sd['id'],))
        cleaned.append(sd['id'])
    db.commit()
    db.close()
    return jsonify({'cleaned': cleaned, 'count': len(cleaned)})


@app.route('/api/scans/<int:scan_id>/resume', methods=['POST'])
def resume_scan(scan_id):
    """המשך סריקה תקועה ידנית — בלי לאבד תוצאות קיימות"""
    db = get_db()
    scan = db.execute(
        'SELECT s.*, b.name as business_name, b.lat, b.lng '
        'FROM scans s JOIN businesses b ON b.id=s.business_id WHERE s.id=?',
        (scan_id,)
    ).fetchone()
    if not scan:
        db.close()
        return jsonify({'error': 'סריקה לא נמצאה'}), 404

    scan_dict = dict(scan)
    status = scan_dict['status']

    # אפשר resume רק לסריקות שנתקעו
    if status == 'done':
        db.close()
        return jsonify({'error': 'הסריקה כבר הסתיימה'}), 400

    # מצא אילו נקודות כבר הושלמו
    completed_points = db.execute(
        "SELECT grid_row, grid_col FROM scan_results WHERE scan_id=?",
        (scan_id,)
    ).fetchall()
    completed_set = {(r['grid_row'], r['grid_col']) for r in completed_points}

    # צור מחדש את כל נקודות הגריד
    all_points = generate_grid(
        scan_dict['lat'], scan_dict['lng'],
        scan_dict['grid_size'], scan_dict['spacing_km']
    )

    # סנן רק נקודות שלא הושלמו
    remaining = [p for p in all_points if (p['row'], p['col']) not in completed_set]

    if not remaining:
        # כל הנקודות הושלמו — סגור
        all_ranks = db.execute(
            "SELECT rank FROM scan_results WHERE scan_id=?", (scan_id,)
        ).fetchall()
        avg = round(sum(r['rank'] for r in all_ranks) / len(all_ranks), 1) if all_ranks else 20
        db.execute(
            "UPDATE scans SET status='done', avg_rank=?, completed_at=CURRENT_TIMESTAMP WHERE id=?",
            (avg, scan_id))
        db.commit()
        db.close()
        return jsonify({'status': 'done', 'avg_rank': avg, 'message': 'כל הנקודות כבר הושלמו'})

    done_count = len(completed_set)
    total = len(all_points)
    db.execute("UPDATE scans SET status=? WHERE id=?",
               (f'running:{done_count}/{total}', scan_id))
    db.commit()
    db.close()

    # הרץ את הנקודות הנותרות ב-thread
    t = threading.Thread(
        target=run_scan_sync,
        args=(scan_id, scan_dict['business_name'], scan_dict['keyword'],
              remaining, DB_PATH),
        kwargs={'already_done': done_count, 'total_override': total},
        daemon=True
    )
    t.start()

    return jsonify({
        'status': 'resumed',
        'completed': done_count,
        'remaining': len(remaining),
        'total': total
    })


@app.route('/api/debug-scrape', methods=['POST'])
def debug_scrape():
    """דיבוג — מריץ סריקה של נקודה אחת ומחזיר את ה-HTML של הדף"""
    import asyncio
    from playwright.async_api import async_playwright

    data = request.json or {}
    lat = data.get('lat', 36.0970)
    lng = data.get('lng', -80.2453)
    keyword = data.get('keyword', 'Air Duct Cleaning Near Me')
    keyword_url = '+'.join(keyword.strip().split())

    async def _debug():
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=[
                '--no-sandbox', '--disable-setuid-sandbox',
                '--disable-dev-shm-usage', '--disable-gpu',
                '--single-process', '--window-size=1280,720',
            ])
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                viewport={'width': 1280, 'height': 720},
                locale='en-US',
                timezone_id='America/New_York',
                geolocation={'latitude': lat, 'longitude': lng},
                permissions=['geolocation'],
            )
            page = await context.new_page()
            url = f"https://www.google.com/maps/search/{keyword_url}/@{lat},{lng},13z?hl=en"
            await page.goto(url, timeout=25000, wait_until='domcontentloaded')
            await asyncio.sleep(5)

            # בדוק אם יש feed
            feed = await page.query_selector('div[role="feed"]')
            feed_exists = feed is not None

            # שלוף כותרת, URL נוכחי, וטקסט ראשוני של הדף
            title = await page.title()
            current_url = page.url

            # שלוף HTML קצר של body
            body_text = await page.evaluate('() => document.body.innerText.substring(0, 3000)')

            # בדוק אם יש consent dialog
            consent = await page.query_selector('form[action*="consent"]')
            consent_exists = consent is not None

            # בדוק כמה ילדים יש ב-feed
            feed_children = 0
            if feed_exists:
                feed_children = await page.evaluate('() => { const f = document.querySelector(\'div[role="feed"]\'); return f ? f.children.length : 0; }')

            await browser.close()
            return {
                'url_loaded': url,
                'current_url': current_url,
                'title': title,
                'feed_exists': feed_exists,
                'feed_children': feed_children,
                'consent_dialog': consent_exists,
                'body_text_preview': body_text[:2000]
            }

    loop = asyncio.new_event_loop()
    result = loop.run_until_complete(_debug())
    loop.close()
    return jsonify(result)


@app.route('/api/test-subprocess', methods=['POST'])
def test_subprocess():
    """בודק subprocess בודד עם נקודה אחת ומחזיר את ה-stderr"""
    import subprocess as sp
    import json as js

    data = request.json or {}
    lat = data.get('lat', 36.0970)
    lng = data.get('lng', -80.2453)
    keyword = data.get('keyword', 'Air Duct Cleaning Near Me')
    business_name = data.get('business_name', 'Precision Air Care')

    batch_input = js.dumps({
        'keyword': keyword,
        'business_name': business_name,
        'points': [{'lat': lat, 'lng': lng, 'row': 0, 'col': 0}]
    })

    try:
        result = sp.run(
            [sys.executable, os.path.abspath('scraper.py'), '--batch-worker'],
            input=batch_input,
            capture_output=True,
            text=True,
            timeout=120
        )
        return jsonify({
            'returncode': result.returncode,
            'stdout': result.stdout[:2000] if result.stdout else '',
            'stderr': result.stderr[:3000] if result.stderr else '',
            'success': result.returncode == 0
        })
    except sp.TimeoutExpired:
        return jsonify({'error': 'subprocess timeout (120s)', 'success': False})
    except Exception as e:
        return jsonify({'error': str(e), 'success': False})


@app.route('/api/scans/<int:scan_id>', methods=['DELETE'])
def delete_scan(scan_id):
    db = get_db()
    db.execute('DELETE FROM scans WHERE id=?', (scan_id,))
    db.commit()
    db.close()
    return jsonify({'ok': True})

# ── הרצה ───────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('DEBUG', 'false').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug)


@app.route('/api/parse-maps-url', methods=['POST'])
def parse_maps_url():
    import re
    import requests as req
    import urllib.parse
    url = request.json.get('url', '').strip()
    if not url:
        return jsonify({'error': 'No URL'}), 400
    try:
        r = req.get(url, allow_redirects=True, timeout=10,
                    headers={'User-Agent': 'Mozilla/5.0'})
        final_url = r.url
        m = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', final_url)
        if m:
            lat, lng = float(m.group(1)), float(m.group(2))
            name = ''
            nm = re.search(r'/place/([^/@]+)', final_url)
            if nm:
                name = urllib.parse.unquote_plus(nm.group(1))
            return jsonify({'lat': lat, 'lng': lng, 'name': name})
        m2 = re.search(r'll=(-?\d+\.\d+),(-?\d+\.\d+)', final_url)
        if m2:
            return jsonify({'lat': float(m2.group(1)), 'lng': float(m2.group(2)), 'name': ''})
        return jsonify({'error': 'לא נמצאו קורדינטות'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/parse-maps-url-browser', methods=['POST'])
def parse_maps_url_browser():
    """משתמש בפלייוורייט לפתוח URL ולחלץ קורדינטות"""
    import re, asyncio
    from playwright.async_api import async_playwright

    url = request.json.get('url', '').strip()
    if not url:
        return jsonify({'error': 'No URL'}), 400

    async def _get_coords():
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=[
                '--no-sandbox', '--disable-setuid-sandbox',
                '--disable-dev-shm-usage', '--disable-gpu'
            ])
            page = await browser.new_page()
            try:
                await page.goto(url, timeout=20000, wait_until='domcontentloaded')
                await asyncio.sleep(3)
                final_url = page.url

                # חלץ @lat,lng
                m = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', final_url)
                if m:
                    lat, lng = float(m.group(1)), float(m.group(2))
                    # נסה לחלץ שם
                    name = ''
                    nm = re.search(r'/place/([^/@]+)', final_url)
                    if nm:
                        import urllib.parse
                        name = urllib.parse.unquote_plus(nm.group(1))
                    return {'lat': lat, 'lng': lng, 'name': name}
                return {'error': 'לא נמצאו קורדינטות גם אחרי טעינה'}
            except Exception as e:
                return {'error': str(e)}
            finally:
                await browser.close()

    loop = asyncio.new_event_loop()
    result = loop.run_until_complete(_get_coords())
    loop.close()

    if 'error' in result:
        return jsonify(result), 400
    return jsonify(result)
