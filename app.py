from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import threading
import os

from database import init_db, get_db, DB_PATH
from grid_utils import generate_grid, geocode_address
from scraper import run_scan_sync

app = Flask(__name__)
CORS(app)

# ── אתחול ──────────────────────────────────────────────────────────────────────

init_db()

# ── דפים ───────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')

# ── API עסקים ──────────────────────────────────────────────────────────────────

@app.route('/api/businesses', methods=['GET'])
def get_businesses():
    db = get_db()
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

    if not name or not address:
        return jsonify({'error': 'שם וכתובת הם שדות חובה'}), 400

    # אם הועברו קורדינטות ישירות (מ-Maps URL)
    lat = data.get('lat')
    lng = data.get('lng')
    if not lat:
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
