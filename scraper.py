import asyncio
import random
import time
import re
import os
import gc
import json
import sys
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    print("⚠️  Playwright not installed - using mock mode")


BROWSER_ARGS = [
    '--no-sandbox',
    '--disable-setuid-sandbox',
    '--disable-dev-shm-usage',
    '--disable-gpu',
    '--disable-extensions',
    '--disable-background-timer-throttling',
    '--disable-renderer-backgrounding',
    '--single-process',
    '--window-size=1280,720',
]

# כמה נקודות לעבד בכל תהליך-בן (subprocess)
BATCH_SIZE = 8

# כמה תהליכי-בן להריץ במקביל (Render free = 512MB, כרומיום צורך ~200MB)
MAX_PARALLEL = 1

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
]


def _find_rank(items_data, business_name):
    """מוצא את הדירוג של העסק שלנו ברשימת התוצאות"""
    name_words = [w.lower() for w in business_name.split() if len(w) > 2]
    business_lower = business_name.lower().strip()
    min_matches = max(2, (len(name_words) + 1) // 2) if len(name_words) >= 2 else 1

    for i, item in enumerate(items_data):
        item_name = item.get('name', '').lower().strip()
        if business_lower in item_name or item_name in business_lower:
            return i + 1
        matches = sum(1 for w in name_words if w in item_name)
        if matches >= min_matches:
            return i + 1
    return 20


async def _extract_top_businesses(page, business_name: str, top_n: int = 5):
    """
    שולף את Top N עסקים מהתוצאות בגוגל מפות.
    מחזיר (rank, businesses_list)
    """
    try:
        await page.wait_for_selector('div[role="feed"]', timeout=15000)
    except:
        await asyncio.sleep(5)

    try:
        items_data = await page.evaluate('''(topN) => {
            const feed = document.querySelector('div[role="feed"]');
            if (!feed) return [];

            const results = [];
            const children = Array.from(feed.children);

            for (const child of children.slice(0, 20)) {
                const text = child.innerText || '';
                if (text.trim().length < 5) continue;

                const nameEl = child.querySelector('a[aria-label]') ||
                               child.querySelector('.fontHeadlineSmall') ||
                               child.querySelector('[data-jc]');
                const name = nameEl ? (nameEl.getAttribute('aria-label') || nameEl.innerText || '').trim() : '';

                const spans = child.querySelectorAll('.fontBodyMedium span');
                let address = '';
                for (const sp of spans) {
                    const t = sp.innerText.trim();
                    if (t.length > 10 && /\\d/.test(t) && !t.includes('(') && !t.includes('★')) {
                        address = t;
                        break;
                    }
                }

                const ratingEl = child.querySelector('.MW4etd') || child.querySelector('span[role="img"]');
                let rating = 0;
                let reviews = 0;
                if (ratingEl) {
                    const rText = ratingEl.innerText || ratingEl.getAttribute('aria-label') || '';
                    const rMatch = rText.match(/(\\d+\\.?\\d*)/);
                    if (rMatch) rating = parseFloat(rMatch[1]);
                }
                const reviewEl = child.querySelector('.UY7F9');
                if (reviewEl) {
                    const revText = reviewEl.innerText.replace(/[^\\d]/g, '');
                    if (revText) reviews = parseInt(revText);
                }

                const linkEl = child.querySelector('a[href*="/maps/place/"]');
                const placeUrl = linkEl ? linkEl.href : '';

                if (name) {
                    results.push({
                        name: name.substring(0, 100),
                        address: address.substring(0, 150),
                        rating,
                        reviews,
                        place_url: placeUrl.substring(0, 500)
                    });
                }
            }
            return results;
        }''', top_n)

        if not items_data:
            return 20, []

        rank = _find_rank(items_data, business_name)

        # אם לא מצאנו, חפש ב-20 הראשונים
        if rank == 20 and len(items_data) < 20:
            all_items = await page.evaluate('''() => {
                const feed = document.querySelector('div[role="feed"]');
                if (!feed) return [];
                return Array.from(feed.children).slice(0, 20).map(c => {
                    const nameEl = c.querySelector('a[aria-label]') ||
                                   c.querySelector('.fontHeadlineSmall');
                    return {name: nameEl ? (nameEl.getAttribute('aria-label') || nameEl.innerText || '').trim() : ''};
                }).filter(t => t.name.length > 0);
            }''')
            rank = _find_rank(all_items, business_name)

        businesses = items_data[:top_n]
        return rank, businesses

    except Exception as e:
        print(f"  Error parsing results: {e}", file=sys.stderr)
        return 20, []


def _mock_rank():
    """מצב דמו"""
    weights = [0.05, 0.08, 0.10, 0.10, 0.10, 0.08, 0.08, 0.07, 0.06, 0.05,
               0.04, 0.04, 0.03, 0.03, 0.02, 0.02, 0.01, 0.01, 0.01, 0.02]
    r = random.choices(range(1, 21), weights=weights)[0]
    mock_biz = []
    names = ["Joe's HVAC", "Cool Air Pros", "Duct Masters", "AirFlow Inc", "CleanVent Co"]
    for i in range(5):
        mock_biz.append({
            'name': names[i],
            'address': f'{100+i*10} Main St, City, ST',
            'rating': round(random.uniform(3.5, 5.0), 1),
            'reviews': random.randint(10, 500),
            'place_url': ''
        })
    return r, mock_biz


# ── עובד Subprocess — מריץ batch של נקודות בתהליך נפרד ──

async def _run_batch_async(keyword, business_name, points):
    """מריץ batch של נקודות בדפדפן אחד — context אחד, cookies נשמרים"""
    results = []
    keyword_url = '+'.join(keyword.strip().split())

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=BROWSER_ARGS)
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={'width': 1280, 'height': 720},
            locale='en-US',
            timezone_id='America/New_York',
            geolocation={'latitude': points[0]['lat'], 'longitude': points[0]['lng']},
            permissions=['geolocation'],
        )
        page = await context.new_page()
        await page.route('**/*.{png,jpg,jpeg,gif,webp,svg,mp4,woff,woff2}',
                        lambda route: route.abort())
        await page.route('**/recaptcha/**', lambda route: route.abort())

        try:
            for i, point in enumerate(points):
                # עדכן geolocation
                await context.set_geolocation(
                    {'latitude': point['lat'], 'longitude': point['lng']}
                )

                url = f"https://www.google.com/maps/search/{keyword_url}/@{point['lat']},{point['lng']},13z?hl=en"
                try:
                    await page.goto(url, timeout=20000, wait_until='domcontentloaded')

                    # בנקודה הראשונה — חכה יותר וטפל בהסכמה עם JS
                    if i == 0:
                        await asyncio.sleep(3)
                        # טיפול בהסכמה עם JavaScript בלבד (ללא :has-text)
                        consent_result = await page.evaluate('''() => {
                            // חפש כפתורי הסכמה
                            const buttons = document.querySelectorAll('button');
                            for (const btn of buttons) {
                                const text = (btn.innerText || '').toLowerCase().trim();
                                if (text === 'accept all' || text === 'reject all' ||
                                    text === 'i agree' || text === 'agree' ||
                                    text === 'accept' || text === 'consent') {
                                    btn.click();
                                    return 'clicked: ' + text;
                                }
                            }
                            // חפש גם בטפסי consent
                            const form = document.querySelector('form[action*="consent"]');
                            if (form) {
                                const submitBtn = form.querySelector('button');
                                if (submitBtn) { submitBtn.click(); return 'clicked consent form button'; }
                            }
                            return 'no consent dialog found';
                        }''')
                        print(f"  [consent] {consent_result}", file=sys.stderr)
                        await asyncio.sleep(1)
                    else:
                        await asyncio.sleep(random.uniform(2.0, 3.0))

                    # דיבוג: בדוק מצב הדף
                    page_state = await page.evaluate('''() => {
                        const feed = document.querySelector('div[role="feed"]');
                        return {
                            hasFeed: !!feed,
                            feedChildren: feed ? feed.children.length : 0,
                            url: window.location.href.substring(0, 80),
                            title: document.title.substring(0, 50),
                        };
                    }''')
                    print(f"  [point {i+1}/{len(points)}] feed={page_state['hasFeed']}, children={page_state['feedChildren']}", file=sys.stderr)

                    rank, businesses = await _extract_top_businesses(page, business_name, top_n=5)
                    print(f"  [point {i+1}/{len(points)}] ({point['lat']:.4f},{point['lng']:.4f}) → rank={rank}, biz={len(businesses)}", file=sys.stderr)
                except Exception as e:
                    print(f"  [point {i+1}/{len(points)}] ERROR at ({point['lat']},{point['lng']}): {e}", file=sys.stderr)
                    rank, businesses = 20, []

                results.append({
                    'point': point,
                    'rank': rank,
                    'businesses': businesses
                })
                await asyncio.sleep(random.uniform(0.3, 0.8))
        finally:
            await browser.close()

    return results


def _run_batch_direct(keyword, business_name, points):
    """
    מריץ batch ישירות בתהליך הנוכחי עם asyncio.run().
    debug-scrape הוכיח שזה עובד — subprocess לא.
    """
    try:
        print(f"  [direct] Running batch of {len(points)} points...", flush=True)
        results = asyncio.run(_run_batch_async(keyword, business_name, points))
        print(f"  [direct] Batch done, got {len(results)} results", flush=True)
        return results
    except Exception as e:
        print(f"  [direct] Batch failed: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return [{'point': p, 'rank': 20, 'businesses': []} for p in points]


def _get_rank_value(row):
    """מחלץ ערך rank משורת DB — תומך גם ב-dict וגם ב-tuple"""
    if isinstance(row, dict):
        return row['rank']
    try:
        return row['rank']
    except (TypeError, KeyError):
        return row[0]


def run_scan_sync(scan_id: int, business_name: str, keyword: str,
                  grid_points: list, db_path: str,
                  already_done: int = 0, total_override: int = None):
    """
    מריץ סריקה באמצעות subprocess לכל batch של נקודות.
    כל batch רץ בתהליך נפרד — הזיכרון משתחרר לחלוטין בין batches.
    already_done: כמה נקודות כבר הושלמו (להמשך סריקה)
    total_override: סה"כ נקודות (כולל שהושלמו) — לחישוב אחוזים
    """
    from database import get_db

    conn = get_db()

    try:
        total = total_override or len(grid_points)
        remaining = len(grid_points)
        print(f"\n🔍 {'Resuming' if already_done else 'Starting'} scan #{scan_id}: '{keyword}' for '{business_name}'")
        print(f"   Points: {remaining} remaining out of {total}, batch size: {BATCH_SIZE}\n", flush=True)

        if not PLAYWRIGHT_AVAILABLE:
            rank_sum = 0
            completed = already_done
            for point in grid_points:
                rank, businesses = _mock_rank()
                _save_result(conn, scan_id, point, rank, businesses)
                rank_sum += rank
                completed += 1
                conn.execute("UPDATE scans SET status=? WHERE id=?",
                            (f'running:{completed}/{total}', scan_id))
                conn.commit()
            # חשב ממוצע על כל התוצאות
            all_ranks = conn.execute(
                "SELECT rank FROM scan_results WHERE scan_id=?", (scan_id,)
            ).fetchall()
            avg_rank = round(sum(_get_rank_value(r) for r in all_ranks) / len(all_ranks), 1) if all_ranks else 20
            conn.execute(
                '''UPDATE scans SET status='done', avg_rank=?, completed_at=CURRENT_TIMESTAMP
                   WHERE id=?''', (avg_rank, scan_id))
            conn.commit()
            print(f"\n✅ Scan #{scan_id} done (mock). Avg rank: {avg_rank}")
            return

        # ── חלק לbatches והרץ במקביל ──
        rank_sum = 0
        completed = already_done

        # חלק את כל הנקודות ל-batches
        all_batches = []
        for batch_start in range(0, remaining, BATCH_SIZE):
            all_batches.append(grid_points[batch_start:batch_start + BATCH_SIZE])

        total_batches = len(all_batches)
        print(f"  📦 {total_batches} batches, running {MAX_PARALLEL} in parallel", flush=True)

        # הרץ batches ברצף (subprocess אחד בכל פעם)
        for batch_idx, batch_points in enumerate(all_batches):
            print(f"\n  🚀 Batch {batch_idx + 1}/{total_batches} ({len(batch_points)} points)", flush=True)

            batch_results = _run_batch_direct(keyword, business_name, batch_points)

            for result in batch_results:
                point = result['point']
                rank = result['rank']
                businesses = result.get('businesses', [])

                print(f"    📍 ({point['lat']:.4f},{point['lng']:.4f}) → rank {rank} ({len(businesses)} biz)")

                _save_result(conn, scan_id, point, rank, businesses)
                rank_sum += rank
                completed += 1

            conn.execute("UPDATE scans SET status=? WHERE id=?",
                        (f'running:{completed}/{total}', scan_id))
            conn.commit()
            print(f"  ✅ Batch {batch_idx + 1} done. Progress: {completed}/{total}", flush=True)
            gc.collect()

        # חשב ממוצע על כל התוצאות (כולל מסריקה קודמת אם זה resume)
        all_ranks = conn.execute(
            "SELECT rank FROM scan_results WHERE scan_id=?", (scan_id,)
        ).fetchall()
        avg_rank = round(sum(_get_rank_value(r) for r in all_ranks) / len(all_ranks), 1) if all_ranks else 20
        conn.execute(
            '''UPDATE scans SET status='done', avg_rank=?, completed_at=CURRENT_TIMESTAMP
               WHERE id=?''', (avg_rank, scan_id))
        conn.commit()
        print(f"\n✅ Scan #{scan_id} done. Avg rank: {avg_rank}")

    except Exception as e:
        print(f"❌ Scan #{scan_id} failed: {e}")
        import traceback
        traceback.print_exc()
        conn.execute("UPDATE scans SET status='error' WHERE id=?", (scan_id,))
        conn.commit()
    finally:
        conn.close()


# ── נקודת כניסה לתהליך-בן (subprocess worker) ──

if __name__ == '__main__' and '--batch-worker' in sys.argv:
    """
    מצב עובד: מקבל JSON מ-stdin, מריץ batch, מחזיר JSON ל-stdout.
    כל הזיכרון משתחרר כשהתהליך מסתיים.
    """
    input_data = json.loads(sys.stdin.read())
    keyword = input_data['keyword']
    business_name = input_data['business_name']
    points = input_data['points']

    loop = asyncio.new_event_loop()
    results = loop.run_until_complete(_run_batch_async(keyword, business_name, points))
    loop.close()

    print(json.dumps(results), flush=True)


def _save_result(conn, scan_id, point, rank, businesses):
    """שומר תוצאה בודדת למסד הנתונים"""
    cursor = conn.execute(
        '''INSERT INTO scan_results (scan_id, lat, lng, grid_row, grid_col, rank)
           VALUES (?, ?, ?, ?, ?, ?)''',
        (scan_id, point['lat'], point['lng'],
         point['row'], point['col'], rank)
    )
    result_id = cursor.lastrowid

    for idx, biz in enumerate(businesses):
        conn.execute(
            '''INSERT INTO scan_result_businesses
               (scan_result_id, position, name, address, rating, reviews, place_url)
               VALUES (?, ?, ?, ?, ?, ?, ?)''',
            (result_id, idx + 1, biz.get('name', ''), biz.get('address', ''),
             biz.get('rating', 0), biz.get('reviews', 0), biz.get('place_url', ''))
        )
    conn.commit()
