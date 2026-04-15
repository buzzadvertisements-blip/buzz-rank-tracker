import asyncio
import random
import time
import re

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
    '--window-size=1280,720',
]

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
]

# ── מקבילות ──────────────────────────────────────────────────────────────────
MAX_CONCURRENT = 3  # 3 תהליכים מקבילים


async def _extract_top_businesses(page, business_name: str, top_n: int = 5):
    """
    שולף את Top N עסקים מהתוצאות בגוגל מפות.
    מחזיר (rank, businesses_list)
    rank = מיקום העסק שלנו (1-20, 20=לא נמצא)
    businesses_list = רשימת dict עם name, address, rating, reviews, place_url
    """
    name_words = [w.lower() for w in business_name.split() if len(w) > 2]

    try:
        await page.wait_for_selector('div[role="feed"]', timeout=12000)
    except:
        await asyncio.sleep(4)

    try:
        items_data = await page.evaluate('''(topN) => {
            const feed = document.querySelector('div[role="feed"]');
            if (!feed) return [];

            const results = [];
            const children = Array.from(feed.children);

            for (const child of children.slice(0, 20)) {
                const text = child.innerText || '';
                if (text.trim().length < 5) continue;

                // שם העסק - בדרך כלל הלינק הראשון או ה-heading
                const nameEl = child.querySelector('a[aria-label]') ||
                               child.querySelector('.fontHeadlineSmall') ||
                               child.querySelector('[data-jc]');
                const name = nameEl ? (nameEl.getAttribute('aria-label') || nameEl.innerText || '').trim() : '';

                // כתובת
                const spans = child.querySelectorAll('.fontBodyMedium span');
                let address = '';
                for (const sp of spans) {
                    const t = sp.innerText.trim();
                    if (t.length > 10 && /\\d/.test(t) && !t.includes('(') && !t.includes('★')) {
                        address = t;
                        break;
                    }
                }

                // דירוג וביקורות
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

                // URL
                const linkEl = child.querySelector('a[href*="/maps/place/"]');
                const placeUrl = linkEl ? linkEl.href : '';

                if (name) {
                    results.push({
                        name: name.substring(0, 100),
                        address: address.substring(0, 150),
                        rating,
                        reviews,
                        place_url: placeUrl.substring(0, 500),
                        _text: text.toLowerCase()
                    });
                }

                if (results.length >= topN) break;
            }
            return results;
        }''', top_n)

        if not items_data:
            return 20, []

        # מצא את המיקום של העסק שלנו מתוך כל 20 הראשונים
        rank = 20
        # סרוק גם מעבר ל-top_n כדי למצוא דירוג נכון
        all_items = await page.evaluate('''() => {
            const feed = document.querySelector('div[role="feed"]');
            if (!feed) return [];
            return Array.from(feed.children).slice(0, 20)
                .map(c => (c.innerText || '').toLowerCase())
                .filter(t => t.trim().length > 5);
        }''')

        for i, text in enumerate(all_items):
            matches = sum(1 for w in name_words if w in text)
            if matches >= max(1, len(name_words) // 2):
                rank = i + 1
                break

        # נקה _text מהתוצאות
        businesses = []
        for item in items_data:
            item.pop('_text', None)
            businesses.append(item)

        return rank, businesses

    except Exception as e:
        print(f"  Error parsing results: {e}")
        return 20, []


async def scrape_point(page, lat: float, lng: float, keyword: str, business_name: str):
    """
    בודק נקודת GPS אחת - משתמש בדף קיים (לא פותח דפדפן חדש).
    מחזיר (rank, businesses_list)
    """
    keyword_url = '+'.join(keyword.strip().split())
    url = f"https://www.google.com/maps/search/{keyword_url}/@{lat},{lng},14z?hl=en"

    try:
        await page.goto(url, timeout=30000, wait_until='domcontentloaded')
        await asyncio.sleep(random.uniform(2.0, 3.5))
        rank, businesses = await _extract_top_businesses(page, business_name, top_n=5)
        print(f"  📍 ({lat:.4f},{lng:.4f}) → rank {rank} for '{keyword}' ({len(businesses)} biz)")
        return rank, businesses
    except Exception as e:
        print(f"  ❌ Scrape error at ({lat},{lng}): {e}")
        return 20, []


async def _worker(worker_id: int, queue: asyncio.Queue, results: dict,
                  business_name: str, keyword: str, playwright_instance):
    """
    עובד מקבילי - פותח דפדפן אחד ומעבד מספר נקודות.
    """
    browser = await playwright_instance.chromium.launch(headless=True, args=BROWSER_ARGS)
    try:
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={'width': 1280, 'height': 720},
            locale='en-US',
            timezone_id=random.choice([
                'America/New_York', 'America/Chicago',
                'America/Denver', 'America/Los_Angeles'
            ]),
        )
        page = await context.new_page()

        # חסום תמונות ומשאבים כבדים
        await page.route('**/*.{png,jpg,jpeg,gif,webp,svg,mp4,woff,woff2}',
                        lambda route: route.abort())

        while True:
            try:
                point = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            rank, businesses = await scrape_point(
                page, point['lat'], point['lng'], keyword, business_name
            )
            results[f"{point['row']},{point['col']}"] = {
                'point': point,
                'rank': rank,
                'businesses': businesses
            }

            # השהייה למניעת חסימה - מפוזרת בין העובדים
            await asyncio.sleep(random.uniform(2.0, 4.5))

    except Exception as e:
        print(f"  ❌ Worker {worker_id} error: {e}")
    finally:
        await browser.close()


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


def run_scan_sync(scan_id: int, business_name: str, keyword: str,
                  grid_points: list, db_path: str):
    """
    מריץ סריקה מלאה עם 3 תהליכים מקבילים
    """
    import sqlite3

    async def _run():
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys = ON")

        try:
            total = len(grid_points)
            print(f"\n🔍 Starting scan #{scan_id}: '{keyword}' for '{business_name}'")
            print(f"   Grid: {total} points, {MAX_CONCURRENT} parallel workers\n")

            if not PLAYWRIGHT_AVAILABLE:
                # מצב דמו - ללא מקבילות
                rank_sum = 0
                completed = 0
                for point in grid_points:
                    rank, businesses = _mock_rank()

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
                            (result_id, idx + 1, biz['name'], biz['address'],
                             biz['rating'], biz['reviews'], biz.get('place_url', ''))
                        )

                    rank_sum += rank
                    completed += 1
                    conn.execute("UPDATE scans SET status=? WHERE id=?",
                                (f'running:{completed}/{total}', scan_id))
                    conn.commit()

                avg_rank = round(rank_sum / total, 1)
                conn.execute(
                    '''UPDATE scans SET status='done', avg_rank=?, completed_at=CURRENT_TIMESTAMP
                       WHERE id=?''', (avg_rank, scan_id))
                conn.commit()
                print(f"\n✅ Scan #{scan_id} done (mock). Avg rank: {avg_rank}")
                return

            # ── מצב אמיתי עם Playwright ──
            # ערבב את הנקודות כדי שהעובדים לא יסרקו אזורים צמודים
            shuffled = list(grid_points)
            random.shuffle(shuffled)

            queue = asyncio.Queue()
            for p in shuffled:
                await queue.put(p)

            results = {}

            async with async_playwright() as p:
                workers = [
                    _worker(i, queue, results, business_name, keyword, p)
                    for i in range(MAX_CONCURRENT)
                ]
                # הרץ את העובדים עם עדכוני התקדמות
                tasks = [asyncio.create_task(w) for w in workers]

                # עדכן התקדמות כל כמה שניות
                while not all(t.done() for t in tasks):
                    completed = len(results)
                    conn.execute("UPDATE scans SET status=? WHERE id=?",
                                (f'running:{completed}/{total}', scan_id))
                    conn.commit()
                    await asyncio.sleep(3)

                await asyncio.gather(*tasks)

            # שמור תוצאות למסד הנתונים
            rank_sum = 0
            for key, data in results.items():
                point = data['point']
                rank = data['rank']
                businesses = data['businesses']

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

                rank_sum += rank
                conn.commit()

            avg_rank = round(rank_sum / total, 1) if total > 0 else 20

            conn.execute(
                '''UPDATE scans SET status='done', avg_rank=?, completed_at=CURRENT_TIMESTAMP
                   WHERE id=?''', (avg_rank, scan_id))
            conn.commit()
            print(f"\n✅ Scan #{scan_id} done. Avg rank: {avg_rank}")

        except Exception as e:
            print(f"❌ Scan #{scan_id} failed: {e}")
            conn.execute("UPDATE scans SET status='error' WHERE id=?", (scan_id,))
            conn.commit()
        finally:
            conn.close()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_run())
    loop.close()
