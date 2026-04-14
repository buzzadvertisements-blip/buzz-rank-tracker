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
]


async def _get_rank_from_page(page, business_name: str) -> int:
    """מחפש את העסק ברשימת התוצאות ומחזיר את המיקום שלו"""
    # מילים משמעותיות משם העסק לחיפוש
    name_words = [w.lower() for w in business_name.split() if len(w) > 2]

    try:
        # המתן לתפריט תוצאות
        await page.wait_for_selector('div[role="feed"]', timeout=12000)
    except:
        # ניסיון חלופי
        await asyncio.sleep(4)

    try:
        # שלוף את כל הפריטים ברשימה
        items_text = await page.evaluate('''() => {
            const feed = document.querySelector('div[role="feed"]');
            if (!feed) {
                // ניסיון חלופי - כל הכרטיסיות
                const cards = document.querySelectorAll('[jsaction*="mouseover"]');
                return Array.from(cards).slice(0, 25).map(c => c.innerText);
            }
            const results = [];
            const children = Array.from(feed.children);
            for (const child of children.slice(0, 25)) {
                const text = child.innerText || '';
                if (text.trim().length > 5) {
                    results.push(text.toLowerCase());
                }
            }
            return results;
        }''')

        if not items_text:
            return 20

        for i, text in enumerate(items_text):
            # בדוק אם מספר מילים מהשם מופיעות בטקסט
            matches = sum(1 for w in name_words if w in text)
            if matches >= max(1, len(name_words) // 2):
                return i + 1

    except Exception as e:
        print(f"  Error parsing results: {e}")

    return 20  # לא נמצא בטופ 20


async def scrape_rank(lat: float, lng: float, keyword: str, business_name: str) -> int:
    """
    בודק את הדירוג של העסק בגוגל מפות מנקודת GPS ספציפית.
    מחזיר 1-20 (20 = לא נמצא בטופ 20)
    """
    if not PLAYWRIGHT_AVAILABLE:
        return _mock_rank()

    keyword_url = '+'.join(keyword.strip().split())
    url = f"https://www.google.com/maps/search/{keyword_url}/@{lat},{lng},14z?hl=en"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=BROWSER_ARGS)
        try:
            context = await browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                viewport={'width': 1280, 'height': 720},
                locale='en-US',
                timezone_id='America/New_York',
            )
            page = await context.new_page()

            # חסום תמונות ומשאבים כבדים להאצה
            await page.route('**/*.{png,jpg,jpeg,gif,webp,svg,mp4,woff,woff2}',
                           lambda route: route.abort())

            await page.goto(url, timeout=30000, wait_until='domcontentloaded')

            # השהייה אנושית אקראית
            await asyncio.sleep(random.uniform(2.0, 3.5))

            rank = await _get_rank_from_page(page, business_name)
            print(f"  📍 ({lat:.4f},{lng:.4f}) → rank {rank} for '{keyword}'")
            return rank

        except Exception as e:
            print(f"  ❌ Scrape error at ({lat},{lng}): {e}")
            return 20
        finally:
            await browser.close()


def _mock_rank() -> int:
    """מצב דמו - מחזיר דירוגים אקראיים לבדיקת ממשק"""
    weights = [0.05, 0.08, 0.10, 0.10, 0.10, 0.08, 0.08, 0.07, 0.06, 0.05,
               0.04, 0.04, 0.03, 0.03, 0.02, 0.02, 0.01, 0.01, 0.01, 0.02]
    import random as r
    return r.choices(range(1, 21), weights=weights)[0]


def run_scan_sync(scan_id: int, business_name: str, keyword: str,
                  grid_points: list, db_path: str):
    """
    מריץ סריקה מלאה בצינור סינכרוני (נקרא מthreading.Thread)
    """
    import sqlite3

    async def _run():
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys = ON")

        try:
            total = len(grid_points)
            rank_sum = 0
            completed = 0

            print(f"\n🔍 Starting scan #{scan_id}: '{keyword}' for '{business_name}'")
            print(f"   Grid: {total} points\n")

            for point in grid_points:
                rank = await scrape_rank(
                    point['lat'], point['lng'],
                    keyword, business_name
                )

                conn.execute(
                    '''INSERT INTO scan_results (scan_id, lat, lng, grid_row, grid_col, rank)
                       VALUES (?, ?, ?, ?, ?, ?)''',
                    (scan_id, point['lat'], point['lng'],
                     point['row'], point['col'], rank)
                )
                conn.commit()

                rank_sum += rank
                completed += 1

                # עדכן סטטוס התקדמות
                conn.execute(
                    "UPDATE scans SET status=? WHERE id=?",
                    (f'running:{completed}/{total}', scan_id)
                )
                conn.commit()

                # השהייה למניעת חסימה
                if PLAYWRIGHT_AVAILABLE:
                    await asyncio.sleep(random.uniform(3.0, 6.0))

            avg_rank = round(rank_sum / total, 1)

            conn.execute(
                '''UPDATE scans SET status='done', avg_rank=?, completed_at=CURRENT_TIMESTAMP
                   WHERE id=?''',
                (avg_rank, scan_id)
            )
            conn.commit()
            print(f"\n✅ Scan #{scan_id} done. Avg rank: {avg_rank}")

        except Exception as e:
            print(f"❌ Scan #{scan_id} failed: {e}")
            conn.execute(
                "UPDATE scans SET status='error' WHERE id=?", (scan_id,)
            )
            conn.commit()
        finally:
            conn.close()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_run())
    loop.close()
