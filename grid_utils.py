import math
import requests

def generate_grid(center_lat: float, center_lng: float, grid_size: int, spacing_km: float) -> list:
    """
    יוצר רשת של נקודות GPS סביב מרכז העסק.
    grid_size: 5, 7, 9 וכו'
    spacing_km: מרחק בין נקודות בק"מ
    """
    half = grid_size // 2
    # המרה מק"מ לדרגות
    lat_per_km = 1.0 / 111.0
    lng_per_km = 1.0 / (111.0 * math.cos(math.radians(center_lat)))

    points = []
    for row in range(grid_size):
        for col in range(grid_size):
            lat_offset = (row - half) * spacing_km * lat_per_km
            lng_offset = (col - half) * spacing_km * lng_per_km
            points.append({
                'lat': round(center_lat + lat_offset, 6),
                'lng': round(center_lng + lng_offset, 6),
                'row': row,
                'col': col
            })

    return points


def geocode_address(address: str):
    """
    מחזיר (lat, lng) לפי כתובת - חינמי דרך Nominatim של OpenStreetMap
    """
    try:
        url = "https://nominatim.openstreetmap.org/search"
        params = {
            'q': address,
            'format': 'json',
            'limit': 1,
            'countrycodes': 'us'
        }
        headers = {'User-Agent': 'BuzzRankTracker/1.0'}
        r = requests.get(url, params=params, headers=headers, timeout=10)
        data = r.json()
        if data:
            return float(data[0]['lat']), float(data[0]['lon'])
    except Exception as e:
        print(f"Geocoding error: {e}")
    return None, None


def get_rank_color(rank: int) -> str:
    """צבע לפי מיקום (לשימוש בפרונטאנד)"""
    if rank <= 3:   return '#1a9c3e'   # ירוק כהה
    if rank <= 7:   return '#5cb85c'   # ירוק בהיר
    if rank <= 10:  return '#f0ad4e'   # צהוב
    if rank <= 14:  return '#e67e22'   # כתום
    if rank <= 17:  return '#e74c3c'   # אדום
    return '#8e1a0e'                    # אדום כהה (לא נמצא)
