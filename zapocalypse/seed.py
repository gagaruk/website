import random
import math
import h3
from datetime import datetime, timedelta, timezone
from zapocalypse.main import H3Manager, supabase
 
# ── H3 Resolutions (corrected from spreadsheet) ──────────────────────────────
# NOTE: update h3_resolutions in zapocalypse/main.py to match these values.
# Old values were sector=5, horde=8, resource=9 — all wrong per the spec sheet.
#
#   Unit          Res   Avg Area    Edge
#   Zone/Region    5    252  km²    9.8  km   ← parent rollup target
#   Sector         7      5.1 km²   1.4  km   ← was 5, now correct
#   Horde          9      0.1 km²   0.17 km   ← was 8, now correct
#   Resource      11    0.002 km²   0.02 km   ← was 9, now correct
 
H3_RES = {"parent": 5, "sector": 7, "horde": 9, "resource": 11}
 
# ── World constants ───────────────────────────────────────────────────────────
# Res-7 cell area = 5.1 km² → population = density × 5.1
# Resource density = 1 per 1,000 people (from spreadsheet)
SECTOR_AREA_KM2     = 5.1
RESOURCE_PER_PERSON = 1 / 1000
 
# Istanbul center
CTR_LNG, CTR_LAT = 28.97, 41.01
 
# Zone definitions: density band (people/km²), sector count, Gaussian spread
# std values are in degrees; at 41°N: 1° lng ≈ 83 km, 1° lat ≈ 111 km
# Urban:    tight cluster around center
# Suburban: medium ring outward
# Rural:    wide sparse spread
ZONES = {
    "urban":    {"density": (15_000, 30_000), "count": 150, "std_lng": 0.04, "std_lat": 0.03},
    "suburban": {"density": ( 3_000,  7_000), "count": 350, "std_lng": 0.14, "std_lat": 0.10},
    "rural":    {"density": (     50,    500), "count": 500, "std_lng": 0.45, "std_lat": 0.30},
}
 
RESOURCE_TYPES = [
    "Raspberry Pi Node", "OpenWRT Gear", "LiDAR Sensor",
    "Battery Bank", "Trauma Kit", "Machine Shop",
]
 
 
# ── Helpers ───────────────────────────────────────────────────────────────────
 
def safe_gaussian(mean_lng: float, mean_lat: float,
                  std_lng: float, std_lat: float) -> tuple[float, float]:
    """Box-Muller with u1=0 guard to prevent math.log(0) crash."""
    while True:
        u1 = random.random()
        if u1 > 0:          # guard: log(0) is undefined
            break
    u2 = random.random()
    mag = math.sqrt(-2.0 * math.log(u1))
    lng = mean_lng + mag * math.cos(2.0 * math.pi * u2) * std_lng
    lat = mean_lat + mag * math.sin(2.0 * math.pi * u2) * std_lat
    return lng, lat
 
 
def chunked_insert(table: str, rows: list, chunk_size: int = 500) -> None:
    """Insert rows in chunks to stay within Supabase payload limits."""
    for i in range(0, len(rows), chunk_size):
        supabase.table(table).insert(rows[i : i + chunk_size]).execute()
 
 
# ── Seeding ───────────────────────────────────────────────────────────────────
 
def seed_world() -> None:
    sectors   = []
    resources = []
    seen_h3   = set()           # prevent duplicate H3 cells in sectors table
 
    for zone_type, cfg in ZONES.items():
        d_lo, d_hi   = cfg["density"]
        std_lng      = cfg["std_lng"]
        std_lat      = cfg["std_lat"]
        target_count = cfg["count"]
        generated    = 0
        max_attempts = target_count * 20    # safety cap against infinite loop
 
        for _ in range(max_attempts):
            if generated >= target_count:
                break
 
            lng, lat = safe_gaussian(CTR_LNG, CTR_LAT, std_lng, std_lat)
            geo = H3Manager.latlng_to_hierarchy(lat, lng, H3_RES["sector"])
            h3_cell = geo["h3_primary"]
 
            if h3_cell in seen_h3:          # skip duplicate cells
                continue
            seen_h3.add(h3_cell)
 
            density    = random.randint(d_lo, d_hi)
            population = int(density * SECTOR_AREA_KM2)
 
            sectors.append({
                "name":         f"{zone_type.capitalize()} Sector {generated + 1}",
                "zone_type":    zone_type,
                "h3_res7":      h3_cell,
                "parent_zone":  geo["h3_parent"],
                "population":   population,
                "area_sqkm":    SECTOR_AREA_KM2,
                "coords":       f"POINT({lng} {lat})",
            })
 
            # Derive resource count from population density
            resource_count = max(1, round(population * RESOURCE_PER_PERSON))
            for _ in range(resource_count):
                r_lng, r_lat = safe_gaussian(lng, lat, 0.005, 0.003)
                r_geo = H3Manager.latlng_to_hierarchy(r_lat, r_lng, H3_RES["resource"])
                resources.append({
                    "type":          random.choice(RESOURCE_TYPES),
                    "h3_res11":      r_geo["h3_primary"],
                    "parent_sector": r_geo["h3_parent"],   # rolls up to res-5 parent
                    "coords":        f"POINT({r_lng} {r_lat})",
                })
 
            generated += 1
 
    print(f"Sectors generated : {len(sectors)}")
    print(f"Resources derived : {len(resources)}")
 
    chunked_insert("sectors",   sectors)
    chunked_insert("resources", resources)
    print("World seeded.")
 
 
def seed_hordes() -> None:
    """
    Horde size bands (from spreadsheet):
      80%  normal    :   50 –  5,000
      18%  large     : 5,000 – 10,000
       2%  mega      : 10,000 – 50,000  (rare; would depopulate a Res-7 sector)
 
    Movement: genuine random walk — each step builds on the previous position,
    so paths are continuous rather than teleporting.
 
    Insert order: oldest step first so the trigger fn_update_horde_latest
    always sees increasing timestamps and leaves horde_latest in correct state.
    """
    horde_history = []
 
    for h_id in range(1, 201):
        roll = random.random()
        if roll < 0.80:
            start_count = random.randint(50, 5_000)
        elif roll < 0.98:
            start_count = random.randint(5_000, 10_000)
        else:
            start_count = random.randint(10_000, 50_000)
 
        # Starting position — spread across Istanbul metro area
        lng = 28.2 + random.random() * 1.5
        lat = 40.8 + random.random() * 0.6
 
        # Generate steps oldest → newest (step 9 = now, step 0 = 90 min ago)
        # We build the list in chronological order so inserts go oldest-first,
        # ensuring the trigger always advances current_t forward.
        steps = []
        for step in range(10):
            # Random walk: each step nudges from the previous position
            lng += random.uniform(-0.001, 0.001)
            lat += random.uniform(-0.001, 0.001)
 
            # step=0 is 90 min ago, step=9 is now
            timestamp = datetime.now(timezone.utc) - timedelta(minutes=(9 - step) * 10)
 
            geo = H3Manager.latlng_to_hierarchy(lat, lng, H3_RES["horde"])
            steps.append({
                "horde_id":      h_id,
                "est_count":     int(start_count * random.uniform(0.98, 1.02)),
                "h3_res9":       geo["h3_primary"],
                "parent_sector": geo["h3_parent"],
                "coords":        f"POINT({lng} {lat})",
                "timestamp":     timestamp.isoformat(),
            })
 
        horde_history.extend(steps)   # oldest first within each horde
 
    print(f"Horde history rows : {len(horde_history)}")
    chunked_insert("hordes", horde_history, chunk_size=500)
    print("Hordes seeded.")
 
 
def verify_seeding() -> None:
    """Sanity check row counts after seeding."""
    tables = ["sectors", "resources", "hordes", "horde_latest"]
    print("\n── Verification ──────────────────────────")
    for table in tables:
        result = supabase.table(table).select("*", count="exact").limit(1).execute()
        print(f"  {table:<15} {result.count:>7} rows")
    print("──────────────────────────────────────────")
 
 
if __name__ == "__main__":
    seed_world()
    seed_hordes()
    verify_seeding()