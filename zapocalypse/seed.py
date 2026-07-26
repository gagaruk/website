import os
import math
import random
import h3
import osmnx as ox
import geopandas as gpd
from fastapi import FastAPI, BackgroundTasks, HTTPException
from supabase import create_client, Client
from shapely.geometry import mapping, Polygon
from datetime import datetime, timedelta, timezone
import uvicorn

app = FastAPI(title="Z-Apocalypse Dynamic Engine")

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("WARNING: SUPABASE_URL or SUPABASE_KEY not set. DB operations will fail.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

SUPABASE_PAYLOAD_CEILING_BYTES = 3 * 1024 * 1024
ROW_BYTES = {
    "hex_stats":  242,
    "edge_to_h3": 118,
    "sectors": 150,
    "resources": 100,
    "hordes": 120
}

H3_RES = {"parent": 5, "sector": 7, "horde": 9, "resource": 11}
SECTOR_AREA_KM2 = 5.1
RESOURCE_PER_PERSON = 1 / 1_000

ZONES = {
    "urban":    {"density": (15_000, 30_000), "count": 150, "std_lng": 0.04, "std_lat": 0.03},
    "suburban": {"density": ( 3_000,  7_000), "count": 350, "std_lng": 0.14, "std_lat": 0.10},
    "rural":    {"density": (     50,    500), "count": 500, "std_lng": 0.45, "std_lat": 0.30},
}

RESOURCE_TYPES = [
    "Raspberry Pi Node", "OpenWRT Gear", "LiDAR Sensor",
    "Battery Bank", "Trauma Kit", "Machine Shop",
]

HORDE_COUNT = 200
LOG_COUNT = 10

def calculate_batch_size(table: str, total_rows: int, city_hex_count: int) -> int:
    row_bytes = ROW_BYTES.get(table, 200)
    base = max(100, int(SUPABASE_PAYLOAD_CEILING_BYTES / row_bytes))

    if city_hex_count < 2_000: scale = 1.00
    elif city_hex_count < 10_000: scale = 0.80
    elif city_hex_count < 30_000: scale = 0.60
    elif city_hex_count < 80_000: scale = 0.40
    else: scale = 0.25

    return max(100, min(int(base * scale), 5_000))

def report_status(city_slug: str, stage: str, pct: int, error: str = None) -> None:
    try:
        supabase.table("pipeline_status").upsert(
            {
                "city_slug":  city_slug,
                "stage":      stage,
                "pct":        pct,
                "error":      error,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
            on_conflict="city_slug",
        ).execute()
    except Exception as e:
        print(f"  [status] Failed to update pipeline_status: {e}")

def safe_gaussian(mean_lng: float, mean_lat: float, std_lng: float, std_lat: float) -> tuple[float, float]:
    while True:
        u1 = random.random()
        if u1 > 0: break
    u2  = random.random()
    mag = math.sqrt(-2.0 * math.log(u1))
    lng = mean_lng + mag * math.cos(2.0 * math.pi * u2) * std_lng
    lat = mean_lat + mag * math.sin(2.0 * math.pi * u2) * std_lat
    return lng, lat

def latlng_to_hierarchy(lat: float, lng: float, primary_res: int) -> dict:
    h3_primary = h3.latlng_to_cell(lat, lng, primary_res)
    h3_parent = h3.cell_to_parent(h3_primary, H3_RES["parent"])
    return {"h3_primary": h3_primary, "h3_parent": h3_parent}


def fetch_and_prune_city(city_name: str):
    print(f"[{city_name}] Fetching road network...")
    G = ox.graph_from_place(city_name, network_type="drive")
    try:
        G_connected = ox.truncate.largest_component(G, strongly=True)
    except AttributeError:
        G_connected = ox.utils_graph.get_largest_component(G, strongly=True)

    boundary_gdf = ox.geocode_to_gdf(city_name)
    boundary_poly = boundary_gdf.geometry.iloc[0]
    return G_connected, boundary_poly

def generate_h3_grid(boundary_poly: Polygon) -> list[str]:
    print("Generating H3 grid...")
    geojson_poly = mapping(boundary_poly)
    return list(h3.geo_to_cells(geojson_poly, H3_RES["horde"]))

def calculate_szd_parameters(city_name: str, hex_strings: list[str]) -> list[dict]:
    print(f"[{city_name}] Fetching POIs and calculating SZD...")
    try:
        pois = ox.features_from_place(city_name, tags={
            "amenity": ["police", "hospital", "fire_station"],
            "natural":  ["water", "wood"],
            "building": True,
        })
    except Exception:
        pois = gpd.GeoDataFrame()

    hex_polys = gpd.GeoDataFrame(
        {"h3_str": hex_strings},
        geometry=[Polygon([(lng, lat) for lat, lng in h3.cell_to_boundary(h)]) for h in hex_strings],
        crs="EPSG:4326",
    )

    hex_data_list = []
    if pois.empty:
        return [{"h3_res9": h3.str_to_int(h), "count_s": 10, "count_z": 0, "count_d": 0, "terrain_type": "urban", "alpha": 0.001, "beta": 0.005, "delta": 0.010, "phi": 0.0001} for h in hex_strings]

    hex_polys_m = hex_polys.to_crs("EPSG:3857")
    pois_m = pois.to_crs("EPSG:3857")
    joined = gpd.sjoin(pois_m, hex_polys_m, how="left", predicate="intersects")
    grouped = {k: v for k, v in joined.groupby("h3_str")} if not joined.empty else {}

    for hex_str in hex_strings:
        h3_int = h3.str_to_int(hex_str)
        local = grouped.get(hex_str, gpd.GeoDataFrame())
        alpha, beta, delta, terrain, base_s = 0.001, 0.005, 0.010, "urban", 10

        if not local.empty:
            if "amenity" in local.columns:
                alpha += local["amenity"].isin(["police", "hospital"]).sum() * 0.005
            if "building" in local.columns:
                buildings = local[local["building"].notna()]
                building_m2 = buildings.geometry.area.sum() if not buildings.empty else 0.0
                beta = 0.005 * (1.0 + (building_m2 / 10_000.0))
                base_s = int(building_m2 / 20) if building_m2 > 0 else 10
            if "natural" in local.columns:
                water_area = local.loc[local["natural"] == "water", "geometry"].area.sum()
                if water_area > 0:
                    delta = min(0.010 + (water_area / 500_000.0), 0.030)
                    terrain = "coastal"

        hex_data_list.append({
            "h3_res9": h3_int, "count_s": base_s, "count_z": 0, "count_d": 0,
            "terrain_type": terrain, "alpha": round(alpha, 6), "beta": round(beta, 6),
            "delta": round(delta, 6), "phi": 0.0001 if terrain == "urban" else 0.00005,
        })
    return hex_data_list

def build_edge_to_h3_mapping(G, hex_strings: list[str]) -> list[dict]:
    print("Building H3-to-edge bridge...")
    G_4326 = ox.project_graph(G, to_crs="EPSG:4326")
    _, gdf_edges = ox.graph_to_gdfs(G_4326)
    gdf_edges = gdf_edges.reset_index()

    hex_polys = gpd.GeoDataFrame(
        {"h3_str": hex_strings, "h3_int": [h3.str_to_int(h) for h in hex_strings]},
        geometry=[Polygon([(lng, lat) for lat, lng in h3.cell_to_boundary(h)]) for h in hex_strings],
        crs="EPSG:4326",
    )
    joined = gpd.sjoin(gdf_edges, hex_polys, how="inner", predicate="intersects")
    return [{"source": int(row["u"]), "target": int(row["v"]), "h3_res9": int(row["h3_int"])} for _, row in joined.iterrows()]

def chunked_upsert(table: str, rows: list[dict], batch_size: int, conflict_col: str, city_slug: str, stage_label: str) -> None:
    total = len(rows)
    for i in range(0, total, batch_size):
        chunk = rows[i : i + batch_size]
        supabase.table(table).upsert(chunk, on_conflict=conflict_col).execute()
        report_status(city_slug, stage_label, min(100, int(((i + len(chunk)) / total) * 100)))
    if total == 0: report_status(city_slug, stage_label, 100)

def chunked_insert(table: str, rows: list, city_hex_count: int, city_slug: str, stage_label: str) -> None:
    total = len(rows)
    batch_size = calculate_batch_size(table, total, city_hex_count)
    for i in range(0, total, batch_size):
        chunk = rows[i : i + batch_size]
        supabase.table(table).insert(chunk).execute()
        report_status(city_slug, stage_label, min(100, int(((i + len(chunk)) / total) * 100)))
    if total == 0: report_status(city_slug, stage_label, 100)

def push_base_twin_to_supabase(city_slug: str, hex_data: list, bridge_data: list, city_hex_count: int) -> str:
    print(f"[{city_slug}] Registering city...")
    city_res = supabase.table("cities").upsert(
        {"slug": city_slug, "display_name": city_slug.replace("-", " ").title()},
        on_conflict="slug"
    ).execute()
    city_id = city_res.data[0]["id"]

    for row in hex_data: row["city_id"] = city_id
    for row in bridge_data: row["city_id"] = city_id

    hex_batch = calculate_batch_size("hex_stats", len(hex_data), city_hex_count)
    edge_batch = calculate_batch_size("edge_to_h3", len(bridge_data), city_hex_count)

    chunked_upsert("hex_stats", hex_data, hex_batch, "city_id,h3_res9", city_slug, "upsert_hex_stats")
    chunked_upsert("edge_to_h3", bridge_data, edge_batch, "city_id,source,target,h3_res9", city_slug, "upsert_edge_to_h3")
    
    return city_id

def seed_world_dynamic(city_id: str, center_lng: float, center_lat: float, city_slug: str, hex_count: int):
    print(f"[{city_slug}] Seeding Sectors and Resources...")
    sectors, resources, seen_h3 = [], [], set()

    for zone_type, cfg in ZONES.items():
        d_lo, d_hi, std_lng, std_lat, target_count = cfg["density"][0], cfg["density"][1], cfg["std_lng"], cfg["std_lat"], cfg["count"]
        generated = 0

        for _ in range(target_count * 20):
            if generated >= target_count: break

            lng, lat = safe_gaussian(center_lng, center_lat, std_lng, std_lat)
            geo = latlng_to_hierarchy(lat, lng, H3_RES["sector"])
            h3_cell = geo["h3_primary"]

            if h3_cell in seen_h3: continue
            seen_h3.add(h3_cell)

            population = int(random.randint(d_lo, d_hi) * SECTOR_AREA_KM2)
            neighbor_zones = list(h3.grid_disk(geo["h3_parent"], 1))

            sectors.append({
                "city_id": city_id,
                "name": f"{zone_type.capitalize()} Sector {generated + 1}",
                "zone_type": zone_type,
                "h3_res7": h3_cell,
                "parent_zone": geo["h3_parent"],
                "neighbor_zones": neighbor_zones,
                "population": population,
                "area_sqkm": SECTOR_AREA_KM2,
                "coords": f"POINT({lng} {lat})"
            })

            for _ in range(max(1, round(population * RESOURCE_PER_PERSON))):
                r_lng, r_lat = safe_gaussian(lng, lat, 0.005, 0.003)
                r_geo = latlng_to_hierarchy(r_lat, r_lng, H3_RES["resource"])
                resources.append({
                    "city_id": city_id,
                    "type": random.choice(RESOURCE_TYPES),
                    "h3_res11": r_geo["h3_primary"],
                    "parent_sector": r_geo["h3_parent"],
                    "coords": f"POINT({r_lng} {r_lat})"
                })
            generated += 1

    chunked_insert("sectors", sectors, hex_count, city_slug, "seed_sectors")
    chunked_insert("resources", resources, hex_count, city_slug, "seed_resources")

def seed_hordes_dynamic(city_id: str, center_lng: float, center_lat: float, city_slug: str, hex_count: int):
    print(f"[{city_slug}] Seeding Hordes...")
    horde_history = []

    for h_id in range(1, HORDE_COUNT + 1):
        roll = random.random()
        if roll < 0.80: start_count = random.randint(50, 5_000)
        elif roll < 0.98: start_count = random.randint(5_000, 10_000)
        else: start_count = random.randint(10_000, 50_000)

        lng, lat = safe_gaussian(center_lng, center_lat, 0.2, 0.2)
        
        steps = []
        for step in range(LOG_COUNT):
            lng += random.uniform(-0.001, 0.001)
            lat += random.uniform(-0.001, 0.001)
            minutes_ago = (LOG_COUNT - 1 - step) * 10
            timestamp = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
            geo = latlng_to_hierarchy(lat, lng, H3_RES["horde"])
            
            steps.append({
                "city_id": city_id,
                "horde_id": h_id,
                "est_count": int(start_count * random.uniform(0.98, 1.02)),
                "h3_res9": geo["h3_primary"],
                "parent_sector": geo["h3_parent"],
                "coords": f"POINT({lng} {lat})",
                "timestamp": timestamp.isoformat(),
            })
        horde_history.extend(steps)

    chunked_insert("hordes", horde_history, hex_count, city_slug, "seed_hordes")
    
    try:
        supabase.rpc("set_horde_sequence", {"start_val": HORDE_COUNT + 1}).execute()
    except Exception as e:
        print(f"  [Warning] Could not reset horde sequence: {e}")

def run_ingestion_pipeline(city_name: str) -> None:
    slug = city_name.lower().replace(" ", "-")
    try:

        report_status(slug, "spatial", 0)
        G, boundary = fetch_and_prune_city(city_name)
        hex_strings = generate_h3_grid(boundary)
        city_hex_count = len(hex_strings)
        
        center_lng, center_lat = boundary.centroid.x, boundary.centroid.y
        report_status(slug, "spatial", 100)

        report_status(slug, "szd", 0)
        hex_data = calculate_szd_parameters(city_name, hex_strings)
        report_status(slug, "szd", 100)

        report_status(slug, "bridge", 0)
        bridge_data = build_edge_to_h3_mapping(G, hex_strings)
        report_status(slug, "bridge", 100)

        report_status(slug, "upsert_hex_stats", 0)
        city_id = push_base_twin_to_supabase(slug, hex_data, bridge_data, city_hex_count)

        report_status(slug, "seed_sectors", 0)
        seed_world_dynamic(city_id, center_lng, center_lat, slug, city_hex_count)
        
        report_status(slug, "seed_hordes", 0)
        seed_hordes_dynamic(city_id, center_lng, center_lat, slug, city_hex_count)

        report_status(slug, "complete", 100)
        print(f"[{city_name}] Digital twin AND Population seeding complete.")

    except Exception as e:
        print(f"[{city_name}] Pipeline failed: {str(e)}")
        report_status(slug, "failed", 0, error=str(e))


@app.post("/sim/initialize/{city_name}")
async def initialize_city(city_name: str, background_tasks: BackgroundTasks):
    background_tasks.add_task(run_ingestion_pipeline, city_name)
    return {
        "status":  "processing",
        "message": f"Building digital twin and populating initial state for {city_name}.",
        "slug":    city_name.lower().replace(" ", "-"),
    }

@app.get("/sim/status/{city_slug}")
async def city_status(city_slug: str):
    res = supabase.table("pipeline_status").select("*").eq("city_slug", city_slug).maybe_single().execute()
    if not res.data:
        raise HTTPException(status_code=404, detail=f"No pipeline found for '{city_slug}'")
    return res.data

if __name__ == "__main__":
    print("Starting Z-Apocalypse Ingestion & Seeding Engine...")
    uvicorn.run(app, host="0.0.0.0", port=8000)