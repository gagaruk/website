import random
import math
import h3
from datetime import datetime, timedelta
from zapocalypse.main import H3Manager, supabase, h3_resolutions


def generate_gaussian_point(mean_lng, mean_lat, std_lng, std_lat):
    """Replicates the SQL Box-Muller logic for realistic clustering"""
    u1, u2 = random.random(), random.random()
    mag = math.sqrt(-2.0 * math.log(u1))
    lng = mean_lng + mag * math.cos(2.0 * math.pi * u2) * std_lng
    lat = mean_lat + mag * math.sin(2.0 * math.pi * u2) * std_lat
    return lng, lat

def seed_world():
    # --- SECTORS (Res 5) ---
    sectors = []
    for i in range(1, 1001):
        lng, lat = generate_gaussian_point(28.97, 41.01, 0.1, 0.05)
        # We calculate H3 directly in Python
        geo = H3Manager.latlng_to_hierarchy(lat, lng, h3_resolutions["sector"]) 
        sectors.append({
            "name": f"Sector {i}",
            "h3_res5": geo["h3_primary"],
            "population": random.randint(5000, 800000),
            "area_sqkm": round(random.uniform(5.0, 300.0), 2),
            "coords": f"POINT({lng} {lat})"
        
        })

    # --- RESOURCES (Res 9) ---
    resources = []
    types = ['Raspberry Pi Node', 'OpenWRT Gear', 'LiDAR Sensor', 'Battery Bank', 'Trauma Kit', 'Machine Shop']
    for _ in range(5000):
        # Cluster logic (flipping between two points)
        m_lng, m_lat = (28.97, 41.01) if random.random() > 0.5 else (29.05, 41.08)
        lng, lat = generate_gaussian_point(m_lng, m_lat, 0.02, 0.01)
        
        geo = H3Manager.latlng_to_hierarchy(lat, lng, h3_resolutions["resource"])
        resources.append({
            "type": random.choice(types),
            "h3_res9": geo["h3_primary"],
            "parent_sector": geo["h3_parent"],
            "coords": f"POINT({lng} {lat})"
        })

    # --- BATCH UPSERT TO SUPABASE ---
    supabase.table("sectors").insert(sectors).execute()
    supabase.table("resources").insert(resources).execute()
    print("World Seeded with H3 Indices!")

def seed_hordes():
    horde_history = []
    for h_id in range(1, 201):
        start_count = random.randint(100, 2000) if random.random() < 0.8 else random.randint(10000, 150000)
        start_lng, start_lat = 28.2 + random.random() * 1.5, 40.8 + random.random() * 0.6
        
        for step in range(10):
            timestamp = datetime.now() - timedelta(minutes=step * 10)
            # Add drift
            lng = start_lng + (step * (random.uniform(-0.001, 0.001)))
            lat = start_lat + (step * (random.uniform(-0.001, 0.001)))
            
            # Important: Get the H3 for the current step's position
            geo = H3Manager.latlng_to_hierarchy(lat, lng, h3_resolutions["horde"])
            
            horde_history.append({
                "horde_id": h_id,
                "est_count": int(start_count * random.uniform(0.98, 1.02)),
                "h3_res8": geo["h3_primary"],
                "parent_sector": geo["h3_parent"],
                "coords": f"POINT({lng} {lat})",
                "timestamp": timestamp.isoformat()
            })
    
    supabase.table("hordes").insert(horde_history).execute()

if __name__ == "__main__":
    seed_world()
    seed_hordes()