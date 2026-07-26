from fastapi import FastAPI, Query
from pydantic import BaseModel, ConfigDict, model_validator, StringConstraints
from datetime import datetime
import h3
from supabase import create_client, Client
from typing import List, Annotated
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    supabase_url: str
    supabase_key: str

    class Config:
        env_file = ".env"

settings = Settings()
supabase: Client = create_client(settings.supabase_url, settings.supabase_key)

app = FastAPI()

h3_resolutions = {"parent": 5, "sector": 7, "horde": 9, "resource": 11}


class SpatialEntityModel(BaseModel):
    lat: float
    lng: float

    model_config = ConfigDict(from_attributes=True)

    @model_validator(mode="before")
    @classmethod
    def int_to_hex_str(cls, values: dict):
        processed={}
        for k, v_raw in values.items():
            if "h3" in k:
                v_processed= h3.int_to_str(v_raw) if isinstance(v_raw, int) else v_raw
                if not isinstance(v_processed, str) or not h3.is_valid_cell(v_processed):
                    raise ValueError(f"Invalid H3 index in field {k}: {v_processed}")
                processed[k] = v_processed
            else:
                processed[k] = v_raw
        return processed

class HordeModel(SpatialEntityModel):
    horde_id:      int | None = None
    est_count:     int
    h3_res9:       str
    parent_sector: str
    timestamp:     datetime | None = None


class SectorModel(SpatialEntityModel):
    sector_id:  int | None = None
    name:       Annotated[str, StringConstraints(max_length=25)]
    population: int
    h3_res7:    str
    area_sqkm:  float


class ResourceModel(SpatialEntityModel):
    type:          str
    h3_res11:      str
    parent_sector: str


class H3Manager:
    @staticmethod
    def latlng_to_hierarchy(lat: float, lng: float, primary_res: int) -> dict:
        h3_primary = h3.latlng_to_cell(lat, lng, primary_res)
        h3_parent  = h3.cell_to_parent(h3_primary, h3_resolutions["parent"])
        return {"h3_primary": h3_primary, "h3_parent": h3_parent}

    @staticmethod
    def get_perimeter_indexes(h3_origin: str, rings: int) -> list[str]:
        return list(h3.grid_disk(h3_origin, rings))

class EntityManager:
    @staticmethod
    def create_sector(name: str, lat: float, lng: float,
                      population: int, area_sqkm: float):
        geo = H3Manager.latlng_to_hierarchy(lat, lng, h3_resolutions["sector"])
        payload = {
            "name":        name,
            "h3_res7":     geo["h3_primary"],
            "parent_zone": geo["h3_parent"],
            "population":  population,
            "area_sqkm":   area_sqkm,
            "coords":      f"POINT({lng} {lat})",
        }
        return supabase.table("sectors").insert(payload).execute()

    @staticmethod
    def create_resource(resource_type: str, lat: float, lng: float):
        geo = H3Manager.latlng_to_hierarchy(lat, lng, h3_resolutions["resource"])
        payload = {
            "type":          resource_type,
            "h3_res11":      geo["h3_primary"],
            "parent_sector": geo["h3_parent"],
            "coords":        f"POINT({lng} {lat})",
        }
        return supabase.table("resources").insert(payload).execute()

    @staticmethod
    def create_horde(horde_id: int | None, lat: float, lng: float, est_count: int):
        geo = H3Manager.latlng_to_hierarchy(lat, lng, h3_resolutions["horde"])
        payload = {
            "est_count":     est_count,
            "h3_res9":       geo["h3_primary"],
            "parent_sector": geo["h3_parent"],
            "coords":        f"POINT({lng} {lat})"
        }
        if horde_id is not None:
            payload["horde_id"] = horde_id

        return supabase.table("hordes").insert(payload).execute()
    
class SimulationManager:
    @staticmethod
    async def get_horde_next_move(horde_id: int, city_id: int, search_rings: int = 5):
        """
        Moves the horde using JSONB path caching. 
        Only hits pg_routing when the path is exhausted or a new target is needed.
        """
        
        horde_res = supabase.table("hordes") \
            .select("h3_res9, target_h3, current_path") \
            .eq("horde_id", horde_id) \
            .single().execute()
            
        if not horde_res.data:
            return {"status": "error", "message": "Horde not found"}
            
        horde = horde_res.data
        current_h3 = horde['h3_res9']
        path_cache = horde['current_path'] or []
        
        if not path_cache:

            nearby_cells = list(h3.grid_disk(current_h3, search_rings))
            
            stats_res = supabase.table("hex_stats") \
                .select("h3_res9, count_s, alpha") \
                .eq("city_id", city_id) \
                .in_("h3_res9", nearby_cells) \
                .execute()
                
            if not stats_res.data:
                return {"status": "idle", "message": "No targets in range"}
                
            best_target = None
            highest_score = -1.0
            
            for cell in stats_res.data:
                t_h3 = cell['h3_res9']
                s_pop = cell['count_s']
                alpha = cell['alpha']
                
                if t_h3 == current_h3 or s_pop == 0: continue
                
                dist = h3.grid_distance(current_h3, t_h3)
                score = s_pop / ((dist ** 2) * (1.0 + alpha))
                
                if score > highest_score:
                    highest_score = score
                    best_target = t_h3
            
            if not best_target:
                return {"status": "idle", "message": "No viable targets"}

            route_res = supabase.rpc("get_path_between_h3", {
                "p_city_id": city_id,
                "p_start_h3": current_h3,
                "p_end_h3": best_target
            }).execute()
            
            if not route_res.data or len(route_res.data) < 2:
                return {"status": "blocked", "message": "No road path to target"}
                
            new_path = []
            for step in route_res.data[1:]:
                coords = step['geom']['coordinates'] # [lng, lat]
                step_h3 = h3.latlng_to_cell(coords[1], coords[0], 9)
                new_path.append(step_h3)
                
            path_cache = new_path
            horde['target_h3'] = best_target

        next_h3 = path_cache.pop(0)
        
        lat, lng = h3.cell_to_latlng(next_h3)
        
        supabase.table("hordes").update({
            "h3_res9": next_h3,
            "coords": f"POINT({lng} {lat})",
            "target_h3": horde['target_h3'],
            "current_path": path_cache 
        }).eq("horde_id", horde_id).execute()
        
        return {
            "status": "moved",
            "current": next_h3,
            "target": horde['target_h3'],
            "steps_remaining": len(path_cache)
        }
    @staticmethod
    def update_sdz_step(s:int, z:int, d:int, params):
        """
        params = {
            'beta': 0.005,  # Infection rate
            'alpha': 0.002, # Combat kill rate
            'phi': 0.0001, # Birthrate (0.01% growth per tick)
            'delta': 0.05,  # Zombie decay (5% die per tick)
            'max_capacity': 500 # Max humans per Res 9 cell
        }
        """
        births = int(s * params['phi']) if s < params['max_capacity'] else 0
        
        new_infections = int(params['beta'] * s * z)
        
        combat_kills = int(params['alpha'] * s * z)
        
        natural_decay = int(z * params['delta'])
        
        next_s = max(0, s + births - new_infections)
        next_z = max(0, z + new_infections - combat_kills - natural_decay)
        next_d = d + combat_kills + natural_decay
        
        return next_s, next_z, next_d

@app.post("/zapocalypse/resource/create")
async def create_resource(entity: ResourceModel):
    result = EntityManager.create_resource(entity.type, entity.lat, entity.lng)
    return {"status": "success", "data": result.data}


@app.post("/zapocalypse/sector/create")
async def create_sector(entity: SectorModel):
    result = EntityManager.create_sector(
        entity.name, entity.lat, entity.lng,
        entity.population, entity.area_sqkm,
    )
    return {"status": "success", "data": result.data}


@app.post("/zapocalypse/horde/create")
async def create_horde(entity: HordeModel):
    result = EntityManager.create_horde(
        entity.horde_id, entity.lat, entity.lng, entity.est_count,
    )
    return {"status": "success", "data": result.data}


@app.post("/zapocalypse/sector/perimeter")
async def surrounding_indexes(
    sectors: List[SectorModel],
    rings: int = Query(default=1, ge=1, le=6),
):
    response = {}
    for sector in sectors:
        h3_neighbours = H3Manager.get_perimeter_indexes(sector.h3_res7, rings)

        neighbours = supabase.table("hordes") \
            .select("*") \
            .in_("parent_sector", h3_neighbours) \
            .execute()

        response[sector.sector_id] = [
            HordeModel(**h) for h in neighbours.data
        ]

    return response

