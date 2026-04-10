from fastapi import FastAPI
from pydantic import BaseModel, ConfigDict, model_validator, StringConstraints
from datetime import datetime
import h3
from supabase import create_client, Client
from typing import List, Annotated

supabase_url:str = "URL"
supabase_key:str = "KEY"
supabase:Client =  create_client(supabase_url, supabase_key)

app = FastAPI()

h3_resolutions= {"parent":5, "sector":5, "horde":8, "resource":9}

#////////////////////////////////////////////////////////////////////////////////////////////7

class SpatialEntityModel(BaseModel):
    lan:float
    lng:float

    model_config= ConfigDict(from_attributes=True)
    @model_validator(mode="before")
    @classmethod
    def int_to_hex_str(cls, values: dict):
        return {
            k: h3.int_to_str(v) if "h3" in k and isinstance(v, int) else v
            for k, v in values.items()
    }
    
class HordeModel(SpatialEntityModel):
    horde_id:int
    est_count:int
    h3_res8:str
    parent_sector:str
    timestamp:datetime

class SectorModel(SpatialEntityModel):
    sector_id:int | None = None
    name:Annotated[str, StringConstraints(max_length=25)]
    population:int
    h3_res5:str
    area_sqkm:float

class ResourceModel(SpatialEntityModel):
    resource_id:int
    type:str
    h3_res9:str
    parent_sector:str

#////////////////////////////////////////////////////////////////////////////

class H3Manager:
    @staticmethod
    def latlng_to_hierarchy(lat:float, lng:float, primary_res:int):
        h3_primary = h3.latlng_to_cell(lat, lng, primary_res)
        h3_parent = h3.cell_to_parent(h3_primary, h3_resolutions["parent"])

        return{
            "h3_primary": h3_primary,
            "h3_parent": h3_parent
        }
    
    @staticmethod
    def get_perimeter_indexes(h3_origin:str, rings:int):
        neighbors= h3.grid_disk(h3_origin, rings)
        return[h for h in neighbors]
    
class EntityManager:
    @staticmethod
    def create_sector(name: str, lat: float, lng: float, population: int, area_sqkm:float):
        # Sectors live at Resolution 5
        geo = H3Manager.latlng_to_hierarchy(lat, lng, h3_resolutions["sector"])
        
        payload = {
            "name": name,
            "h3_res5": geo["h3_primary"],
            "population": population,
            "area_sqkm": area_sqkm,
            "coords": f"POINT({lng} {lat})"
        }
        
        return supabase.table("sectors").insert(payload).execute()

    @staticmethod
    def create_resource(type: str, lat: float, lng: float):
        # Resources live at Resolution 9
        geo = H3Manager.latlng_to_hierarchy(lat, lng, h3_resolutions["resource"])
        
        payload = {
            "type": type,
            "h3_res9": geo["h3_primary"],
            "parent_sector": geo["h3_parent"], # Derived from Res 9
            "coords": f"POINT({lng} {lat})"
        }
        
        return supabase.table("resources").insert(payload).execute()

    @staticmethod
    def create_horde(horde_id: int, lat: float, lng: float, est_count: int):
        # Hordes live at Resolution 8
        geo = H3Manager.latlng_to_hierarchy(lat, lng, h3_resolutions["horde"])
        
        payload = {
            "horde_id": horde_id,
            "est_count": est_count,
            "h3_res8": geo["h3_primary"],
            "parent_sector": geo["h3_parent"], # Derived from Res 8
            "coords": f"POINT({lng} {lat})",
            "timestamp": datetime.now().isoformat()
        }
        
        # This insert will trigger your 'func_update_horde_latest' in Postgres
        return supabase.table("hordes").insert(payload).execute()
    
#//////////////////////////////////////////////////////////////////

@app.post("/zapocalypse/resource/create")
async def create_resource(entity:ResourceModel):
    result = EntityManager.create_resource(entity.type, entity.lat, entity.lng)
    return {"status": "success", "data": result.data}

@app.post("/zapocalypse/sector/create")
async def create_sector(entity:SectorModel):
    result = EntityManager.create_sector(entity.name, entity.lat, entity.lng, entity.population, entity.area_sqkm)
    return {"status": "success", "data": result.data}

@app.post("/zapocalypse/horde/create")
async def create_horde(entity:HordeModel):
    result = EntityManager.create_horde(entity.horde_id, entity.lat, entity.lng, entity.est_count)

@app.post("/zapocalypse/sector/perimeter")
async def surrounding_indexes(sectors: List[SectorModel]):
    response = {}
    for sector in sectors:
        h3_neighbours = H3Manager.get_perimeter_indexes(sector.h3_res5, rings=1)
        
        neighbours = supabase.table("hordes") \
            .select("*") \
            .in_("parent_sector", h3_neighbours) \
            .execute()
        
        response[sector.id] = [HordeModel(**h) for h in neighbours.data]
    
    return response 

