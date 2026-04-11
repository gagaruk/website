from fastapi import FastAPI, Query
from pydantic import BaseModel, ConfigDict, model_validator, StringConstraints
from datetime import datetime, timezone
import h3
from supabase import create_client, Client
from typing import List, Annotated
from pydantic_settings import BaseSettings


# ── Config ────────────────────────────────────────────────────────────────────

class Settings(BaseSettings):
    supabase_url: str
    supabase_key: str

    class Config:
        env_file = ".env"

settings = Settings()
supabase: Client = create_client(settings.supabase_url, settings.supabase_key)

app = FastAPI()

# ── H3 Resolutions (matches seed.py and spreadsheet spec) ────────────────────
#
#   Unit          Res   Avg Area    Edge
#   Zone/Region    5    252  km²    9.8  km   ← parent rollup
#   Sector         7      5.1 km²   1.4  km
#   Horde          9      0.1 km²   0.17 km
#   Resource      11    0.002 km²   0.02 km

h3_resolutions = {"parent": 5, "sector": 7, "horde": 9, "resource": 11}


# ── Models ────────────────────────────────────────────────────────────────────

class SpatialEntityModel(BaseModel):
    lat: float          # fixed typo: was `lan`
    lng: float

    model_config = ConfigDict(from_attributes=True)

    @model_validator(mode="before")
    @classmethod
    def int_to_hex_str(cls, values: dict):
        return {
            k: h3.int_to_str(v) if "h3" in k and isinstance(v, int) else v
            for k, v in values.items()
        }


class HordeModel(SpatialEntityModel):
    horde_id:      int
    est_count:     int
    h3_res9:       str          # updated: was h3_res8
    parent_sector: str
    timestamp:     datetime


class SectorModel(SpatialEntityModel):
    sector_id:  int | None = None
    name:       Annotated[str, StringConstraints(max_length=25)]
    population: int
    h3_res7:    str             # updated: was h3_res5
    area_sqkm:  float


class ResourceModel(SpatialEntityModel):
    # resource_id removed — DB generates it; no reason to require it on creation
    type:          str
    h3_res11:      str          # updated: was h3_res9
    parent_sector: str


# ── H3Manager ─────────────────────────────────────────────────────────────────

class H3Manager:
    @staticmethod
    def latlng_to_hierarchy(lat: float, lng: float, primary_res: int) -> dict:
        h3_primary = h3.latlng_to_cell(lat, lng, primary_res)
        h3_parent  = h3.cell_to_parent(h3_primary, h3_resolutions["parent"])
        return {"h3_primary": h3_primary, "h3_parent": h3_parent}

    @staticmethod
    def get_perimeter_indexes(h3_origin: str, rings: int) -> list[str]:
        return list(h3.grid_disk(h3_origin, rings))


# ── EntityManager ─────────────────────────────────────────────────────────────

class EntityManager:
    @staticmethod
    def create_sector(name: str, lat: float, lng: float,
                      population: int, area_sqkm: float):
        geo = H3Manager.latlng_to_hierarchy(lat, lng, h3_resolutions["sector"])
        payload = {
            "name":        name,
            "h3_res7":     geo["h3_primary"],   # updated column name
            "parent_zone": geo["h3_parent"],
            "population":  population,
            "area_sqkm":   area_sqkm,
            "coords":      f"POINT({lng} {lat})",
        }
        return supabase.table("sectors").insert(payload).execute()

    @staticmethod
    def create_resource(resource_type: str, lat: float, lng: float):
        # Renamed param to resource_type to avoid shadowing the builtin `type`
        geo = H3Manager.latlng_to_hierarchy(lat, lng, h3_resolutions["resource"])
        payload = {
            "type":          resource_type,
            "h3_res11":      geo["h3_primary"],   # updated column name
            "parent_sector": geo["h3_parent"],
            "coords":        f"POINT({lng} {lat})",
        }
        return supabase.table("resources").insert(payload).execute()

    @staticmethod
    def create_horde(horde_id: int, lat: float, lng: float, est_count: int):
        geo = H3Manager.latlng_to_hierarchy(lat, lng, h3_resolutions["horde"])
        payload = {
            "horde_id":      horde_id,
            "est_count":     est_count,
            "h3_res9":       geo["h3_primary"],   # updated column name
            "parent_sector": geo["h3_parent"],
            "coords":        f"POINT({lng} {lat})",
            "timestamp":     datetime.now(timezone.utc).isoformat(),  # tz-aware
        }
        return supabase.table("hordes").insert(payload).execute()


# ── Endpoints ─────────────────────────────────────────────────────────────────

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
    return {"status": "success", "data": result.data}   # was missing return


@app.post("/zapocalypse/sector/perimeter")
async def surrounding_indexes(
    sectors: List[SectorModel],
    rings: int = Query(default=1, ge=1, le=6),  # configurable, was hardcoded
):
    response = {}
    for sector in sectors:
        h3_neighbours = H3Manager.get_perimeter_indexes(sector.h3_res7, rings)

        neighbours = supabase.table("hordes") \
            .select("*") \
            .in_("parent_sector", h3_neighbours) \
            .execute()

        response[sector.sector_id] = [        # fixed: was sector.id
            HordeModel(**h) for h in neighbours.data
        ]

    return response