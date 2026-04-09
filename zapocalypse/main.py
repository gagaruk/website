from fastapi import FastAPI
from pydantic import BaseModel
import h3

from supabase import Client

app = FastAPI()

class HordeObservation(BaseModel):
    horde_id: int
    est_count: int
    lat: int
    lng: int

@app.post("/horde/update")
async def update_horde(observation: HordeObservation):
    h3_cell = h3.latlng_to_h3(observation.lat, observation.lng, 9)

    return {"status": "success", "h3_index": h3_cell}


