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

@app.get("/")
async def index():
    return{"status":"test_success"}
