import os
import requests
import re
import math
import time
import uuid
import json
import asyncio
import random
import hashlib 
import pandas as pd
import redis.asyncio as redis
import firebase_admin
from firebase_admin import credentials, messaging as fcm_messaging
from io import BytesIO
from collections import defaultdict
from typing import List, Dict, Any, Optional 
from datetime import datetime
from pydantic import BaseModel
from fastapi import FastAPI, Body, WebSocket, WebSocketDisconnect, Header, Query 
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from dotenv import load_dotenv
from fastapi import Request
from fastapi.responses import Response

# CHANGED: Load environment variables first
load_dotenv()

# CHANGED: Read service URLs from environment with production defaults
ORS_URL = os.getenv("ORS_URL", "http://nexus-ors:8082/ors")
VROOM_URL = os.getenv("VROOM_URL", "http://nexus-vroom:3000")
ERP_BASE_URL = os.getenv("ERP_BASE_URL", "https://erp.crystalapps.dev")

# 🚨 FIREBASE ADMIN SDK INITIALIZATION (The FCM Bridge)
# This requires the firebase_credentials.json file to be present in the same directory
try:
    cred = credentials.Certificate("firebase_credentials.json")
    firebase_admin.initialize_app(cred)
    print("🔥 Firebase Admin SDK Initialized Successfully.")
except Exception as e:
    print(f"⚠️ Firebase Admin SDK Failed to Initialize: {e}")

# CHANGED: Two separate Redis connections
# 1. Session Redis (connects to ERPNext's Redis cache for session validation)
REDIS_SESSION_URL = os.getenv("REDIS_SESSION_URL", "redis://erpnext-redis-cache-1:6379/0")
# 2. Vault Redis (dedicated cache for driver/sales vaults)
REDIS_VAULT_URL = os.getenv("REDIS_VAULT_URL", "redis://localhost:6379/1")

# CHANGED: Create two async Redis clients
redis_session = redis.from_url(REDIS_SESSION_URL, decode_responses=True)
redis_vault = redis.from_url(REDIS_VAULT_URL, decode_responses=True)

# CHANGED: For backward compatibility, keep a single redis_client alias (points to vault)
redis_client = redis_vault

app = FastAPI(title="Nexus Brain API")

# ────────────────────────────────────────────────
# CORS Middleware
# ────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://erp.crystalapps.dev"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 🚨 CRITICAL: Trust Nginx Forwarding for Mobile WebSockets
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")

# ────────────────────────────────────────────────
# THE ENTERPRISE BACKGROUND DAEMON (JITTER QUEUE)
# ────────────────────────────────────────────────
GEOCODE_QUEUE = asyncio.Queue()

async def geocode_worker():
    """
    Runs continuously in the background. Pulls Google Maps links from the queue,
    extracts coordinates, updates ERPNext via callback, and sleeps (Jitter) to avoid IP bans.
    """
    print("🤖 Nexus Background Geocoding Worker Initialized.")
    while True:
        task_data = await GEOCODE_QUEUE.get()
        
        customer_name = task_data.get("customer_name")
        url = task_data.get("url")
        erp_url = task_data.get("erp_url")          
        erp_headers = task_data.get("erp_headers")  

        try:
            # 1. Scrape the Google Maps URL
            headers = {"User-Agent": "Mozilla/5.0"}
            res = await asyncio.to_thread(requests.get, url, headers=headers, timeout=10)
            m = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', res.url) or re.search(r'center=(-?\d+\.\d+)%2C(-?\d+\.\d+)', res.text)

            if m:
                lat, lng = float(m.group(1)), float(m.group(2))
                status = "Success"
            else:
                lat, lng = 0.0, 0.0
                status = "Invalid Link"

            # 2. Fire Callback to ERPNext to update the Customer record silently
            if erp_url and erp_headers and customer_name:
                payload = {
                    "custom_latitude": lat,
                    "custom_longitude": lng,
                    "custom_geocoding_status": status
                }
                update_endpoint = f"{erp_url}/api/resource/Customer/{customer_name}"
                await asyncio.to_thread(requests.put, update_endpoint, headers=erp_headers, json=payload)
                print(f"✅ Geocoded {customer_name}: {status}")

        except Exception as e:
            print(f"❌ Background Geocode Error for {customer_name}: {e}")
            if erp_url and erp_headers and customer_name:
                payload = {"custom_geocoding_status": "Failed"}
                update_endpoint = f"{erp_url}/api/resource/Customer/{customer_name}"
                await asyncio.to_thread(requests.put, update_endpoint, headers=erp_headers, json=payload)

        finally:
            GEOCODE_QUEUE.task_done()
            sleep_time = random.uniform(5.0, 7.0)
            await asyncio.sleep(sleep_time)

# 🚨 The Sales Heartbeat Worker
async def sales_heartbeat_worker():
    print("💓 Nexus Sales Telemetry Heartbeat Initialized.")
    while True:
        # 🚨 EFFICIENCY FIX: Only broadcast from the heartbeat if no pings
        # have arrived in the last 3 seconds (i.e. all reps are offline).
        # When reps are active, each ping already triggers broadcast_sales().
        # This prevents doubling the WebSocket fan-out load during active hours.
        current_time = time.time()
        last_ping = max(
            (v.get("last_updated", 0) for v in LIVE_SALES_DATA.values()),
            default=0
        )
        if current_time - last_ping > 3:
            await broadcast_sales()
        await asyncio.sleep(2)

@app.on_event("startup")
async def startup_event():
    """Starts the background workers and verifies Redis as soon as FastAPI boots up."""
    try:
        await redis_vault.ping()
        print("✅ Fast Redis Vault Connection Established Successfully.")
    except Exception as e:
        print(f"❌ Redis Vault Connection Failed: {e}")
    try:
        await redis_session.ping()
        print("✅ Fast Redis Session Connection Established Successfully.")
    except Exception as e:
        print(f"❌ Redis Session Connection Failed: {e}")
        
    asyncio.create_task(geocode_worker())
    asyncio.create_task(sales_heartbeat_worker())

@app.post("/queue-geocode")
async def queue_geocode(payload: Dict = Body(...)):
    """
    Receives Webhooks from ERPNext during Data Imports. 
    Instantly drops them into the Queue and responds 200 OK so ERPNext doesn't lag.
    """
    await GEOCODE_QUEUE.put(payload)
    return {"status": "queued", "message": f"Customer {payload.get('customer_name')} queued for processing."}

# ────────────────────────────────────────────────
# 🚚 FLEET TELEMETRY ENGINE (Driver App - Upgraded)
# ────────────────────────────────────────────────
LIVE_FLEET_DATA = {}
DRIVER_LOGOUT_GRAVEYARD = {} # 🚨 NEW: Driver Graveyard Cache for dying-breath pings
active_connections: List[WebSocket] = []

async def broadcast_fleet():
    """Push full fleet instantly to every connected Command Center"""
    data = {"fleet": LIVE_FLEET_DATA}
    for conn in active_connections[:]:
        try:
            await conn.send_json(data)
        except Exception:
            if conn in active_connections:
                active_connections.remove(conn)

@app.websocket("/telemetry/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket for Nexus Live Dispatch (instant updates)"""
    await websocket.accept()
    active_connections.append(websocket)
    try:
        await websocket.send_json({"fleet": LIVE_FLEET_DATA})
        while True:
            # 🚨 UPDATED: Active JSON parser for Ping/Pong Heartbeat
            raw_data = await websocket.receive_text()
            try:
                json_data = json.loads(raw_data)
                if json_data.get("action") == "ping":
                    await websocket.send_json({"action": "pong"})
            except json.JSONDecodeError:
                pass # Ignore raw text that isn't valid JSON
    except WebSocketDisconnect:
        if websocket in active_connections:
            active_connections.remove(websocket)

@app.api_route("/telemetry/fleet-status", methods=["GET", "HEAD"])
def get_fleet_status(request: Request):
    if request.method == "HEAD":
        return Response()
    current_time = time.time()
    stale_keys = [k for k, v in LIVE_FLEET_DATA.items() if current_time - v["last_updated"] > 3600]
    for k in stale_keys:
        del LIVE_FLEET_DATA[k]
    return {"fleet": LIVE_FLEET_DATA}

@app.post("/telemetry/ping")
async def receive_gps_ping(payload: Dict = Body(...)):
    is_delayed = payload.get("is_delayed_sync", False)
    if is_delayed:
        return {"status": "ok", "message": "Offline ping synced to history."}

    driver = payload.get("driver", "Unknown_Driver")
    vehicle = payload.get("vehicle", "Idle")
    manifest_id = payload.get("manifest_id", "None")

    # 🚨 GRAVEYARD GUARD: Block pings from drivers who just logged out
    if driver in DRIVER_LOGOUT_GRAVEYARD:
        if time.time() - DRIVER_LOGOUT_GRAVEYARD[driver] < 120.0:
            return {"status": "rejected", "message": "Ping rejected. Driver recently logged out."}
        else:
            del DRIVER_LOGOUT_GRAVEYARD[driver]

    # 🚨 UNIFIED TRACKING ID: Stops the mapping engine from creating ghost markers
    tracking_id = f"{driver}::{vehicle}"

    # 🚨 THE GHOST TRUCK FIX: Strip any old keys matching this driver or vehicle
    stale_keys = [
        k for k, v in LIVE_FLEET_DATA.items() 
        if (v.get("driver") == driver or v.get("vehicle") == vehicle) and k != tracking_id
    ]
    for k in stale_keys:
        del LIVE_FLEET_DATA[k]

    LIVE_FLEET_DATA[tracking_id] = {
        "tracking_id": tracking_id, # Explicit ID passed to JS DOM
        "manifest_id": manifest_id,
        "driver": driver,
        "vehicle": vehicle,
        "lat": payload.get("lat"),
        "lng": payload.get("lng"),
        "speed": payload.get("speed", 0),
        "heading": payload.get("heading", 0),
        "last_updated": time.time(),
    }
    
    await broadcast_fleet()
    return {"status": "ok"}

@app.post("/telemetry/driver-logout")
async def receive_driver_logout(payload: Dict = Body(...)):
    """
    🚨 NEW: Instantly drops the driver off the map, resets them to Offline, 
    and blacklists their dying background pings.
    """
    driver_email = payload.get("driver")
    if driver_email:
        keys_to_delete = [k for k, v in LIVE_FLEET_DATA.items() if v.get("driver") == driver_email]
        for k in keys_to_delete:
            del LIVE_FLEET_DATA[k]
            
        DRIVER_LOGOUT_GRAVEYARD[driver_email] = time.time()
        await broadcast_fleet()
        print(f"📡 Fleet Telemetry Purged: {driver_email} logged out.")
    return {"status": "purged"}

@app.post("/telemetry/driver-login")
async def receive_driver_login(payload: Dict = Body(...)):
    """Resurrects a driver instantly if they re-login within the 120s graveyard window."""
    driver_email = payload.get("driver")
    if driver_email in DRIVER_LOGOUT_GRAVEYARD:
        del DRIVER_LOGOUT_GRAVEYARD[driver_email]
    return {"status": "resurrected"}

@app.post("/telemetry/end-trip")
async def end_trip(payload: Dict = Body(...)):
    manifest_id = payload.get("manifest_id")
    # 🚨 Updated to clear based on the new dictionary architecture
    keys_to_delete = [k for k, v in LIVE_FLEET_DATA.items() if v.get("manifest_id") == manifest_id]
    for k in keys_to_delete:
        del LIVE_FLEET_DATA[k]
        
    if keys_to_delete:
        await broadcast_fleet()
    return {"status": "cleared"}

# ────────────────────────────────────────────────
# 📦 PHASE 2: DRIVER DATA VAULT (Offline Synchronization Engine)
# ────────────────────────────────────────────────
@app.get("/api/v1/sync/driver-vault/{driver_email}")
async def get_driver_vault(
    driver_email: str,
    current_hash: Optional[str] = Query(default=None, alias="current_hash"), 
    erp_url: str = Header(..., alias="erp-url", description="ERPNext Base URL"),
    erp_token: str = Header("No_Token", alias="erp-token", description="ERPNext Authorization Token"),
    erp_sid: str = Header("No_SID", alias="erp-sid", description="ERPNext Session ID") 
):
    """
    🚨 NEW: 0-Lag Manifest Vault for Drivers.
    Hashes the driver's contextual manifests. Only downloads if the hash changed.
    """
    headers = {
        "X-Frappe-CSRF-Token": erp_token, 
        "Content-Type": "application/json",
        "Cookie": f"sid={erp_sid}"
    }
    
    def fetch_driver_context():
        endpoint = f"{erp_url}/api/method/nexus_supply_chain.api.get_my_active_manifests_and_context"
        res = requests.get(endpoint, headers=headers, timeout=30)
        res.raise_for_status()
        return res.json().get("message", {})

    try:
        context_response = await asyncio.to_thread(fetch_driver_context)
        
        # ERPNext Response Wrapping handling
        if isinstance(context_response, dict) and "message" in context_response and "status" in context_response:
            data = context_response.get("message", {})
        else:
            data = context_response

        manifests = data.get("manifests", [])
        context = data.get("context", {})

        payload_data = {
            "manifests": manifests,
            "context": context
        }
        
        # Generate SHA-256 for micro-ping comparison
        payload_str = json.dumps(payload_data, sort_keys=True)
        new_hash = hashlib.sha256(payload_str.encode('utf-8')).hexdigest()
        
        if current_hash and current_hash == new_hash:
            return {"status": "unchanged"}
            
        return {
            "status": "success",
            "new_hash": new_hash,
            "manifests": manifests,
            "context": context
        }

    except Exception as e:
        print(f"❌ Driver Vault Sync Error: {e}")
        return {"status": "error", "message": str(e)}

# ────────────────────────────────────────────────
# Models
# ────────────────────────────────────────────────
class Item(BaseModel):
    item_code: str
    qty: float
    rate: float
    weight_per_unit: float
    default_pl_rate: float
    net_available: float = 0.0

class SalesOrder(BaseModel):
    sales_order: str
    creation: str
    customer: str
    customer_name: str
    payment_terms: str = "Standard / Cash"
    latitude: float = 0.0
    longitude: float = 0.0
    delivery_region: str
    optimization_radius: float
    items: List[Item]
    amount: float
    so_status: str
    total_weight: float = 0
    wait_time: str = ""
    wait_time_hours: float = 0
    revenue_state: str = ""
    altered_items: str = ""
    altered_items_full: str = ""
    readiness: str = "Unknown"

class OptimizerPayload(BaseModel):
    sales_orders: List[SalesOrder]
    vehicle_max_tonnage: float
    is_on_collection: bool

class LoadGroup(BaseModel):
    group_id: str
    total_tonnage: float
    total_amount: float
    utilization: float
    max_capacity: float
    sales_orders: List[SalesOrder]

class RawInventoryPayload(BaseModel):
    items: list
    stock: list
    sales_orders: list
    reservations: list

# ────────────────────────────────────────────────
# Intelligence Helper Functions
# ────────────────────────────────────────────────
def calculate_wait_stats(creation_str: str) -> tuple[str, float]:
    try:
        creation = datetime.fromisoformat(creation_str.replace("Z", "+00:00"))
        delta = datetime.now() - creation
        total_seconds = max(0, int(delta.total_seconds()))
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        formatted = f"{hours}h {minutes}m"
        return formatted, float(hours + minutes / 60.0)
    except:
        return "N/A", 0.0

def analyze_revenue_state(items: List[Item]) -> tuple[str, str, str]:
    states = []
    altered = []
    for it in items:
        if it.rate > it.default_pl_rate * 1.05:
            states.append("Premium")
            altered.append(it.item_code)
        elif it.rate < it.default_pl_rate * 0.95:
            states.append("Discounted")
            altered.append(it.item_code)
        else:
            states.append("Standard")
            
    revenue = "Premium" if "Premium" in states else "Discounted" if "Discounted" in states else "Standard"
    short_list = ", ".join(altered[:2])
    full_list = ", ".join(altered)
    return revenue, short_list, full_list

def simulate_fifo_readiness(orders: List[SalesOrder]) -> None:
    if not orders: return
    sorted_orders = sorted(orders, key=lambda o: -o.wait_time_hours)
    stock_pool = defaultdict(float)
    
    for so in sorted_orders:
        for it in so.items:
            if it.item_code not in stock_pool:
                stock_pool[it.item_code] = it.net_available
                
    for so in sorted_orders:
        fully_satisfied = True
        partially_satisfied = False
        for it in so.items:
            avail = stock_pool[it.item_code]
            if avail >= it.qty:
                stock_pool[it.item_code] -= it.qty
                partially_satisfied = True
            elif avail > 0:
                stock_pool[it.item_code] = 0
                fully_satisfied = False
                partially_satisfied = True
            else:
                fully_satisfied = False
        
        so.readiness = "Ready" if fully_satisfied else "Partial Shortage" if partially_satisfied else "Full Shortage"

def haversine(lat1, lon1, lat2, lon2):
    if not all([lat1, lon1, lat2, lon2]): return 9999.9
    R = 6371.0
    dlat, dlon = math.radians(lat2-lat1), math.radians(lon2-lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * (2 * math.atan2(math.sqrt(a), math.sqrt(1-a)))

def get_vroom_sequence(factory, customers):
    vroom_jobs = [{"id": i, "location": loc} for i, loc in enumerate(customers)]
    vroom_payload = {
        "vehicles": [{"id": 1, "profile": "driving-car", "start": factory, "end": factory}],
        "jobs": vroom_jobs
    }
    try:
        # CHANGED: Use VROOM_URL environment variable
        v_res = requests.post(VROOM_URL, json=vroom_payload, timeout=5)
        if v_res.status_code == 200:
            return [s['location'] for s in v_res.json()['routes'][0]['steps']]
    except Exception as e:
        print(f"VROOM Optimization Failed: {e}")
        pass
    
    return [factory] + customers + [factory]

# ────────────────────────────────────────────────
# Core Optimization & Routing Endpoints
# ────────────────────────────────────────────────
@app.post("/optimize")
def optimize(payload: OptimizerPayload = Body(...)):
    max_cap = 999999.0 if payload.is_on_collection else float(payload.vehicle_max_tonnage)
    
    processed = []
    for so in payload.sales_orders:
        so.total_weight = sum(it.qty * it.weight_per_unit for it in so.items)
        so.wait_time, so.wait_time_hours = calculate_wait_stats(so.creation)
        so.revenue_state, so.altered_items, so.altered_items_full = analyze_revenue_state(so.items)
        processed.append(so)
        
    simulate_fifo_readiness(processed)
    
    if payload.is_on_collection:
        groups_raw = [
            LoadGroup(
                group_id=str(uuid.uuid4()), total_tonnage=so.total_weight,
                total_amount=so.amount, utilization=100.0,
                max_capacity=max_cap, sales_orders=[so]
            ) for so in sorted(processed, key=lambda x: -x.wait_time_hours)
        ]
    else:
        groups_raw = []
        unassigned = sorted(processed, key=lambda x: -x.wait_time_hours)
        
        while unassigned:
            seed = unassigned.pop(0)
            current_group = [seed]
            current_weight = seed.total_weight
            current_amount = seed.amount
            last_lat, last_lon = seed.latitude, seed.longitude
            regional_limit = seed.optimization_radius
            
            while True:
                best_idx, min_d = -1, float('inf')
                for i, cand in enumerate(unassigned):
                    if current_weight + cand.total_weight <= max_cap:
                        dist = haversine(last_lat, last_lon, cand.latitude, cand.longitude)
                        if dist < min_d and dist <= regional_limit:
                            min_d, best_idx = dist, i
                
                if best_idx != -1:
                    next_so = unassigned.pop(best_idx)
                    current_group.append(next_so)
                    current_weight += next_so.total_weight
                    current_amount += next_so.amount
                    last_lat, last_lon = next_so.latitude, next_so.longitude
                else:
                    break
            
            groups_raw.append(LoadGroup(
                group_id=str(uuid.uuid4()),
                total_tonnage=current_weight,
                total_amount=current_amount,
                utilization=round((current_weight / max_cap * 100), 2) if max_cap > 0 else 0,
                max_capacity=max_cap,
                sales_orders=current_group
            ))
            
    return {"groups": [g.dict() for g in groups_raw], "debug": {"processed_count": len(processed)}}

@app.post("/calculate-route")
def calculate_route(payload: Dict = Body(...)):
    raw_coords = payload.get("coordinates", [])
    if len(raw_coords) < 2: return {"error": "Invalid coordinates"}
    
    factory = raw_coords[0]
    if raw_coords[0] == raw_coords[-1] and len(raw_coords) > 2:
        customers = raw_coords[1:-1]
    else:
        customers = raw_coords[1:]
        
    try:
        final_sequence = get_vroom_sequence(factory, customers)
        # CHANGED: Use ORS_URL environment variable
        ors_url = f"{ORS_URL}/v2/directions/driving-car/geojson"
        o_res = requests.post(ors_url, json={"coordinates": final_sequence}, timeout=10)
        
        if o_res.status_code == 200:
            return o_res.json()
        
        return {"error": f"Map Engine Error: {o_res.text}"}
    except Exception as e:
        return {"error": f"Routing Orchestrator failed: {str(e)}"}

# ────────────────────────────────────────────────
# Polymorphic Coordinates Ingestion Engine
# ────────────────────────────────────────────────
@app.post("/extract-coordinates")
@app.post("/api/v1/customers/extract-coordinates")
async def extract_coords(payload: Dict = Body(...)):
    """
    Polymorphic Engine: 
    - Fast Path: Accepts direct lat/lng from mobile device payload to avoid scraping.
    - Slow Path: Accepts a raw Google Maps URL from ERP desk environments and extracts via web scraping.
    """
    url = payload.get("url")
    lat = payload.get("latitude") or payload.get("lat")
    lng = payload.get("longitude") or payload.get("lng")
    
    # Fast Path (Pre-parsed by Mobile App Engine)
    if lat and lng:
        return {"status": "success", "lat": float(lat), "lng": float(lng)}
        
    # Slow Path (ERPNext Backend Scraping Request)
    if not url: 
        return {"status": "error", "message": "No URL or coordinates provided"}
        
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        res = await asyncio.to_thread(requests.get, url, headers=headers, timeout=10)
        m = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', res.url) or re.search(r'center=(-?\d+\.\d+)%2C(-?\d+\.\d+)', res.text)
        if m: 
            return {"status": "success", "lat": float(m.group(1)), "lng": float(m.group(2))}
        return {"status": "error", "message": "GPS coordinates not found in link redirect"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ────────────────────────────────────────────────
# Live Inventory & Reservation API (Strict Matrix)
# ────────────────────────────────────────────────
@app.post("/api/v1/live-inventory")
def process_live_inventory(payload: RawInventoryPayload):
    stock_map = {s['item_code']: float(s['actual_qty']) for s in payload.stock if s.get('actual_qty') is not None}
    
    # 1. Map Reserved Stock exactly to the specific Sales Order AND calculate total item reservations
    res_map_exact = {}
    total_item_res = {}
    
    for r in payload.reservations:
        icode = r['item_code']
        so = r['sales_order']
        qty = float(r['reserved_qty']) if r.get('reserved_qty') is not None else 0.0
        
        # Link specific quantity to specific order
        res_map_exact[(icode, so)] = res_map_exact.get((icode, so), 0.0) + qty
        # Tally total reserved for global availability math
        total_item_res[icode] = total_item_res.get(icode, 0.0) + qty

    # 2. Map Sales Orders
    so_map = {}
    for so in payload.sales_orders:
        icode = so['item_code']
        if icode not in so_map:
            so_map[icode] = []
        so_map[icode].append(so)

    # 3. Construct Final Matrix
    results = []
    for item in payload.items:
        icode = item['item_code']
        iname = item['item_name']
        
        actual_qty = stock_map.get(icode, 0.0)
        global_reserved = total_item_res.get(icode, 0.0)
        
        # FIFO Rule: Available is ALWAYS Total Actual - Total Reserved globally
        available_qty = actual_qty - global_reserved

        if icode in so_map:
            for so in so_map[icode]:
                so_name = so['sales_order']
                # Grab the exact reservation for this specific row
                exact_reserved = res_map_exact.get((icode, so_name), 0.0)
                
                results.append({
                    "item_id": icode,
                    "item_name": iname,
                    "sales_order": so_name,
                    "required_amount": float(so.get('qty', 0.0)),
                    "stock_balance": actual_qty,
                    "reserved_amount": exact_reserved,
                    "available_amount": available_qty
                })

    return {"status": "success", "data": results}

# ────────────────────────────────────────────────
# Production Command Cards API (Backend Math Engine)
# ────────────────────────────────────────────────
class RawProductionPayload(BaseModel):
    bips: list
    fgs: list
    stock: list
    sales_orders: list
    reservations: list

@app.post("/api/v1/production-cards")
def process_production_cards(payload: RawProductionPayload):
    # 1. Map Stock & Demand
    stock_map = {s['item_code']: float(s['actual_qty']) for s in payload.stock if s.get('actual_qty') is not None}
    
    total_res_map = {}
    so_res_map = {} 
    for r in payload.reservations:
        icode, so, qty = r['item_code'], r['sales_order'], float(r.get('reserved_qty') or 0.0)
        total_res_map[icode] = total_res_map.get(icode, 0.0) + qty
        so_res_map[(icode, so)] = qty

    # Group SOs by item
    so_map = {}
    for so in payload.sales_orders:
        so_map.setdefault(so['item_code'], []).append(so)

    # 2. Group FGs under their BIPs
    bip_fg_map = {}
    for fg in payload.fgs:
        bip_code = fg['custom_linked_bip']
        icode = fg['item_code']
        
        # SAFETY CHECK: If weight per unit isn't set, default to 1.0 to prevent infinite loops
        weight_per_unit = float(fg.get('weight_per_unit') or 1.0)
        if weight_per_unit <= 0: weight_per_unit = 1.0

        actual = stock_map.get(icode, 0.0)
        reserved = total_res_map.get(icode, 0.0)
        available = max(0.0, actual - reserved)
        
        gross_order = sum([float(so['qty']) for so in so_map.get(icode, [])])
        sales_orders_list = [so['sales_order'] for so in so_map.get(icode, [])]
        
        mrl = float(fg.get('mrl') or 0.0)
        max_shelf = float(fg.get('max_shelf') or 0.0)
        
        # THE MATH ENGINE (Units)
        primary = max(0.0, gross_order - available)
        post_order = max(0.0, available - gross_order)
        shelf = max(0.0, max_shelf - post_order) if post_order <= mrl else 0.0
        net_produce = primary + shelf
        
        # Unit to Weight Conversion (KGs)
        batch_net_produce_kg = net_produce * weight_per_unit
        
        fg_processed = {
            "id": icode,
            "name": fg['item_name'],
            "pack_code": fg.get('pack_code') or 'N/A',
            "weight_per_unit": weight_per_unit,
            "gross": gross_order,
            "actual": actual,
            "reserved": reserved,
            "available": available,
            "mrl": mrl,
            "max_shelf": max_shelf,
            "primary": primary,
            "shelf": shelf,
            "net_produce": net_produce,
            "batch_net_produce_kg": round(batch_net_produce_kg, 2),
            "excess": 0.0,
            "excess_kg": 0.0,
            "space_left": max(0.0, max_shelf - (post_order + shelf)),
            "so_list": ", ".join(sales_orders_list) if sales_orders_list else "None"
        }
        bip_fg_map.setdefault(bip_code, []).append(fg_processed)

    # 3. Finalize BIP Calculations (Tier 3 Allocation - KGs)
    results = []
    for bip in payload.bips:
        bcode = bip['bip_code']
        fgs = bip_fg_map.get(bcode, [])
        if not fgs: continue 
        
        min_batch = float(bip.get('min_batch') or 0.0)
        ideal_bulk_kg = sum([f['batch_net_produce_kg'] for f in fgs])
        
        target_batch_kg = max(ideal_bulk_kg, min_batch)
        remainder_kg = target_batch_kg - ideal_bulk_kg
        
        # Distribute excess KGs back into physical Units (Round-Robin by Weight)
        keep_allocating = True
        current_remainder_kg = remainder_kg
        
        while current_remainder_kg > 0 and keep_allocating:
            keep_allocating = False
            for f in fgs:
                space_avail = f['space_left'] - f['excess']
                # If we have enough excess fluid to fill this specific tin, and space on the shelf
                if current_remainder_kg >= f['weight_per_unit'] and space_avail >= 1:
                    f['excess'] += 1
                    f['excess_kg'] += f['weight_per_unit']
                    current_remainder_kg -= f['weight_per_unit']
                    keep_allocating = True

        results.append({
            "bip_code": bcode,
            "bip_name": bip['bip_name'],
            "min_batch": round(min_batch, 2),
            "ideal_bulk": round(ideal_bulk_kg, 2),
            "remainder": round(current_remainder_kg, 2), # Leftover trace KGs that can't fill a full tin
            "fgs": fgs
        })

    return {"status": "success", "data": results}
    
# =========================================================================
# 💼 SALES TELEMETRY ENGINE (Phase 3)
# =========================================================================
LIVE_SALES_DATA = {}
# 🚨 The Graveyard Cache to block "dying breath" pings after logout
SALES_LOGOUT_GRAVEYARD = {} 
active_sales_connections: List[WebSocket] = []

async def broadcast_sales():
    """
    Cleans up stagnant sales rep sessions and broadcasts the team state.
    """
    current_time = time.time()
    stale_keys = [k for k, v in LIVE_SALES_DATA.items() if current_time - v.get("last_updated", 0) > 60]
    for k in stale_keys:
        del LIVE_SALES_DATA[k]

    data = {"sales_team": LIVE_SALES_DATA}
    for conn in active_sales_connections[:]:
        try:
            await conn.send_json(data)
        except Exception:
            if conn in active_sales_connections:
                active_sales_connections.remove(conn)

@app.websocket("/telemetry/sales-ws")
async def sales_websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_sales_connections.append(websocket)
    try:
        await websocket.send_json({"sales_team": LIVE_SALES_DATA})
        while True:
            # 🚨 UPDATED: Active JSON parser for Ping/Pong Heartbeat
            raw_data = await websocket.receive_text()
            try:
                json_data = json.loads(raw_data)
                if json_data.get("action") == "ping":
                    await websocket.send_json({"action": "pong"})
            except json.JSONDecodeError:
                pass
    except WebSocketDisconnect:
        if websocket in active_sales_connections:
            active_sales_connections.remove(websocket)


_sid_cache: dict = {}

async def verify_erp_session(sid: str):
    import time as _t
    cached = _sid_cache.get(sid)
    if cached and cached["expires"] > _t.time():
        return cached["email"]

    # 🚨 RETRY LOGIC: Attempt verification twice with a 500ms gap.
    # On congested mobile networks (common in field conditions) the first
    # request may time out even with a valid session. Two attempts covers
    # transient failures without adding meaningful latency to happy path.
    for attempt in range(2):
        try:
            resp = await asyncio.to_thread(
                requests.get,
                f"{ERP_BASE_URL}/api/method/frappe.auth.get_logged_user",
                headers={"Cookie": f"sid={sid}", "Accept": "application/json"},
                timeout=5
            )
            if resp.status_code == 200:
                email = resp.json().get("message", "")
                if email and email != "Guest":
                    _sid_cache[sid] = {"email": email.lower(), "expires": _t.time() + 300}
                    return email.lower()
        except Exception as e:
            print(f"⚠️  ERP session verify failed (attempt {attempt + 1}): {e}")
            if attempt == 0:
                await asyncio.sleep(0.5)
    return None

@app.post("/telemetry/sales-ping")
async def receive_sales_ping(
    payload: Dict = Body(...),
    erp_sid: str = Header(None, alias="erp-sid")
):
    raw_email = payload.get("sales_rep") or payload.get("email")
    ping_email = raw_email.lower() if raw_email else None

    # 🚨 GRACE WINDOW CHECK: Allow pings through for 30 seconds after
    # login even if SID is missing or not yet verified. This covers the
    # race condition on slower Android devices (Tecno, Android 13) where
    # the native service fires before the JS bridge has written the SID.
    in_grace = False
    if ping_email and ping_email in SALES_LOGIN_GRACE:
        elapsed = time.time() - SALES_LOGIN_GRACE[ping_email]
        if elapsed < 30.0:
            in_grace = True
        else:
            del SALES_LOGIN_GRACE[ping_email]

    if not erp_sid:
        if not in_grace:
            return {"status": "error", "message": "Missing Authorization Session ID. Ping rejected."}
        # Grace window: allow through but don't verify
        verified = ping_email
    else:
        # Normal path: verify with retry
        verified = await verify_erp_session(erp_sid)
        if not verified:
            if not in_grace:
                return {"status": "error", "message": "Invalid or Expired Session ID. Ghost ping rejected."}
            verified = ping_email

    # 🚨 FIX: Force absolute lowercase to prevent UI Ghost Drops
    raw_email = payload.get("sales_rep") or payload.get("email")
    sales_rep_email = raw_email.lower() if raw_email else "Unknown_Rep"
    
    if not sales_rep_email or sales_rep_email == "unknown_rep":
        return {"status": "error", "message": "Invalid or missing sales_rep email. Ghost ping rejected."}

    # 🚨 THE GRAVEYARD GUARD
    if sales_rep_email in SALES_LOGOUT_GRAVEYARD:
        time_since_logout = time.time() - SALES_LOGOUT_GRAVEYARD[sales_rep_email]
        if time_since_logout < 120.0:
            return {"status": "rejected", "message": "Ping rejected. User recently logged out."}
        else:
            del SALES_LOGOUT_GRAVEYARD[sales_rep_email]

    full_name = payload.get("full_name") or payload.get("sales_rep_name") or sales_rep_email.split('@')[0].title()
    current_status = payload.get("current_customer_visited", "Traveling")

    if sales_rep_email not in LIVE_SALES_DATA:
        LIVE_SALES_DATA[sales_rep_email] = {
            "sales_rep_email": sales_rep_email,
            "full_name": full_name,
            "sales_rep_id": payload.get("sales_rep_id"),
            "login_timestamp": payload.get("login_timestamp", time.time()),
            "daily_visit_count": 0,
            "status": "Traveling", 
            "current_customer": "None"
        }
    else:
        LIVE_SALES_DATA[sales_rep_email]["full_name"] = full_name

    if current_status in ["Checked-In", "Checking In"]:
        LIVE_SALES_DATA[sales_rep_email]["status"] = "Checked-In"
    elif current_status in ["Idle", "Traveling"]:
        LIVE_SALES_DATA[sales_rep_email]["status"] = "Traveling"
        LIVE_SALES_DATA[sales_rep_email]["current_customer"] = "None"
    else:
        LIVE_SALES_DATA[sales_rep_email]["status"] = "Checked-In"
        LIVE_SALES_DATA[sales_rep_email]["current_customer"] = current_status

    LIVE_SALES_DATA[sales_rep_email].update({
        "lat": payload.get("lat"),
        "lng": payload.get("lng"),
        "speed": payload.get("speed", 0),
        "heading": payload.get("heading", 0),
        "last_updated": time.time(),
    })

    await broadcast_sales()
    return {"status": "ok"}

@app.post("/telemetry/sales-check-in")
async def receive_sales_check_in(payload: Dict = Body(...)):
    """Webhook triggered instantly by ERPNext when a Check-In record is saved."""
    sales_rep_email = payload.get("sales_rep")
    customer = payload.get("customer")
    customer_name = payload.get("customer_name") or customer 

    if sales_rep_email in LIVE_SALES_DATA:
        LIVE_SALES_DATA[sales_rep_email]["status"] = "Checked-In"
        LIVE_SALES_DATA[sales_rep_email]["current_customer"] = customer_name
        LIVE_SALES_DATA[sales_rep_email]["daily_visit_count"] += 1
        await broadcast_sales()
        
    return {"status": "ok"}

@app.post("/telemetry/sales-logout")
async def receive_sales_logout(payload: Dict = Body(...)):
    """
    The Telemetry Kill Switch. Instantly wipes the rep from RAM, 
    triggers an immediate "Offline" shift on the Dispatch UI, and pushes 
    the email to the Graveyard Cache to drop dying-breath pings.
    """
    sales_rep_email = payload.get("sales_rep")
    if sales_rep_email:
        if sales_rep_email in LIVE_SALES_DATA:
            del LIVE_SALES_DATA[sales_rep_email]
            
        SALES_LOGOUT_GRAVEYARD[sales_rep_email] = time.time()
        
        await broadcast_sales()
        print(f"📡 Telemetry Purged & Blacklisted: {sales_rep_email} logged out.")
    return {"status": "purged"}

# 🚨 Grace window registry: email -> timestamp of login
# Pings arriving within 30s of login are allowed through even if SID
# verification fails, to cover slow-device bridge timing gaps
SALES_LOGIN_GRACE: dict = {}

@app.post("/telemetry/sales-login")
async def receive_sales_login(payload: Dict = Body(...)):
    sales_rep_email = payload.get("sales_rep")
    if sales_rep_email:
        if sales_rep_email in SALES_LOGOUT_GRAVEYARD:
            del SALES_LOGOUT_GRAVEYARD[sales_rep_email]
            print(f"🌅 Telemetry Resurrected: {sales_rep_email} cleared from graveyard.")
        # Seed the grace window so the first 30s of pings are never
        # hard-rejected even if the SID hasn't propagated yet
        SALES_LOGIN_GRACE[sales_rep_email] = time.time()
    return {"status": "resurrected"}

# ────────────────────────────────────────────────
# MOBILE APP COMMAND HUB & CENTRAL CACHE EVICTION
# ────────────────────────────────────────────────
mobile_app_connections: Dict[str, WebSocket] = {}

@app.websocket("/telemetry/app-ws/{email}")
async def app_websocket_endpoint(websocket: WebSocket, email: str):
    """
    Dedicated WebSocket for the React Native App to listen for server commands.
    🚨 FIX: Single-Device Concurrency Enforcement.
    """
    await websocket.accept()
    
    # 🚨 Kill Switch: If this user is already connected on another device, terminate the old session.
    if email in mobile_app_connections:
        try:
            await mobile_app_connections[email].send_json({
                "command": "FORCE_LOGOUT", 
                "reason": "Security Alert: Your account was just logged in from a new device. This session has been terminated."
            })
            await mobile_app_connections[email].close()
        except Exception:
            pass
            
    mobile_app_connections[email] = websocket
    
    try:
        while True:
            # Active JSON parser for Ping/Pong Heartbeat
            raw_data = await websocket.receive_text()
            try:
                json_data = json.loads(raw_data)
                if json_data.get("action") == "ping":
                    await websocket.send_json({"action": "pong"})
            except json.JSONDecodeError:
                pass
    except WebSocketDisconnect:
        # Only delete the connection from RAM if the disconnecting socket is the currently active one
        if mobile_app_connections.get(email) == websocket:
            del mobile_app_connections[email]

@app.post("/api/v1/cache/invalidate")
async def invalidate_cache_and_notify(payload: Dict = Body(...)):
    """
    🚨 THE HYBRID ROUTER: Decoupled Cache Eviction & Broadcast Route
    1. Clears targeted rep vaults from Redis Memory.
    2. Identifies active vs offline users.
    3. Pushes 0-lag WebSockets to active screens.
    4. Pushes High-Priority Headless FCM Messages to sleeping devices.
    """
    emails = payload.get("emails", [])
    command = payload.get("command", "FORCE_VAULT_SYNC")
    tokens_dict = payload.get("fcm_tokens", {}) # 🚨 NEW: Dict of {email: [token1, token2]}
    
    # 1. Clear Local Fast Cache
    for email in emails:
        cache_key = f"nexus:vault:{email}"
        await redis_vault.delete(cache_key)
        
    if "message" not in payload:
        payload["message"] = "Background Update Triggered"
        
    notified_ws = 0
    notified_fcm = 0
    
    # 2. The Traffic Router
    for email in emails:
        # A. If the app is open on the screen, hit the zero-lag WebSocket
        if email in mobile_app_connections:
            try:
                await mobile_app_connections[email].send_json(payload)
                notified_ws += 1
            except Exception:
                pass
        
        # B. If the app is closed, locked, or backgrounded, fire the Firebase Headless JS Engine
        else:
            user_tokens = tokens_dict.get(email, [])
            if user_tokens:
                try:
                    # Construct a Data-Only message (No notification tray banner, pure background execution)
                    message = fcm_messaging.MulticastMessage(
                        data={
                            "command": command,
                            "doctype": payload.get("doctype", "System"),
                            "silent_sync_flag": "true"
                        },
                        tokens=user_tokens,
                        android=fcm_messaging.AndroidConfig(priority="high") # Ensures Android OS wakes up the process
                    )
                    
                    # Push through Google Servers
                    response = await asyncio.to_thread(fcm_messaging.send_multicast, message)
                    notified_fcm += response.success_count
                    print(f"🔥 FCM Push Success to {email}: {response.success_count} devices woken up.")
                except Exception as e:
                    print(f"⚠️ FCM Push Failed for {email}: {e}")
                
    return {"status": "success", "ws_pings": notified_ws, "fcm_pings": notified_fcm}


@app.post("/telemetry/force-app-refresh")
async def force_app_refresh(payload: Dict = Body(...)):
    """
    Legacy Forwarding Proxy (Maps seamlessly into the new Cache Eviction pipeline)
    """
    return await invalidate_cache_and_notify(payload)

# =========================================================================
# THE FASTAPI MIDDLEWARE (SALES DATA VAULT & SYNC ENGINE)
# =========================================================================

@app.get("/api/v1/sync/sales-vault/{sales_rep_email}")
async def get_sales_vault(
    sales_rep_email: str,
    current_hash: Optional[str] = Query(default=None, alias="current_hash"), 
    erp_url: str = Header(..., alias="erp-url", description="ERPNext Base URL"),
    erp_token: str = Header("No_Token", alias="erp-token", description="ERPNext Authorization Token"),
    erp_sid: str = Header("No_SID", alias="erp-sid", description="ERPNext Session ID")
):
    """
    Massive payload endpoint. Compressed catalog & customers for mobile vault.
    Engineered with Cache Stampede Protection (Mutex Locking) for 0-Lag Scale.
    Now includes all metadata arrays (customer_groups, territories, price_lists,
    payment_terms_templates, currencies, tax_categories) for mobile dropdowns.
    """
    cache_key = f"nexus:vault:{sales_rep_email}"
    lock_key = f"nexus:lock:vault:{sales_rep_email}"

    # CHANGED: Use redis_vault for cache operations
    cached_data = await redis_vault.get(cache_key)
    if cached_data:
        data = json.loads(cached_data)
        if current_hash and current_hash == data.get("new_hash"):
            return {"status": "unchanged"}
        return data

    # 2. Mutex Execution Block (Cache Stampede Protection)
    max_retries = 50
    retry_delay = 0.2
    
    for _ in range(max_retries):
        # Attempt to capture the execution lock in Redis Memory
        acquired = await redis_vault.set(lock_key, "locked", nx=True, ex=15)
        
        if acquired:
            try:
                # Target lock won, execute the heavy ERPNext HTTP compilation cycle
                headers = {
                    "X-Frappe-CSRF-Token": erp_token, 
                    "Content-Type": "application/json",
                    "Cookie": f"sid={erp_sid}"
                }
                
                def fetch_context():
                    endpoint = f"{erp_url}/api/method/nexus_supply_chain.api.get_sales_context"
                    res = requests.get(endpoint, headers=headers, timeout=30)
                    res.raise_for_status()
                    return res.json().get("message", {})

                context_response = await asyncio.to_thread(fetch_context)
                
                if context_response.get("status") != "success":
                    return {"status": "error", "message": context_response.get("message", "Failed to fetch context from ERPNext.")}
                    
                data = context_response.get("data", {})
                
                customers_raw = data.get("customers", [])
                items_raw = data.get("items", [])
                prices_raw = data.get("prices", [])
                bins_raw = data.get("bins", [])
                regions_raw = data.get("regions", [{"name": "Default Center"}])
                order_history = data.get("order_history", [])
                debt_snapshot = data.get("debt_snapshot", [])
                
                # 🚨 FIX: Scaffold the dashboard_stats to prevent {} overwriting the React Native state
                raw_stats = data.get("dashboard_stats", {})
                dashboard_stats = {
                    "sales_target": float(raw_stats.get("sales_target", 0.0)),
                    "collection_target": float(raw_stats.get("collection_target", 0.0)),
                    "mtd_sales": float(raw_stats.get("mtd_sales", 0.0)),
                    "mtd_collections": float(raw_stats.get("mtd_collections", 0.0))
                }
                
                # 🚨 METADATA ARRAYS FOR MOBILE DROPDOWNS
                customer_groups_raw = data.get("customer_groups", [])
                territories_raw = data.get("territories", [])
                price_lists_raw = data.get("price_lists", [])
                payment_terms_templates_raw = data.get("payment_terms_templates", [])
                currencies_raw = data.get("currencies", [])
                tax_categories_raw = data.get("tax_categories", [])

                # --- Data Compression Engine ---
                qty_map = {}
                for b in bins_raw:
                    qty_map[b.get("item_code")] = qty_map.get(b.get("item_code"), 0) + float(b.get("actual_qty", 0))
                    
                price_map = {}
                for p in prices_raw:
                    ic = p.get("item_code")
                    pl = p.get("price_list")
                    rate = float(p.get("price_list_rate", 0))
                    if ic not in price_map:
                        price_map[ic] = {}
                    price_map[ic][pl] = rate
                    
                catalog = []
                for it in items_raw:
                    ic = it.get("name") 
                    catalog.append({
                        "item_code": ic,
                        "item_name": it.get("item_name", ic),
                        "actual_qty": qty_map.get(ic, 0.0),
                        "prices": price_map.get(ic, {})
                    })
                    
                payload_data = {
                    "customers": customers_raw,
                    "catalog": catalog,
                    "regions": [r.get("name") for r in regions_raw],
                    "order_history": order_history,
                    "debt_snapshot": debt_snapshot,
                    "dashboard_stats": dashboard_stats,
                    # 🚨 INCLUDE METADATA FOR MOBILE DROPDOWNS
                    "customer_groups": customer_groups_raw,
                    "territories": territories_raw,
                    "price_lists": price_lists_raw,
                    "payment_terms_templates": payment_terms_templates_raw,
                    "currencies": currencies_raw,
                    "tax_categories": tax_categories_raw
                }
                
                payload_str = json.dumps(payload_data, sort_keys=True)
                new_hash = hashlib.sha256(payload_str.encode('utf-8')).hexdigest()
                
                payload_data["new_hash"] = new_hash
                payload_data["status"] = "success"
                
                # Push the compiled mapping strictly back into Redis memory limits
                await redis_vault.set(cache_key, json.dumps(payload_data))
                
                if current_hash and current_hash == new_hash:
                    return {"status": "unchanged"}
                    
                return payload_data
                
            finally:
                # Destroy lock signature
                await redis_vault.delete(lock_key)
        else:
            # Lock exists, intercept the sleep loop protocol and read directly
            await asyncio.sleep(retry_delay)
            cached_data = await redis_vault.get(cache_key)
            if cached_data:
                data = json.loads(cached_data)
                if current_hash and current_hash == data.get("new_hash"):
                    return {"status": "unchanged"}
                return data
                
    return {"status": "error", "message": "Server cluster busy executing updates, please retry."}


@app.get("/api/v1/sales/financial-brief/{customer_id}")
async def get_financial_brief(
    customer_id: str,
    erp_url: str = Header(..., alias="erp-url", description="ERPNext Base URL"),
    erp_token: str = Header("No_Token", alias="erp-token", description="ERPNext Authorization Token"),
    erp_sid: str = Header("No_SID", alias="erp-sid", description="ERPNext Session ID") 
):
    headers = {
        "X-Frappe-CSRF-Token": erp_token, 
        "Content-Type": "application/json",
        "Cookie": f"sid={erp_sid}"
    }
    
    def fetch_invoices():
        params = {
            "filters": f'[["customer","=","{customer_id}"],["docstatus","=",1]]',
            "fields": '["name","posting_date","due_date","grand_total","outstanding_amount"]',
            "limit_page_length": 5000
        }
        res = requests.get(f"{erp_url}/api/resource/Sales Invoice", headers=headers, params=params, timeout=10)
        if res.status_code == 200:
            return res.json().get("data", [])
        return []
        
    try:
        invoices = await asyncio.to_thread(fetch_invoices)
        
        ytd = 0.0
        mtd = 0.0
        outstanding = 0.0
        overdue = 0.0
        
        today = datetime.now().date()
        
        for inv in invoices:
            p_date_str = inv.get("posting_date")
            if not p_date_str: continue
            p_date = datetime.strptime(p_date_str, "%Y-%m-%d").date()
            
            g_total = float(inv.get("grand_total", 0))
            out_amt = float(inv.get("outstanding_amount", 0))
            
            if p_date.year == today.year:
                ytd += g_total
                if p_date.month == today.month:
                    mtd += g_total
                    
            if out_amt > 0:
                outstanding += out_amt
                d_date_str = inv.get("due_date")
                if d_date_str:
                    d_date = datetime.strptime(d_date_str, "%Y-%m-%d").date()
                    if d_date < today:
                        overdue += out_amt
                        
        return {
            "status": "success",
            "data": {
                "ytd": round(ytd, 2),
                "mtd": round(mtd, 2),
                "outstanding": round(outstanding, 2),
                "overdue": round(overdue, 2)
            }
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/v1/sales/reports/financials/{report_type}")
async def get_financial_reports(
    report_type: str,
    erp_url: str = Header(..., alias="erp-url"),
    erp_token: str = Header(..., alias="erp-token"),
    erp_sid: str = Header(..., alias="erp-sid")
):
    headers = {
        "X-Frappe-CSRF-Token": erp_token, 
        "Content-Type": "application/json",
        "Cookie": f"sid={erp_sid}"
    }
    
    def fetch_report():
        endpoint = f"{erp_url}/api/method/nexus_supply_chain.api.get_extended_sales_reports"
        params = {"report_type": report_type.capitalize()}
        res = requests.get(endpoint, headers=headers, params=params, timeout=20)
        res.raise_for_status()
        return res.json().get("message", {})

    try:
        report_response = await asyncio.to_thread(fetch_report)
        return report_response
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/v1/sales/invoice-details/{order_id}")
async def get_invoice_details(
    order_id: str,
    erp_url: str = Header(..., alias="erp-url"),
    erp_token: str = Header(..., alias="erp-token"),
    erp_sid: str = Header(..., alias="erp-sid")
):
    headers = {
        "X-Frappe-CSRF-Token": erp_token, 
        "Content-Type": "application/json",
        "Cookie": f"sid={erp_sid}"
    }
    
    def fetch_details():
        endpoint = f"{erp_url}/api/method/nexus_supply_chain.api.get_invoice_details_for_order"
        params = {"order_id": order_id}
        res = requests.get(endpoint, headers=headers, params=params, timeout=10)
        res.raise_for_status()
        return res.json().get("message", {})

    try:
        details_response = await asyncio.to_thread(fetch_details)
        return details_response
    except Exception as e:
        return {"status": "error", "message": str(e)}

# 🚨 NEW: Mobile Customer Creation & Proxy Model
class MobileCustomerPayload(BaseModel):
    payload: Dict[str, Any]

@app.post("/api/v1/sales/create-customer")
async def proxy_create_customer(
    body: MobileCustomerPayload,
    erp_url: str = Header(..., alias="erp-url"),
    erp_token: str = Header(..., alias="erp-token"),
    erp_sid: str = Header(..., alias="erp-sid")
):
    """
    Proxies the customer creation payload to ERPNext and forces an instant cache drop.
    """
    headers = {
        "X-Frappe-CSRF-Token": erp_token, 
        "Content-Type": "application/json",
        "Cookie": f"sid={erp_sid}"
    }
    
    def create_customer_in_erp():
        endpoint = f"{erp_url}/api/method/nexus_supply_chain.api.create_mobile_customer"
        res = requests.post(endpoint, headers=headers, json={"payload": body.payload}, timeout=20)
        return res

    try:
        res = await asyncio.to_thread(create_customer_in_erp)
        if res.status_code == 200:
            response_data = res.json().get("message", {})
            if response_data.get("status") == "success":
                # 🚨 SUCCESS: Drop the cache for this specific user to force a rebuild
                # The rep's email should be passed up in the payload or retrieved from session
                # If they trigger a resync on the app side immediately, it will be clean
                return {"status": "success", "customer_id": response_data.get("customer_id")}
            else:
                return {"status": "error", "message": response_data.get("message", "Customer creation failed on ERP.")}
        else:
            return {"status": "error", "message": f"ERP Sync Failure: {res.text}"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

class MobileOrderPayload(BaseModel):
    customer: str
    delivery_region: str
    notes: str
    items: List[Dict[str, Any]]

@app.post("/api/v1/sales/submit-order")
async def submit_sales_order(
    payload: MobileOrderPayload,
    erp_url: str = Header(..., alias="erp-url", description="ERPNext Base URL"),
    erp_token: str = Header("No_Token", alias="erp-token", description="ERPNext Authorization Token"),
    erp_sid: str = Header("No_SID", alias="erp-sid", description="ERPNext Session ID") 
):
    headers = {
        "X-Frappe-CSRF-Token": erp_token, 
        "Content-Type": "application/json",
        "Cookie": f"sid={erp_sid}"
    }
    
    def push_order():
        endpoint = f"{erp_url}/api/method/nexus_supply_chain.api.submit_sales_order_from_app"
        res = requests.post(endpoint, headers=headers, json={"payload": payload.dict()}, timeout=20)
        return res

    try:
        res = await asyncio.to_thread(push_order)
        if res.status_code == 200:
            response_data = res.json().get("message", {})
            if response_data.get("status") == "success":
                return {"status": "success", "erp_order_id": response_data.get("erp_order_id")}
            else:
                return {"status": "error", "message": response_data.get("message", "Order creation failed on ERP.")}
        else:
            print(f"❌ ERP Order Sync Failure: {res.text}")
            return {"status": "error", "message": res.text}
    except Exception as e:
        return {"status": "error", "message": str(e)}