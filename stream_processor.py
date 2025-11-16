import json
import uuid
import random
from datetime import datetime, timedelta, timezone

from kafka_connectors import KafkaSource, KafkaSink 
from bytewax.dataflow import Dataflow
from bytewax import operators as op

from bytewax.operators import windowing as win
from bytewax.operators.windowing import EventClock, SessionWindower

KAFKA_BROKERS = ["kafka:29092"]
INPUT_TOPIC = "bess.raw.data"
CYCLES_OUTPUT_TOPIC = "bess.cycles.calculated"
TX_LOG_OUTPUT_TOPIC = "bess.transaction.log"
SESSION_GAP_SEC = 30

def get_timestamp(reading):
    return datetime.fromisoformat(reading['timestamp']).replace(tzinfo=timezone.utc)

clock = EventClock(get_timestamp, wait_for_system_duration=timedelta(seconds=5))
window = SessionWindower(gap=timedelta(seconds=SESSION_GAP_SEC))

def builder():
    return {
        "charge_wh": 0.0,
        "discharge_wh": 0.0,
        "start_time": None,
        "end_time": None,
        "count": 0,
        "interval_sec": 1
    }

def folder(acc, reading):
    power_kw = reading['power_charge_discharge']
    ts = get_timestamp(reading)
    
    energy_wh = (power_kw * 1000 * acc["interval_sec"]) / 3600 

    if energy_wh > 0:
        acc["charge_wh"] += energy_wh
    else:
        acc["discharge_wh"] += abs(energy_wh)
        
    acc["end_time"] = ts
    if acc["start_time"] is None:
        acc["start_time"] = ts
    acc["count"] += 1
    return acc

def merger(acc1, acc2):
    acc1["charge_wh"] += acc2["charge_wh"]
    acc1["discharge_wh"] += acc2["discharge_wh"]
    
    if acc1["start_time"] is None:
        acc1["start_time"] = acc2["start_time"]
    elif acc2["start_time"] is not None:
        acc1["start_time"] = min(acc1["start_time"], acc2["start_time"])
        
    if acc1["end_time"] is None:
        acc1["end_time"] = acc2["end_time"]
    elif acc2["end_time"] is not None:
        acc1["end_time"] = max(acc1["end_time"], acc2["end_time"])
        
    acc1["count"] += acc2["count"]
    return acc1

def calculate_efficiency(key_window_data):
    bess_id, (window_id, data) = key_window_data
    
    if data["count"] == 0:
        return None

    cycle_type = "UNKNOWN"
    efficiency = None

    if data["charge_wh"] > 0.1 and data["discharge_wh"] < 0.1:
        cycle_type = "CHARGE_ONLY"
    elif data["charge_wh"] < 0.1 and data["discharge_wh"] > 0.1:
        cycle_type = "DISCHARGE_ONLY"
    elif data["charge_wh"] > 0.1 and data["discharge_wh"] > 0.1:
        cycle_type = "FULL_CYCLE"
        efficiency = data["discharge_wh"] / data["charge_wh"]
    
    return {
        "bess_id": bess_id,
        "cycle_type": cycle_type,
        "start_time": data["start_time"],
        "end_time": data["end_time"],
        "total_charged_wh": data["charge_wh"],
        "total_discharged_wh": data["discharge_wh"],
        "roundtrip_efficiency": efficiency,
        "message_count": data["count"]
    }

def run_2pc_coordinator(cycle_data):
    if cycle_data is None:
        return {"settlement": None, "log": None}

    tx_id = str(uuid.uuid4())
    print(f"[2PC Coord {tx_id}]: PREPARE for BESS {cycle_data['bess_id']}")
    
    delivered = cycle_data["total_discharged_wh"]
    committed = delivered * random.uniform(0.95, 1.05) 
    
    vote_p1 = "VOTE_ABORT"
    details_p1 = "No energy delivered"
    
    if delivered > 1.0 and abs(delivered - committed) < (committed * 0.05):
        vote_p1 = "VOTE_COMMIT"
        details_p1 = f"Delivered {delivered:.2f}Wh (Committed {committed:.2f}Wh)"

    market_service_ok = random.choice([True, True, False])
    vote_p2 = "VOTE_COMMIT" if market_service_ok else "VOTE_ABORT"
    details_p2 = "Market service validated" if market_service_ok else "Market service NOT FOUND"

    print(f"[2PC Coord {tx_id}]: Votes: P1={vote_p1}, P2={vote_p2}")
    
    log_payload = {
        "tx_id": tx_id,
        "coordinator_node": "bytewax_processor",
        "details": f"P1: {details_p1} | P2: {details_p2}",
        "event_time": datetime.now(timezone.utc).isoformat()
    }

    if vote_p1 == "VOTE_COMMIT" and vote_p2 == "VOTE_COMMIT":
        print(f"[2PC Coord {tx_id}]: GLOBAL_COMMIT")
        log_payload["status"] = "GLOBAL_COMMIT"
        
        settlement_payload = {
            **cycle_data,
            "tx_id": tx_id,
            "committed_energy": committed,
            "delivered_energy": delivered,
            "payout_amount": delivered * 0.15
        }
        return {"settlement": settlement_payload, "log": log_payload}
    else:
        print(f"[2PC Coord {tx_id}]: GLOBAL_ABORT")
        log_payload["status"] = "GLOBAL_ABORT"
        return {"settlement": None, "log": log_payload}

flow = Dataflow("bess_processor")

stream = op.input("kafka_in", flow, KafkaSource(KAFKA_BROKERS, INPUT_TOPIC, group_id="bytewax_bess_reader"))

def deserialize(key_bytes_tuple):
    key, value_bytes = key_bytes_tuple
    if value_bytes is None:
        return None
    try:
        return key, json.loads(value_bytes.decode('utf-8'))
    except Exception as e:
        print(f"Deserialization error: {e}, Value: {value_bytes}")
        return None
stream_deserialized = op.filter_map("deserialize", stream, deserialize)

stream_keyed = op.map("key_by_bess_id", stream_deserialized, lambda k_v: (k_v[1]['bess_id'], k_v[1]))

window_out = win.fold_window(
    "session_window", 
    stream_keyed, 
    clock, 
    window, 
    builder, 
    folder,
    merger
)
stream_windowed = window_out.down

stream_cycles = op.filter_map("calc_efficiency", stream_windowed, calculate_efficiency)

stream_2pc_results = op.map("run_2pc", stream_cycles, run_2pc_coordinator)

tx_logs = op.map("get_logs", stream_2pc_results, lambda res: res["log"])
tx_logs_filtered = op.filter("filter_empty_logs", tx_logs, lambda log: log is not None)
tx_logs_formatted = op.map("format_logs", tx_logs_filtered, lambda data: (data['tx_id'], data))
op.output("kafka_log_out", tx_logs_formatted, KafkaSink(KAFKA_BROKERS, TX_LOG_OUTPUT_TOPIC))

settlements = op.map("get_settlements", stream_2pc_results, lambda res: res["settlement"])
settlements_filtered = op.filter("filter_aborted", settlements, lambda s: s is not None)
settlements_formatted = op.map("format_settlements", settlements_filtered, lambda data: (data['bess_id'], data))
op.output("kafka_cycles_out", settlements_formatted, KafkaSink(KAFKA_BROKERS, CYCLES_OUTPUT_TOPIC))