import numpy as np
import json
import os
from collections import deque
from dataclasses import dataclass
from typing import Dict, Tuple

class Config:
    FAT_FINGER_PRICE = 0.0
    STALE_TICKER_MS = 5000        
    WINDOW_SIZE = 100
    SIGMA_MULTIPLIER = 3.0
    MU = np.array([0.0, 0.0])
    INV_COV = np.eye(2)
    TIMESTAMP_TOLERANCE_MS = 60 * 1000 
    
    @classmethod
    def load(cls, config_path):
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    data = json.load(f)
                    cls.MU = np.array(data['mu'])
                    cls.INV_COV = np.array(data['inv_cov'])
                    if 'timestamp_tolerance_ms' in data:
                        cls.TIMESTAMP_TOLERANCE_MS = data['timestamp_tolerance_ms']
            except: pass

@dataclass
class MarketEvent:
    event_time: int
    local_time: int
    type: str
    data: Dict
    sanitization: str = "ACCEPT"
    reject_reason: str = ""

class SystemState(str):
    BOOTSTRAP = "BOOTSTRAP"
    NORMAL = "NORMAL"
    UNSTABLE = "UNSTABLE"
    HALTED = "HALTED"

class DataSanitizer:
    def __init__(self):
        self.seen_ids = set()
        self.recent_ids = deque(maxlen=5000000) 

    def check(self, event: MarketEvent) -> MarketEvent:
        payload = event.data
        
        if 'id' in payload:
            raw_id = payload['id']
            if raw_id is not None:
                try: 
                    str_id = str(int(float(raw_id)))
                except: 
                    str_id = str(raw_id).strip()

                if str_id.lower() != 'nan':
                    if str_id in self.seen_ids:
                        event.sanitization = "QUARANTINE"
                        event.reject_reason = "DUPLICATE"
                        return event
                    
                    self.seen_ids.add(str_id)
                    self.recent_ids.append(str_id)
                    if len(self.seen_ids) > 5000000: 
                        removed = self.recent_ids.popleft()
                        self.seen_ids.discard(removed)

        diff_us = abs(event.event_time - event.local_time)
        diff_ms = diff_us / 1000.0
        
        if diff_ms > Config.TIMESTAMP_TOLERANCE_MS:
            event.sanitization = "QUARANTINE"
            event.reject_reason = "TIMESTAMP_ERROR"
            return event
        
        if 'price' in payload:
            try:
                price = float(payload['price'])
                if price <= Config.FAT_FINGER_PRICE:
                    event.sanitization = "QUARANTINE"
                    event.reject_reason = "FAT_FINGER"
                    return event
            except: pass
        
        event.sanitization = "ACCEPT"
        return event

class TimeManager:
    def __init__(self):
        self.last_ticker = 0
    def update(self, event_local_time):
        self.last_ticker = event_local_time
    def is_stale(self, current_sys_time) -> bool:
        if self.last_ticker == 0: return False
        diff_us = current_sys_time - self.last_ticker
        diff_ms = diff_us / 1000.0
        return diff_ms > Config.STALE_TICKER_MS

class AdaptiveRegimeModel:
    def __init__(self):
        self.prices = deque(maxlen=50) 
        self.current_vol = 0.0
        self.best_bid = 0.0
        self.best_ask = 0.0
        self.current_spread = 0.0
        self.initialized = False
        self.dist_history = deque(maxlen=Config.WINDOW_SIZE)
        self.mu = Config.MU
        self.inv_cov = Config.INV_COV

    def update_market_data(self, price=None, bid=None, ask=None):
        if price and price > 0:
            self.prices.append(price)
            if len(self.prices) >= 2:
                ret = np.log(price / self.prices[-2])
                self.current_vol = self.current_vol * 0.9 + abs(ret) * 0.1 
                self.initialized = True
        
        if bid and bid > 0:
            self.best_bid = bid
        if ask and ask > 0:
            self.best_ask = ask
            
        if self.best_bid > 0 and self.best_ask > 0:
            self.current_spread = self.best_ask - self.best_bid

    def detect_shock(self) -> Tuple[float, bool, str]:
        if not self.initialized or self.current_spread <= 0 or self.current_vol <= 1e-9:
            return 0.0, False, ""
        x = np.array([np.log(self.current_vol), np.log(self.current_spread)])
        delta = x - self.mu
        dist = np.sqrt(max(0, delta.T @ self.inv_cov @ delta))
        self.dist_history.append(dist)
        
        if len(self.dist_history) < 20: return dist, False, "GATHERING_DATA"
        mean_dist = np.mean(self.dist_history)
        std_dist = np.std(self.dist_history)
        dynamic_threshold = mean_dist + (Config.SIGMA_MULTIPLIER * std_dist)
        return dist, dist > dynamic_threshold, f"Dist:{dist:.2f}"

class DecisionEngine:
    def __init__(self, config_path="/output/model_config.json"):
        Config.load(config_path)
        self.sanitizer = DataSanitizer()
        self.time_manager = TimeManager()
        self.model = AdaptiveRegimeModel()
        self.state = SystemState.BOOTSTRAP
        self.halt_start = 0

    def process_event(self, event: MarketEvent) -> Dict:
        event = self.sanitizer.check(event)
        
        if event.sanitization == "QUARANTINE":
            self.state = SystemState.HALTED
            return self._format_decision(event, "HALT", f"QUARANTINE: {event.reject_reason}")

        if event.type == "ORDERBOOK" and 'price' in event.data and 'side' in event.data:
            try:
                price = float(event.data['price'])
                side = event.data['side']
                if side == 'bid' and self.model.best_ask > 0 and price >= self.model.best_ask:
                    return self._format_decision(event, "IGNORED", "CROSSED_MARKET")
                if side == 'ask' and self.model.best_bid > 0 and price <= self.model.best_bid:
                    return self._format_decision(event, "IGNORED", "CROSSED_MARKET")
            except: pass

        is_stale = self.time_manager.is_stale(event.local_time)
        self.time_manager.update(event.local_time)
        trigger = ""

        if event.type == "TRADE":
            self.model.update_market_data(price=float(event.data.get('price', 0)))
        elif event.type == "ORDERBOOK":
            side = event.data.get('side')
            price = float(event.data.get('price', 0))
            if side == 'bid': self.model.update_market_data(bid=price)
            elif side == 'ask': self.model.update_market_data(ask=price)

        if is_stale:
            self.state = SystemState.HALTED
            trigger = "DATA_STALE"
        else:
            if self.state == SystemState.HALTED:
                self.state = SystemState.NORMAL
                trigger = "RECOVERED"
            
            dist, is_shock, info = self.model.detect_shock()
            if is_shock:
                self.state = SystemState.UNSTABLE
                trigger = f"ADAPTIVE_SHOCK ({info})"
            elif self.state == SystemState.UNSTABLE:
                mean_dist = np.mean(self.model.dist_history)
                std_dist = np.std(self.model.dist_history)
                if dist < (mean_dist + 1.0 * std_dist): 
                    self.state = SystemState.NORMAL

        if self.state == SystemState.BOOTSTRAP and len(self.model.dist_history) > 20:
            self.state = SystemState.NORMAL

        action = "ALLOWED"
        if self.state == SystemState.HALTED: action = "HALT"
        elif self.state == SystemState.UNSTABLE: action = "RESTRICTED"
        elif self.state == SystemState.BOOTSTRAP: action = "HALT"

        return self._format_decision(event, action, trigger)

    def _format_decision(self, event, action, trigger=""):
        duration = 0
        if action == "HALT":
            if self.halt_start == 0: self.halt_start = event.event_time
            duration = event.event_time - self.halt_start
        else: self.halt_start = 0
            
        return {
            "ts": event.event_time,
            "action": action,      
            "reason": trigger if trigger else event.reject_reason, 
            "duration_ms": duration,
            "_internal_state": self.state,
            "_trigger_detail": trigger
        }

    def get_state_info(self):
        trust = "TRUSTED"; hypothesis = "VALID"
        if self.state == SystemState.HALTED: 
            trust = "UNTRUSTED"; hypothesis = "INVALID"
        elif self.state == SystemState.UNSTABLE: 
            trust = "DEGRADED"; hypothesis = "WEAKENING"
        elif self.state == SystemState.BOOTSTRAP: 
            trust = "DEGRADED"; hypothesis = "GATHERING"
        return trust, hypothesis
