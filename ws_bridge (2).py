#!/usr/bin/env python3
"""
ws_bridge.py  —  UWB Full Racing System  (Dynamic Track + Dynamic Penalties)
=============================================================================

UDP packet format from anchor ESP32:
  "A0,AT+RANGE=tid:0,mask:0F,seq:122,range:(354,636,1157,1134,0,0,0,0),ancid:(0,1,2,3,-1,-1,-1,-1)"

  • prefix  A{anchor_id}  (ignored — ancid field in payload is used instead)
  • tid     tag index (0-based)
  • range   8 values, first 4 used, in cm
  • ancid   which anchor each slot corresponds to (-1 = unused)

All positions and distances are in CENTIMETRES internally.
Track CSV coordinates must also be in cm.

Filtering pipeline (added):
  UDP raw ranges
    → Layer 1: per-anchor spike rejection  (jump > JUMP_THRESHOLD_CM discarded)
    → Layer 2: per-anchor rolling median   (window = MEDIAN_WINDOW)
    → Trilateration → (x, y)
    → Layer 3: per-tag Kalman filter       (4-state: x, y, vx, vy)
    → tag.update_position()
"""

import asyncio, websockets, socket, json, math, time, threading, signal, sys
import urllib.request, urllib.error, re
import numpy as np
from datetime import datetime
from collections import defaultdict, deque
import statistics

# ═══════════════════════════════════════════════════════════════════════
# FILTERING CONSTANTS  (tuned to match the actual Arduino firmware timing)
# ═══════════════════════════════════════════════════════════════════════
# Hardware cadence reference (from esp32s3_at_a0.ino / esp32s3_at_t0.ino):
#   AT+SETCAP=<UWB_TAG_COUNT>,10,1  → 10ms time-slot per tag, AT+SETRPT=1
#   means each anchor completes one full ranging round (all tags) every
#   UWB_TAG_COUNT * 10ms and reports immediately (no batching). With
#   UWB_TAG_COUNT=3 that's a ~30ms cycle per anchor → ~33Hz per anchor.
#   Four anchors writing to the same UDP port independently means actual
#   packet arrival at the bridge is irregular (anywhere from <5ms to
#   ~100ms+ apart) even though the underlying hardware cadence is steady.
#   KALMAN_DT below is only a fallback/seed value — the real dt used at
#   runtime is measured per-packet (see UWBFilter._l3) so prediction
#   accuracy doesn't degrade when packets arrive irregularly.
HARDWARE_SLOT_MS  = 10.0    # AT+SETCAP slot length, must match firmware
EXPECTED_TAG_SLOTS = 3      # AT+SETCAP tag count, must match firmware
EXPECTED_CYCLE_MS  = HARDWARE_SLOT_MS * EXPECTED_TAG_SLOTS   # ~30ms/anchor

JUMP_THRESHOLD_CM = 150.0   # L1: reject per-anchor jump larger than this.
                             # Real hardware noise between consecutive
                             # readings for the same anchor is ~0-40cm
                             # (measured from field logs); genuine
                             # multipath/NLOS spikes are 300cm+. 150cm
                             # comfortably separates the two.
MEDIAN_WINDOW     = 15       # L2: rolling median window per anchor —
                             # widened from 5 → 9 (≈270ms of history at
                             # the hardware's ~30ms/anchor cycle) for
                             # heavier smoothing of per-anchor range
                             # noise before it ever reaches trilateration
                             # or the Kalman stage. Lag tradeoff accepted
                             # since flicker reduction was prioritized.
KALMAN_R          = 60.0    # L3: measurement noise covariance (cm²).
KALMAN_Q          = 0.04    # L3: process noise covariance — heavy
                             # smoothing mode. The previous Q=12 traded
                             # too much smoothness for tracking speed:
                             # frame-to-frame jumps averaged ~6cm (visible
                             # flicker) because the filter trusted raw,
                             # noisy measurements almost directly. Q=0.15
                             # with R=30 cuts frame-to-frame jumps to
                             # ~1.4cm (well under the ~6-10cm raw noise
                             # floor) at the cost of more lag while
                             # cornering — an explicit tradeoff since
                             # flicker was the priority and lag is
                             # acceptable for this use case.
KALMAN_DT         = EXPECTED_CYCLE_MS / 1000.0   # seed value only (~0.03s)
KALMAN_DT_MIN     = 0.005   # clamp: ignore implausibly tiny dt (duplicate
                             # or near-simultaneous packets) to avoid
                             # divide-by-near-zero velocity blowups
KALMAN_DT_MAX     = 0.5     # clamp: if a tag went stale and comes back
                             # after a long gap, treat it as a fresh
                             # re-seed rather than projecting velocity
                             # across the whole gap

# ═══════════════════════════════════════════════════════════════════════
# NETWORK CONFIGURATION  ← edit here
# ═══════════════════════════════════════════════════════════════════════
UDP_PORT     = 4210
IMU_UDP_PORT = 4211   # IMU data from tag on RC car
WS_PORT      = 8001

# ── IMU / ZUPT constants ──────────────────────────────────────────────
# ZUPT = Zero Velocity Update: freeze position completely when stationary.
# Position only resumes when motion is confirmed — eliminates all jitter.
IMU_GYRO_DEADBAND       = 0.02   # rad/s  — gyro noise floor, ignored
STATIC_GYRO_THR         = 0.08   # rad/s  — above this = definitely rotating
STATIC_ACCEL_DELTA_THR  = 0.8    # m/s²   — change in accel magnitude = moving
STATIC_FRAMES_TO_FREEZE = 8      # consecutive static IMU frames before freeze
MOTION_FRAMES_TO_UNFREEZE = 3    # consecutive motion frames before unfreeze
# When frozen, Kalman state velocity is zeroed so no velocity artifact on restart

# IMU data per tag — written by imu_listener thread, read by udp_receiver
# {tag_id: {heading, gz, ax, ay, az, gx, gy, ts}}
imu_store: dict = {}


class MotionGate:
    """
    Per-tag ZUPT gate with hysteresis.
    Needs STATIC_FRAMES_TO_FREEZE consecutive static readings to freeze.
    Needs only MOTION_FRAMES_TO_UNFREEZE consecutive motion readings to unfreeze.
    This prevents flickering on the static/moving boundary.
    """
    def __init__(self):
        self.frozen        = False
        self.frozen_x      = 0.0
        self.frozen_y      = 0.0
        self._static_count = 0
        self._motion_count = 0
        self._prev_accel   = None   # previous accel magnitude for delta check

    def update(self, imu: dict | None, kalman_speed_cms: float) -> bool:
        """
        Returns True if car is considered stationary (position should be frozen).
        Uses IMU as primary detector; Kalman speed as fallback.
        """
        if imu is None:
            # No IMU — fall back to Kalman speed only
            moving = kalman_speed_cms > 5.0  # >5 cm/s = moving
        else:
            gz         = abs(imu.get('gz', 0.0))
            ax         = imu.get('ax', 0.0)
            ay         = imu.get('ay', 0.0)
            az         = imu.get('az', 0.0)
            accel_mag  = math.sqrt(ax**2 + ay**2 + az**2)
            accel_delta = abs(accel_mag - self._prev_accel) if self._prev_accel is not None else 0.0
            self._prev_accel = accel_mag

            # Rotating OR accel changing = moving
            rotating       = gz > STATIC_GYRO_THR
            accel_changing = accel_delta > STATIC_ACCEL_DELTA_THR
            moving = rotating or accel_changing

        if moving:
            self._motion_count += 1
            self._static_count  = 0
            if self._motion_count >= MOTION_FRAMES_TO_UNFREEZE:
                self.frozen = False
        else:
            self._static_count += 1
            self._motion_count  = 0
            if self._static_count >= STATIC_FRAMES_TO_FREEZE:
                self.frozen = True

        return self.frozen

    def freeze(self, x: float, y: float):
        """Store freeze position and zero Kalman velocity (called externally)."""
        self.frozen_x = x
        self.frozen_y = y

    def reset(self):
        self.frozen        = False
        self.frozen_x      = self.frozen_y = 0.0
        self._static_count = self._motion_count = 0
        self._prev_accel   = None

DJANGO_API_BASE    = 'https://xraceapi.zyberspace.in'
LAP_API_URL        = f'{DJANGO_API_BASE}/api/record-lap/'

# ── Anchor physical positions in CENTIMETRES ──────────────────────────────
ANCHOR_POSITIONS = {
    0: (0.00, 0.00),
    1: (655.00, 0.00),
    2: (655.00, 920.00),
    3: (0.00, 920.00),
}

ANCHOR_COUNT = 4
TAG_COUNT    = 6

# Range validity limits in CENTIMETRES
MIN_RANGE_M = 10.0
MAX_RANGE_M = 1450.0

# ── Default race / penalty values (overridden by admin_start payload) ──
TOTAL_LAPS_DEFAULT                     = 10
TOTAL_LAPS                             = TOTAL_LAPS_DEFAULT
MIN_LAPS_TO_QUALIFY                    = 3
MIN_LAP_TIME                           = 3.0   # seconds

WALL_HIT_PENALTY_DEFAULT               = 5.0
CAR_COLLISION_ATTACKER_PENALTY_DEFAULT = 5.0
CAR_COLLISION_VICTIM_BONUS_DEFAULT     = 2.0

WALL_HIT_PENALTY               = WALL_HIT_PENALTY_DEFAULT
CAR_COLLISION_ATTACKER_PENALTY = CAR_COLLISION_ATTACKER_PENALTY_DEFAULT
CAR_COLLISION_VICTIM_BONUS     = CAR_COLLISION_VICTIM_BONUS_DEFAULT

# ── Default start/finish in CENTIMETRES (overridden by CSV) ────────────────
START_LINE_X         = 490.00
START_LINE_Y1        = 300.00
START_LINE_Y2        = 340.00
LINE_CROSS_TOLERANCE = 8.00
LINE_Y_TOLERANCE     = 30.00
SF_CROSSING_DIR      = 'left_to_right'
# Proximity-based S/F gate: centre point + radius (cm). Any tag within this
# radius of the S/F centre counts as crossing — direction-agnostic, so the
# CSV's two endpoints no longer need to form a strict vertical line.
SF_GATE_CX     = (START_LINE_X)
SF_GATE_CY     = (START_LINE_Y1 + START_LINE_Y2) / 2
SF_GATE_RADIUS = 90.00   # cm — generous "anywhere near the start" zone.
                          # Must exceed the track's half-width at the S/F
                          # point, or a car hugging either wall edge will
                          # sit right at/outside the radius boundary.

# Minimum effective checkpoint radius (cm). Any checkpoint defined in the
# CSV with a smaller radius is still treated as at least this generous, so
# "touch any part near checkpoint" works reliably.
CP_MIN_RADIUS = 45.00

# ── Default checkpoints in CENTIMETRES (overridden by CSV) ─────────────────
CHECKPOINTS = [
    (390.0, 320.0, 22.0), (290.0, 325.0, 22.0), (190.0, 310.0, 22.0),
    (80.0, 290.0, 22.0), (55.0, 240.0, 22.0), (80.0, 185.0, 22.0),
    (160.0, 140.0, 22.0), (280.0, 100.0, 22.0), (420.0, 110.0, 22.0),
    (530.0, 165.0, 22.0), (555.0, 235.0, 22.0), (530.0, 295.0, 22.0),
]

tag_to_gp:              dict       = {}
current_group_id:       int | None = None
current_tournament_id:  int | None = None   # set from admin_start payload

CORNER_CUT_PENALTY         = 3.0
CORNER_CUT_VOID_LAP        = False
CAR_COLLISION_DISTANCE_M   = 25.0   # fallback centre-to-centre pre-check
CAR_COLLISION_COOLDOWN     = 1.0

# Physical car dimensions in CENTIMETRES (F1-style RC car, tag at centre)
CAR_LENGTH_CM = 40.0   # square car — both sides equal
CAR_WIDTH_CM  = 40.0   # square car — both sides equal
SPEED_DIFF_THRESHOLD       = 10.0
WALL_TOLERANCE_M           = 5.0
WALL_COLLISION_COOLDOWN    = 0.5
MAX_PLAUSIBLE_SPEED_M_S    = 2800.0
SPEED_DISPLAY_UNIT         = 'km/h'

PRINT_LAP_EVENTS       = True
PRINT_COLLISION_EVENTS = True
PRINT_WALL_EVENTS      = True
PRINT_ANOMALIES        = True

TRAIL_LENGTH = 30
TAG_TIMEOUT  = 5

checkpoint_touch_history: dict = {}
checkpoint_active_lap:    dict = {}  # cp_idx -> [{car_id, car_name}], current lap only, cleared per car on lap done


# ═══════════════════════════════════════════════════════════════════════
# THREE-LAYER UWB FILTER  (one instance per tag)
# ═══════════════════════════════════════════════════════════════════════

class UWBFilter:
    """
    Layer 1 — per-anchor jump rejection
      Any anchor reading that jumps more than JUMP_THRESHOLD_CM from its
      last accepted value is treated as a spike and replaced with the
      last known good value (or 0 if none yet).

    Layer 2 — per-anchor rolling median
      A deque of the last MEDIAN_WINDOW valid readings per anchor.
      Median is used (not mean) because it is completely immune to
      remaining outliers.

    Layer 3 — Kalman filter on final (x, y)
      4-state filter: [x, y, vx, vy].  Predicts ahead then corrects
      with the trilaterated measurement.  R / Q are tuned for medium
      aggression: smooth enough to kill jitter, responsive enough to
      follow a fast RC car.
    """

    def __init__(self, tag_id: int, anchor_count: int = 4):
        self.tag_id = tag_id
        self.n      = anchor_count

        # L1
        self._last_valid: dict[int, float] = {}

        # L2
        self._buffers: dict[int, deque] = {}

        # L3
        self._kf             = self._make_kalman()
        self._kf_initialised = False
        self._last_kf_time   = None   # wall-clock time of last KF update,
                                        # used to compute real per-packet dt

        # diagnostics
        self.l1_rejects  = 0
        self.l2_smoothed = 0
        self.l3_updates  = 0

    # ── L1 ──────────────────────────────────────────────────────────────
    def _l1(self, anchor_id: int, dist: float) -> float | None:
        if dist <= 0:
            return None
        prev = self._last_valid.get(anchor_id)
        if prev is not None and abs(dist - prev) > JUMP_THRESHOLD_CM:
            self.l1_rejects += 1
            return None          # spike — caller uses last good median
        self._last_valid[anchor_id] = dist
        return dist

    # ── L2 ──────────────────────────────────────────────────────────────
    def _l2(self, anchor_id: int, dist: float) -> float:
        buf = self._buffers.setdefault(anchor_id, deque(maxlen=MEDIAN_WINDOW))
        buf.append(dist)
        self.l2_smoothed += 1
        return statistics.median(buf)

    # ── L3 ──────────────────────────────────────────────────────────────
    @staticmethod
    def _make_kalman():
        """Build the KF structure once. F/Q get overwritten per-update in
        _l3() using the real measured dt, so the dt baked in here only
        matters for the very first predict() call before any real
        timestamp has been observed."""
        from filterpy.kalman import KalmanFilter
        dt = KALMAN_DT
        kf = KalmanFilter(dim_x=4, dim_z=2)
        kf.F = np.array([
            [1, 0, dt, 0],
            [0, 1,  0, dt],
            [0, 0,  1,  0],
            [0, 0,  0,  1],
        ], dtype=float)
        kf.H = np.array([
            [1, 0, 0, 0],
            [0, 1, 0, 0],
        ], dtype=float)
        kf.R = np.eye(2) * KALMAN_R
        kf.Q = np.eye(4) * KALMAN_Q
        kf.P = np.eye(4) * 20.0
        kf.x = np.zeros((4, 1))
        return kf

    @staticmethod
    def _f_for_dt(dt: float) -> np.ndarray:
        return np.array([
            [1, 0, dt, 0],
            [0, 1,  0, dt],
            [0, 0,  1,  0],
            [0, 0,  0,  1],
        ], dtype=float)

    @staticmethod
    def _q_for_dt(dt: float) -> np.ndarray:
        # Scale process noise with dt so longer gaps between packets
        # widen the uncertainty proportionally instead of using a fixed
        # value tuned for one specific (and often wrong) interval.
        return np.eye(4) * (KALMAN_Q * dt / KALMAN_DT)

    def _l3(self, x: float, y: float, now: float | None = None) -> tuple[float, float]:
        if not self._kf_initialised:
            self._kf.x[:] = [[x], [y], [0.0], [0.0]]
            self._kf_initialised = True
            self._last_kf_time = now
            return x, y

        # Compute real elapsed time since the last update. Falls back to
        # the seed KALMAN_DT if no timestamp was supplied (keeps the
        # filter usable even if a caller forgets to pass `now`).
        if now is not None and self._last_kf_time is not None:
            dt = now - self._last_kf_time
        else:
            dt = KALMAN_DT

        if dt > KALMAN_DT_MAX:
            # Tag was stale for a while (e.g. went out of range and came
            # back) — re-seed instead of projecting velocity across a
            # huge gap, which would fling the predicted position far
            # from reality.
            self._kf.x[:] = [[x], [y], [0.0], [0.0]]
            self._kf.P = np.eye(4) * 20.0
            self._last_kf_time = now
            return x, y

        dt = max(dt, KALMAN_DT_MIN)   # avoid divide-by-near-zero velocity blowups

        self._kf.F = self._f_for_dt(dt)
        self._kf.Q = self._q_for_dt(dt)
        self._kf.predict()
        self._kf.update(np.array([[x], [y]]))
        self._last_kf_time = now
        self.l3_updates += 1
        return float(self._kf.x[0, 0]), float(self._kf.x[1, 0])

    # ── Public API ───────────────────────────────────────────────────────
    def filter_ranges(self, raw: list[float]) -> list[float]:
        """Apply L1 + L2 to anchor ranges. Returns cleaned list (same length)."""
        out = []
        for i, r in enumerate(raw):
            if r <= 0:
                # Keep last smoothed value for this anchor if any
                buf = self._buffers.get(i)
                out.append(statistics.median(buf) if buf else 0.0)
                continue
            r1 = self._l1(i, r)
            if r1 is None:
                # Spike: use current median (don't update buffer)
                buf = self._buffers.get(i)
                out.append(statistics.median(buf) if buf else 0.0)
            else:
                out.append(self._l2(i, r1))
        return out

    def filter_position(self, x: float, y: float, now: float | None = None) -> tuple[float, float]:
        """Apply L3 Kalman to trilaterated (x, y). Pass `now` (time.time())
        so the filter can compute real elapsed-time dt instead of assuming
        a fixed interval — needed because UDP packet arrival from the
        anchors is irregular even though the underlying hardware ranging
        cycle (~30ms, from AT+SETCAP=3,10,1) is steady."""
        return self._l3(x, y, now)

    def reset(self):
        self._last_valid.clear()
        self._buffers.clear()
        self._kf             = self._make_kalman()
        self._kf_initialised = False
        self._last_kf_time   = None
        self.l1_rejects = self.l2_smoothed = self.l3_updates = 0


# ═══════════════════════════════════════════════════════════════════════
# UDP PACKET PARSER  — AT+RANGE string format
# ═══════════════════════════════════════════════════════════════════════

_RE_RANGE = re.compile(
    r'tid:(\d+).*?range:\(([^)]+)\)(?:.*?ancid:\(([^)]+)\))?',
    re.IGNORECASE
)

def parse_at_range(raw_bytes: bytes):
    text = raw_bytes.decode('utf-8', errors='ignore').strip()
    m = _RE_RANGE.search(text)
    if not m:
        raise ValueError(f"No AT+RANGE pattern in: {text!r}")

    tag_id    = int(m.group(1))
    range_raw = [float(x.strip()) for x in m.group(2).split(',')]
    ancid_raw = ([int(x.strip()) for x in m.group(3).split(',')]
                 if m.group(3) else [])
    ranges_m  = range_raw   # hardware already in cm
    return tag_id, ranges_m, ancid_raw


# ═══════════════════════════════════════════════════════════════════════
# TRACK CSV PARSER
# ═══════════════════════════════════════════════════════════════════════

class TrackData:
    def __init__(self):
        self.center:      list = []
        self.inner:       list = []
        self.outer:       list = []
        self.checkpoints: list = []
        self.sf_x:   float = START_LINE_X
        self.sf_y1:  float = START_LINE_Y1
        self.sf_y2:  float = START_LINE_Y2
        self.sf_dir: str   = SF_CROSSING_DIR
        # True midpoint of the two raw S/F endpoints, regardless of whether
        # they form a vertical, horizontal, or diagonal segment. Used for
        # proximity-based gate detection (works for ANY line orientation).
        self.sf_cx:  float = SF_GATE_CX
        self.sf_cy:  float = SF_GATE_CY

    def is_loaded(self) -> bool:
        return bool(self.center)

    def to_dict(self) -> dict:
        return dict(
            center=self.center,
            inner=self.inner,
            outer=self.outer,
            checkpoints=[
                {"id": i, "x": cp[0], "y": cp[1], "r": cp[2]}
                for i, cp in enumerate(self.checkpoints)
            ],
            start_finish=dict(
                x=self.sf_x, y1=self.sf_y1, y2=self.sf_y2, dir=self.sf_dir),
        )


def parse_track_csv(csv_text: str) -> TrackData:
    """CSV coordinates are stored in METRES. Tag positions used throughout the
    rest of this module (CHECKPOINTS, START_LINE_*, live x/y from hardware)
    are in CENTIMETRES — matching the frontend, which does the same *100
    conversion (see parseCsvIntoLiveTrack / applyLiveTrack in tag_manager.html).
    """
    M_TO_CM = 100.0
    td = TrackData()
    cp_dict: dict = {}

    for raw in csv_text.splitlines():
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        parts = [p.strip() for p in line.split(',')]
        if len(parts) < 2:
            continue
        kind = parts[0].upper()
        try:
            if kind == 'CENTER' and len(parts) >= 3:
                td.center.append((float(parts[1]) * M_TO_CM, float(parts[2]) * M_TO_CM))
            elif kind == 'INNER' and len(parts) >= 3:
                td.inner.append((float(parts[1]) * M_TO_CM, float(parts[2]) * M_TO_CM))
            elif kind == 'OUTER' and len(parts) >= 3:
                td.outer.append((float(parts[1]) * M_TO_CM, float(parts[2]) * M_TO_CM))
            elif kind == 'START_FINISH' and len(parts) >= 5:
                x1, y1_sf = float(parts[1]) * M_TO_CM, float(parts[2]) * M_TO_CM
                x2, y2_sf = float(parts[3]) * M_TO_CM, float(parts[4]) * M_TO_CM
                td.sf_x  = (x1 + x2) / 2
                td.sf_y1 = min(y1_sf, y2_sf)
                td.sf_y2 = max(y1_sf, y2_sf)
                # True midpoint of the two raw endpoints — correct regardless
                # of whether the segment is vertical, horizontal, or diagonal.
                # This is what proximity-based gate detection actually uses.
                td.sf_cx = (x1 + x2) / 2
                td.sf_cy = (y1_sf + y2_sf) / 2
                if len(parts) >= 6:
                    td.sf_dir = parts[5].lower().strip()
            elif kind == 'CHECKPOINT' and len(parts) >= 5:
                cp_id = int(parts[1])
                x, y, r = float(parts[2]) * M_TO_CM, float(parts[3]) * M_TO_CM, float(parts[4]) * M_TO_CM
                cp_dict[cp_id] = (x, y, r)
        except (ValueError, IndexError) as e:
            print(f"[CSV] Parse warning on '{line}': {e}")
            continue

    if cp_dict:
        td.checkpoints = [cp_dict[k] for k in sorted(cp_dict.keys())]

    return td


def apply_track_data(td: TrackData):
    global CHECKPOINTS, START_LINE_X, START_LINE_Y1, START_LINE_Y2, SF_CROSSING_DIR
    global SF_GATE_CX, SF_GATE_CY

    if td.checkpoints:
        CHECKPOINTS = list(td.checkpoints)
        print(f"[TRACK] {len(CHECKPOINTS)} checkpoints loaded from CSV")
    else:
        print("[TRACK] No checkpoints in CSV — keeping previous")

    START_LINE_X    = td.sf_x
    START_LINE_Y1   = td.sf_y1
    START_LINE_Y2   = td.sf_y2
    SF_CROSSING_DIR = td.sf_dir
    SF_GATE_CX      = td.sf_cx
    SF_GATE_CY      = td.sf_cy
    print(f"[TRACK] S/F  x={START_LINE_X:.3f}cm  y=[{START_LINE_Y1:.3f}..{START_LINE_Y2:.3f}]cm  dir={SF_CROSSING_DIR}")
    print(f"[TRACK] S/F gate centre = ({SF_GATE_CX:.3f}, {SF_GATE_CY:.3f})cm  radius={SF_GATE_RADIUS:.1f}cm")

    for eng in race_mgr._engines.values():
        eng.reset()
    print("[TRACK] All lap engines reset with new track data")


current_track = TrackData()


# ═══════════════════════════════════════════════════════════════════════
# ANCID-AWARE RANGE REORDERING
# ═══════════════════════════════════════════════════════════════════════

def reorder_by_ancid(slot_ranges, ancid, n=ANCHOR_COUNT):
    has_ancid = bool(ancid) and any(a >= 0 for a in ancid)
    if not has_ancid:
        return [float(r) for r in slot_ranges[:n]]
    out = [0.0] * n
    for slot, anc in enumerate(ancid):
        if 0 <= anc < n and slot < len(slot_ranges):
            out[anc] = float(slot_ranges[slot])
    return out


# ═══════════════════════════════════════════════════════════════════════
# DYNAMIC CONFIG
# ═══════════════════════════════════════════════════════════════════════

def apply_race_config(cfg: dict, new_laps):
    global TOTAL_LAPS, WALL_HIT_PENALTY, CAR_COLLISION_ATTACKER_PENALTY, CAR_COLLISION_VICTIM_BONUS

    TOTAL_LAPS = new_laps if isinstance(new_laps, int) and new_laps > 0 else TOTAL_LAPS_DEFAULT

    w = cfg.get('object_collision_time')
    WALL_HIT_PENALTY = float(w) if w and float(w) > 0 else WALL_HIT_PENALTY_DEFAULT

    a = cfg.get('collision_creating_time')
    CAR_COLLISION_ATTACKER_PENALTY = float(a) if a and float(a) > 0 else CAR_COLLISION_ATTACKER_PENALTY_DEFAULT

    v = cfg.get('collision_absorbing_time')
    CAR_COLLISION_VICTIM_BONUS = float(v) if v and float(v) > 0 else CAR_COLLISION_VICTIM_BONUS_DEFAULT

    print(f"[CONFIG] laps={TOTAL_LAPS}  "
          f"wall={WALL_HIT_PENALTY}s  "
          f"attacker={CAR_COLLISION_ATTACKER_PENALTY}s  "
          f"victim_bonus={CAR_COLLISION_VICTIM_BONUS}s")


def reset_race_config():
    global TOTAL_LAPS, WALL_HIT_PENALTY, CAR_COLLISION_ATTACKER_PENALTY, CAR_COLLISION_VICTIM_BONUS
    TOTAL_LAPS                     = TOTAL_LAPS_DEFAULT
    WALL_HIT_PENALTY               = WALL_HIT_PENALTY_DEFAULT
    CAR_COLLISION_ATTACKER_PENALTY = CAR_COLLISION_ATTACKER_PENALTY_DEFAULT
    CAR_COLLISION_VICTIM_BONUS     = CAR_COLLISION_VICTIM_BONUS_DEFAULT


# ═══════════════════════════════════════════════════════════════════════
# API POSTER
# ═══════════════════════════════════════════════════════════════════════

def post_lap_to_api(tag_id: int, lap):
    gp = tag_to_gp.get(int(tag_id)) or tag_to_gp.get(str(tag_id))
    if not gp:
        print(f"[API] SKIP tag={tag_id} — not in tag_to_gp map ({tag_to_gp})")
        return

    body = json.dumps({
        "gp_id":       gp,
        "lap_number":  lap.lap_number,
        "raw_time":    round(lap.raw_time, 3),
        "elp_time":    round(lap.elp, 3),
        "penalty":     round(lap._pen, 3),
        "bonus":       round(lap._bon, 3),
        "wall_hits":   lap.wall_hits,
        "atk_hits":    lap.atk_hits,
        "vic_hits":    lap.vic_hits,
        "corner_cuts": lap.corner_cuts,
        "voided":      lap.voided,
    }).encode()

    def _go():
        try:
            req = urllib.request.Request(
                LAP_API_URL, data=body,
                headers={'Content-Type': 'application/json'},
                method='POST'
            )
            with urllib.request.urlopen(req, timeout=5) as r:
                resp_body = r.read().decode('utf-8', errors='replace')
                print(f"[API] ✓ tag={tag_id} gp={gp} lap={lap.lap_number} "
                      f"raw={lap.raw_time:.2f}s elp={lap.elp:.2f}s  resp={resp_body[:80]}")
        except urllib.error.HTTPError as e:
            err_body = e.read().decode('utf-8', errors='replace')
            print(f"[API] ✗ HTTP {e.code} tag={tag_id} lap={lap.lap_number}: {err_body[:200]}")
        except Exception as e:
            print(f"[API] ✗ error tag={tag_id} lap={lap.lap_number}: {e}")

    threading.Thread(target=_go, daemon=True).start()


# ═══════════════════════════════════════════════════════════════════════

class Positioning:
    @staticmethod
    def valid_anchors(ranges, ap):
        out = []
        for i, r in enumerate(ranges):
            if r <= 0 or i not in ap:
                continue
            if r < MIN_RANGE_M or r > MAX_RANGE_M:
                continue
            out.append({'id': i, 'range': r, 'x': ap[i][0], 'y': ap[i][1]})
        return out

    @staticmethod
    def tri3(a1, a2, a3):
        x1,y1,r1 = a1['x'],a1['y'],a1['range']
        x2,y2,r2 = a2['x'],a2['y'],a2['range']
        x3,y3,r3 = a3['x'],a3['y'],a3['range']
        A=2*(x2-x1); B=2*(y2-y1)
        C=r1**2-r2**2-x1**2+x2**2-y1**2+y2**2
        D=2*(x3-x2); E=2*(y3-y2)
        F=r2**2-r3**2-x2**2+x3**2-y2**2+y3**2
        den = A*E - B*D
        if abs(den) < 0.0001:
            d = math.hypot(x2-x1, y2-y1)
            ratio = r1/(r1+r2) if (r1+r2) > 0 else 0.5
            return x1+(x2-x1)*ratio, y1+(y2-y1)*ratio
        return (C*E-F*B)/den, (A*F-C*D)/den

    @staticmethod
    def multilat(va):
        if len(va) < 3:
            return None
        combos = []
        for i in range(len(va)):
            for j in range(i+1, len(va)):
                for k in range(j+1, len(va)):
                    px, py = Positioning.tri3(va[i], va[j], va[k])
                    combos.append((px, py))
        if not combos:
            return None
        return sum(c[0] for c in combos)/len(combos), sum(c[1] for c in combos)/len(combos)

    @staticmethod
    def calculate(ranges, ap):
        va = Positioning.valid_anchors(ranges, ap)
        nv = len(va)
        if nv >= 4:
            pos = Positioning.multilat(va); q = 'excellent'
        elif nv == 3:
            pos = Positioning.tri3(*va[:3]); q = 'good'
        elif nv == 2:
            a1, a2 = va[0], va[1]
            ratio = a1['range']/(a1['range']+a2['range']) if (a1['range']+a2['range']) > 0 else 0.5
            pos = (a1['x']+(a2['x']-a1['x'])*ratio, a1['y']+(a2['y']-a1['y'])*ratio)
            q = 'fair'
        else:
            return None, 'poor', nv
        if pos is None:
            return None, q, nv
        return (pos[0], pos[1]), q, nv


# ═══════════════════════════════════════════════════════════════════════
# TAG STATE  (positions in cm, speed in cm/s internally)
# ═══════════════════════════════════════════════════════════════════════

class TagState:
    def __init__(self, tid):
        self.id = tid; self.name = f"Car{tid}"
        self.x = self.y = 0.0
        self.status = False; self.last_update = 0.0
        self.quality = 'unknown'; self.anchor_count = 0
        self.history = deque(maxlen=TRAIL_LENGTH)
        self.update_count = 0
        self._prev_x = self._prev_y = self._prev_t = None
        self.speed_ms = self.max_speed_ms = 0.0
        self.pkt_total = self.pkt_accepted = self.pkt_rejected = 0
        self.last_ranges = [0.0]*ANCHOR_COUNT

        # ── THREE-LAYER FILTER (one per tag) ─────────────────────────
        self._filter = UWBFilter(tid, ANCHOR_COUNT)

        # ── ZUPT motion gate ──────────────────────────────────────────
        self._gate   = MotionGate()
        self.heading = 0.0   # degrees, from IMU

    def update_position(self, rx, ry, quality, anc, now):
        if self._prev_t is not None:
            dt = now - self._prev_t
            if dt > 0:
                self.speed_ms = math.hypot(rx - self._prev_x, ry - self._prev_y) / dt
                self.max_speed_ms = max(self.max_speed_ms, self.speed_ms)
        self._prev_x, self._prev_y, self._prev_t = rx, ry, now
        self.x, self.y = rx, ry
        self.quality = quality; self.anchor_count = anc
        self.status = True; self.last_update = now
        self.history.append((self.x, self.y, now))
        self.update_count += 1; self.pkt_accepted += 1

    def speed_display(self):
        if SPEED_DISPLAY_UNIT == 'km/h':
            return self.speed_ms * 0.036   # cm/s → km/h
        if SPEED_DISPLAY_UNIT == 'm/s':
            return self.speed_ms / 100.0
        return self.speed_ms

    def is_active(self):
        return self.status and (time.time() - self.last_update) < TAG_TIMEOUT

    def reset(self):
        self.history.clear()
        self._prev_x = self._prev_y = self._prev_t = None
        self.speed_ms = self.max_speed_ms = 0.0
        self.status = False
        self.update_count = self.pkt_total = self.pkt_accepted = self.pkt_rejected = 0
        self.last_ranges = [0.0]*ANCHOR_COUNT
        self._filter.reset()   # ← reset all three filter layers
        self._gate.reset()
        self.heading = 0.0


# ═══════════════════════════════════════════════════════════════════════
# SCORING
# ═══════════════════════════════════════════════════════════════════════

class LapScore:
    def __init__(self, car_id, car_name, lap_number):
        self.car_id = car_id; self.car_name = car_name; self.lap_number = lap_number
        self.raw_time = 0.0; self.closed_at = None
        self.wall_hits = self.atk_hits = self.vic_hits = self.corner_cuts = 0
        self.overspeed = self.voided = False; self._pen = self._bon = 0.0

    def add_wall_hit(self):
        self._pen += WALL_HIT_PENALTY; self.wall_hits += 1
        if PRINT_WALL_EVENTS:
            print(f"  🚧 WALL  | {self.car_name} Lap {self.lap_number}  +{WALL_HIT_PENALTY}s")

    def add_attacker_penalty(self):
        self._pen += CAR_COLLISION_ATTACKER_PENALTY; self.atk_hits += 1
        if PRINT_COLLISION_EVENTS:
            print(f"  🔴 ATK   | {self.car_name} Lap {self.lap_number}  +{CAR_COLLISION_ATTACKER_PENALTY}s")

    def add_victim_bonus(self):
        self._bon += CAR_COLLISION_VICTIM_BONUS; self.vic_hits += 1
        if PRINT_COLLISION_EVENTS:
            print(f"  🟢 VIC   | {self.car_name} Lap {self.lap_number}  -{CAR_COLLISION_VICTIM_BONUS}s")

    def add_corner_cut(self):
        self.corner_cuts += 1
        if CORNER_CUT_VOID_LAP:
            self.voided = True
        else:
            self._pen += CORNER_CUT_PENALTY

    @property
    def elp(self):
        return float('inf') if self.voided else max(0.0, self.raw_time + self._pen - self._bon)

    def to_dict(self):
        return dict(car_id=self.car_id, car_name=self.car_name, lap=self.lap_number,
                    raw=round(self.raw_time, 3), penalty=round(self._pen, 3),
                    bonus=round(self._bon, 3), elp=round(self.elp, 3), voided=self.voided)


class ScoringEngine:
    def __init__(self):
        self._history = defaultdict(list); self._open = {}; self._names = {}; self._feed = []

    def register(self, cid, name): self._names[cid] = name
    def open_lap(self, cid, n): self._open[cid] = LapScore(cid, self._names.get(cid, f"Car{cid}"), n)

    def close_lap(self, cid, raw):
        lap = self._open.pop(cid, None) or LapScore(cid, self._names.get(cid, f"Car{cid}"), 0)
        lap.raw_time = raw; lap.closed_at = time.time()
        self._history[cid].append(lap)
        msg = f"📊 LAP | {lap.car_name} Lap {lap.lap_number} raw={raw:.2f}s ELP={lap.elp:.2f}s"
        if PRINT_LAP_EVENTS: print(msg)
        self._feed.append(msg)
        post_lap_to_api(cid, lap)
        return lap

    def laps_done(self, cid): return len(self._history.get(cid, []))
    def qualifies(self, cid): return self.laps_done(cid) >= MIN_LAPS_TO_QUALIFY
    def best_elp(self, cid):
        v = [l.elp for l in self._history.get(cid, []) if not l.voided]
        return min(v) if v else float('inf')

    def wall_hit(self, cid):
        l = self._open.get(cid)
        if l: l.add_wall_hit(); self._feed.append(f"🚧 WALL {self._names.get(cid,'?')}")

    def car_collision(self, atk, vic):
        a = self._open.get(atk); v = self._open.get(vic)
        if a: a.add_attacker_penalty()
        if v: v.add_victim_bonus()
        self._feed.append(f"💥 {self._names.get(atk,'?')}>{self._names.get(vic,'?')}")

    def corner_cut(self, cid):
        l = self._open.get(cid)
        if l: l.add_corner_cut()

    def get_leaderboard(self):
        rows = []
        for cid, laps in self._history.items():
            valid = [l for l in laps if not l.voided]
            if not valid: continue
            best = min(valid, key=lambda l: (l.elp, l.closed_at or 0))
            rows.append(dict(car_id=cid, car_name=self._names.get(cid, f"Car{cid}"),
                             best_elp=round(best.elp, 3), best_raw=round(best.raw_time, 3),
                             best_lap=best.lap_number, laps_done=len(laps),
                             qualifies=self.qualifies(cid),
                             penalty_total=round(sum(l._pen for l in laps), 2),
                             bonus_total=round(sum(l._bon for l in laps), 2)))
        rows.sort(key=lambda r: (r['best_elp'], r['best_lap']))
        return rows

    def get_car_summary(self, cid):
        laps = self._history.get(cid, []); op = self._open.get(cid)
        return dict(car_id=cid, car_name=self._names.get(cid, f"Car{cid}"),
                    laps_done=len(laps), best_elp=self.best_elp(cid),
                    qualifies=self.qualifies(cid),
                    open_lap=op.to_dict() if op else None,
                    history=[l.to_dict() for l in laps])

    def get_feed(self, n=8): return self._feed[-n:]

    def reset(self):
        self._history.clear(); self._open.clear(); self._feed.clear()
        print("📊 Scoring reset")


# ═══════════════════════════════════════════════════════════════════════
# TRACK / COLLISION GEOMETRY
# ═══════════════════════════════════════════════════════════════════════

class Track:
    def __init__(self, outer, inner=None):
        self.outer = outer; self.inner = inner or []

    def has_width(self): return len(self.inner) > 0
    def get_outer_points(self): return self.outer
    def get_inner_points(self): return self.inner


def create_track_from_data(td: TrackData) -> Track:
    if td.inner and td.outer:
        return Track(td.outer, td.inner)
    return create_oval_track()


def create_oval_track(cx=305.0, cy=220.0, ow=260.0, oh=190.0, tw=30.0, n=40):
    o, i = [], []
    for k in range(n):
        a = 2*math.pi*k/n
        o.append((cx + ow*math.cos(a), cy + oh*math.sin(a)))
        i.append((cx + (ow-tw)*math.cos(a), cy + (oh-tw)*math.sin(a)))
    return Track(o, i)


def dist_to_boundary(px, py, pts):
    if not pts or len(pts) < 2:
        return float('inf')
    best = float('inf'); n = len(pts)
    for i in range(n):
        x1, y1 = pts[i]; x2, y2 = pts[(i+1)%n]
        dx, dy = x2-x1, y2-y1; den = dx*dx + dy*dy
        if den == 0:
            d = math.hypot(px-x1, py-y1)
        else:
            t = max(0, min(1, ((px-x1)*dx + (py-y1)*dy) / den))
            d = math.hypot(px-x1-t*dx, py-y1-t*dy)
        best = min(best, d)
    return best


# ═══════════════════════════════════════════════════════════════════════
# LAP ENGINE
# ═══════════════════════════════════════════════════════════════════════

class LapEngine:
    def __init__(self, cid, name, sc):
        self.car_id = cid; self.car_name = name; self.scoring = sc
        self.current_lap = 0; self.laps_done = 0
        self.is_racing = False; self.race_finished = False; self.admin_armed = False
        self._lap_start = None; self._last_cross = 0.0; self._lap_times = []
        # Order-independent checkpoint tracking: a set of indices touched
        # so far this lap. Any checkpoint can be touched in any order, from
        # any direction — it lights up the instant the tag enters its zone.
        # A lap is only valid once ALL checkpoints have been touched at
        # least once (regardless of order) before the next S/F crossing.
        self._cp_touched_this_lap: set = set()
        # Proximity-gate state: True while the tag is currently inside the
        # S/F radius. A crossing event fires only on the rising edge
        # (outside → inside), so sitting in the zone doesn't re-trigger.
        self._in_sf_zone = False
        self.current_lap_cp_hits: list = []

    def arm(self):
        self.admin_armed = True
        print(f"🟢 ARM | {self.car_name}")

    def update(self, x, y, speed, now):
        cp_ev = self._check_checkpoints(x, y, now) if self.is_racing else None
        sf_ev = self._check_sf_line(x, y, now)
        return sf_ev or cp_ev

    def _check_sf_line(self, x, y, now):
        # Proximity-based gate: ANY tag within SF_GATE_RADIUS of the S/F
        # centre point counts as "at the start/finish" — regardless of
        # approach direction or how the CSV's two raw endpoints were
        # oriented. This satisfies "pass anywhere near start".
        dist = math.hypot(x - SF_GATE_CX, y - SF_GATE_CY)
        currently_in = dist <= SF_GATE_RADIUS

        if currently_in and not self._in_sf_zone:
            # Rising edge: just entered the zone — this is the crossing.
            self._in_sf_zone = True
            if now - self._last_cross < MIN_LAP_TIME:
                print(f"[SF] {self.car_name} debounce — ignored (dist={dist:.1f}cm)")
                return None
            print(f"[SF] ✓ {self.car_name} entered S/F zone  "
                  f"x={x:.3f}cm y={y:.3f}cm dist={dist:.1f}cm (radius={SF_GATE_RADIUS:.0f}cm)")
            self._last_cross = now
            return self._process_crossing(now)

        if not currently_in and self._in_sf_zone:
            self._in_sf_zone = False   # left the zone, ready to re-trigger next time
        return None

    def _clear_active_lap(self):
        """Remove this car's dots from all checkpoint active-lap entries and touch history."""
        for cp_list in checkpoint_active_lap.values():
            cp_list[:] = [t for t in cp_list if t['car_id'] != self.car_id]
        for cp_list in checkpoint_touch_history.values():
            cp_list[:] = [t for t in cp_list if t['car_id'] != self.car_id]

    def _process_crossing(self, now):
        if not self.is_racing:
            self.is_racing = True; self.current_lap = 1
            self._lap_start = now; self._cp_touched_this_lap = set()
            self.current_lap_cp_hits = []
            self._clear_active_lap()
            self.scoring.open_lap(self.car_id, 1)
            print(f"🏁 START | {self.car_name} Lap 1/{TOTAL_LAPS}")
            return dict(type='race_start', car_id=self.car_id, car_name=self.car_name, lap=1, time=now)

        if len(self._cp_touched_this_lap) < len(CHECKPOINTS):
            missing = len(CHECKPOINTS) - len(self._cp_touched_this_lap)
            missing_idx = [i for i in range(len(CHECKPOINTS)) if i not in self._cp_touched_this_lap]
            print(f"⚠ LAP VOID | {self.car_name} — {missing} CP(s) not hit (missing: {missing_idx})")
            self._cp_touched_this_lap = set()
            self.current_lap_cp_hits = []
            self._clear_active_lap()
            return dict(type='lap_void', car_id=self.car_id, car_name=self.car_name, lap=self.current_lap, time=now)

        raw = now - self._lap_start
        ls  = self.scoring.close_lap(self.car_id, raw)
        self._lap_times.append(raw); self.laps_done += 1
        self._cp_touched_this_lap = set()
        self.current_lap_cp_hits = []
        self._clear_active_lap()
        ev = dict(type='lap_done', car_id=self.car_id, car_name=self.car_name,
                  lap=self.current_lap, raw_time=raw, elp=ls.elp, time=now)

        if self.laps_done >= TOTAL_LAPS:
            self.is_racing = False; self.race_finished = True
            if PRINT_LAP_EVENTS:
                print(f"🏆 FINISH | {self.car_name} ({self.laps_done} laps)")
            ev['type'] = 'race_finish'
            return ev

        self.current_lap += 1; self._lap_start = now
        self.scoring.open_lap(self.car_id, self.current_lap)
        if PRINT_LAP_EVENTS:
            print(f"🔄 LAP | {self.car_name} Lap {self.current_lap}/{TOTAL_LAPS} "
                  f"raw={raw:.2f}s ELP={ls.elp:.2f}s")
        return ev

    def _check_checkpoints(self, x, y, now):
        for idx, (cx, cy, cr) in enumerate(CHECKPOINTS):
            if idx in self._cp_touched_this_lap:
                continue
            eff_r = max(cr, CP_MIN_RADIUS)
            dist = math.hypot(x-cx, y-cy)
            if dist <= eff_r:
                self._cp_touched_this_lap.add(idx)
                self.current_lap_cp_hits.append(idx)
                print(f"  ✔ CP{idx} | {self.car_name} @ ({x:.3f},{y:.3f})cm "
                      f"dist={dist:.1f}cm (r={eff_r:.0f}cm) "
                      f"[{len(self._cp_touched_this_lap)}/{len(CHECKPOINTS)}]")

                # All-time history (sidebar panel)
                if idx not in checkpoint_touch_history:
                    checkpoint_touch_history[idx] = []
                if not any(t['car_id'] == self.car_id for t in checkpoint_touch_history[idx]):
                    checkpoint_touch_history[idx].append({
                        "car_id": self.car_id, "car_name": self.car_name,
                        "lap": self.current_lap, "time": now,
                    })

                # Active-lap tracking (canvas dots — resets per car per lap)
                if idx not in checkpoint_active_lap:
                    checkpoint_active_lap[idx] = []
                checkpoint_active_lap[idx] = [
                    t for t in checkpoint_active_lap[idx] if t['car_id'] != self.car_id
                ]
                checkpoint_active_lap[idx].append({
                    "car_id": self.car_id, "car_name": self.car_name,
                })

                return dict(type='checkpoint', car_id=self.car_id, car_name=self.car_name,
                            cp_index=idx, total=len(CHECKPOINTS),
                            cp_touches=checkpoint_touch_history.get(idx, []),
                            cp_active=checkpoint_active_lap.get(idx, []))
        return None

    def elapsed(self, now):
        return (now - self._lap_start) if self._lap_start else 0.0

    def best_raw(self):
        return min(self._lap_times) if self._lap_times else 0.0

    def get_info(self, now=None):
        return dict(car_id=self.car_id, car_name=self.car_name,
                    current_lap=self.current_lap, total_laps=TOTAL_LAPS,
                    laps_done=self.laps_done, is_racing=self.is_racing,
                    race_finished=self.race_finished,
                    current_lap_elapsed=self.elapsed(now or time.time()),
                    best_raw=self.best_raw(), lap_times=list(self._lap_times),
                    checkpoints_hit=len(self._cp_touched_this_lap), checkpoints_total=len(CHECKPOINTS),
                    cp_hits_this_lap=list(self.current_lap_cp_hits))

    def reset(self):
        self.current_lap = 0; self.laps_done = 0
        self.is_racing = False; self.race_finished = False; self.admin_armed = False
        self._in_sf_zone = False; self._lap_start = None; self._last_cross = 0.0
        self._lap_times.clear(); self._cp_touched_this_lap = set()
        self.current_lap_cp_hits = []


# ═══════════════════════════════════════════════════════════════════════
# RACE MANAGER
# ═══════════════════════════════════════════════════════════════════════

class RaceManager:
    def __init__(self, sc):
        self.scoring = sc; self._engines = {}
        self.race_active = False; self.race_start_time = self.race_end_time = None

    def register(self, cid, name):
        self.scoring.register(cid, name)
        self._engines[cid] = LapEngine(cid, name, self.scoring)

    def admin_start(self):
        for e in self._engines.values():
            e.arm()
        print(f"🟢 RACE ARMED – {TOTAL_LAPS} laps")

    def update(self, cid, x, y, speed, now):
        eng = self._engines.get(cid)
        if not eng: return None
        ev = eng.update(x, y, speed, now)
        if ev:
            if ev['type'] == 'race_start' and not self.race_active:
                self.race_active = True; self.race_start_time = now
                print("🏁 RACE IN PROGRESS")
            if ev['type'] == 'race_finish' and all(e.race_finished for e in self._engines.values()):
                self.race_active = False; self.race_end_time = now
                print("🏆 ALL FINISHED")
        return ev

    def get_info(self, cid, now=None):
        e = self._engines.get(cid)
        return e.get_info(now) if e else None

    def get_leaderboard(self):
        return self.scoring.get_leaderboard()

    def reset(self):
        for e in self._engines.values():
            e.reset()
        self.scoring.reset(); self.race_active = False
        self.race_start_time = self.race_end_time = None
        print("🔄 Race reset")


# ═══════════════════════════════════════════════════════════════════════
# COLLISION ENGINE
# ═══════════════════════════════════════════════════════════════════════

def _car_corners(x, y, heading):
    """Return 4 corners of the car rectangle in track-space (cm),
    given tag position (x,y) as the rectangle centre and heading in radians."""
    hl = CAR_LENGTH_CM / 2
    hw = CAR_WIDTH_CM  / 2
    cos_h, sin_h = math.cos(heading), math.sin(heading)
    # local corners: (±hl, ±hw), rotated by heading
    offsets = [( hl,  hw), ( hl, -hw), (-hl, -hw), (-hl,  hw)]
    return [(x + cos_h*dx - sin_h*dy, y + sin_h*dx + cos_h*dy)
            for dx, dy in offsets]


def _obb_overlap(cx1, cy1, h1, cx2, cy2, h2):
    """Separating Axis Theorem test for two OBBs (oriented bounding boxes).
    Returns True if the two car rectangles overlap."""
    def axes(h):
        return [(math.cos(h), math.sin(h)), (-math.sin(h), math.cos(h))]

    def project(corners, ax):
        dots = [ax[0]*c[0] + ax[1]*c[1] for c in corners]
        return min(dots), max(dots)

    c1 = _car_corners(cx1, cy1, h1)
    c2 = _car_corners(cx2, cy2, h2)
    for ax in axes(h1) + axes(h2):
        lo1, hi1 = project(c1, ax)
        lo2, hi2 = project(c2, ax)
        if hi1 < lo2 or hi2 < lo1:
            return False   # separating axis found — no overlap
    return True            # no separating axis — rectangles overlap


class CollisionEngine:
    def __init__(self, sc, trk):
        self.scoring = sc; self.track = trk
        self._names = {}; self._pos = {}; self._speeds = {}
        self._laps = {}; self._racing = {}
        self._car_cd = {}; self._wall_cd = {}
        self._heading = {}   # last known heading per car (radians)
        self.events = []; self.anomalies = []

    def register(self, cid, name): self._names[cid] = name
    def set_track(self, trk): self.track = trk

    def update(self, cars, now):
        evts = []
        for cid, d in cars.items():
            px, py = d['x'], d['y']
            # Use IMU heading if available (more accurate than position delta)
            imu = imu_store.get(cid)
            if imu and abs(imu.get('gz', 0)) > IMU_GYRO_DEADBAND:
                self._heading[cid] = math.radians(imu['heading'])
            else:
                prev = self._pos.get(cid)
                if prev:
                    dx, dy = px - prev[0], py - prev[1]
                    if math.hypot(dx, dy) > 0.3:
                        self._heading[cid] = math.atan2(dy, dx)
            self._pos[cid] = (px, py, now)
            self._speeds[cid] = d.get('speed', 0.0)
            self._laps[cid] = d.get('lap', 0)
            self._racing[cid] = d.get('racing', False)
            spd = self._speeds[cid]
            if spd > MAX_PLAUSIBLE_SPEED_M_S:
                self._anomaly(cid, spd, now)

        racing = [c for c, d in cars.items() if d.get('racing', False)]
        for i in range(len(racing)):
            for j in range(i+1, len(racing)):
                e = self._car(racing[i], racing[j], now)
                if e: evts.append(e)
        for cid, d in cars.items():
            if not d.get('racing', False): continue
            e = self._wall(cid, d['x'], d['y'], d.get('lap', 0), now)
            if e: evts.append(e)
        self.events.extend(evts)
        return evts

    def _car(self, a, b, now):
        pa = self._pos.get(a); pb = self._pos.get(b)
        if not pa or not pb: return None
        # Quick centre-to-centre pre-check (cheap reject before OBB test)
        dist = math.hypot(pa[0]-pb[0], pa[1]-pb[1])
        if dist > CAR_LENGTH_CM + 10: return None   # definitely not touching
        key = frozenset([a, b])
        if now - self._car_cd.get(key, 0) < CAR_COLLISION_COOLDOWN: return None
        # Full OBB overlap test using actual car rectangles
        ha = self._heading.get(a, 0); hb = self._heading.get(b, 0)
        if not _obb_overlap(pa[0], pa[1], ha, pb[0], pb[1], hb): return None
        self._car_cd[key] = now
        sa = self._speeds.get(a, 0); sb = self._speeds.get(b, 0)
        atk, vic = ((a, b) if abs(sa-sb) >= SPEED_DIFF_THRESHOLD and sa >= sb
                    else ((b, a) if abs(sa-sb) >= SPEED_DIFF_THRESHOLD else (a, b)))
        self.scoring.car_collision(atk, vic)
        an = self._names.get(atk, f"Car{atk}"); vn = self._names.get(vic, f"Car{vic}")
        if PRINT_COLLISION_EVENTS:
            print(f"💥 CAR | {an}→{vn} dist={dist:.1f}cm  "
                  f"atk+{CAR_COLLISION_ATTACKER_PENALTY}s / vic-{CAR_COLLISION_VICTIM_BONUS}s")
        return dict(type='car', attacker=atk, victim=vic,
                    attacker_name=an, victim_name=vn, dist=round(dist, 3),
                    lap=self._laps.get(atk, 0), time=now)

    def _wall(self, cid, x, y, lap, now):
        if not self.track or not self.track.has_width(): return None
        if now - self._wall_cd.get(cid, 0) < WALL_COLLISION_COOLDOWN: return None
        heading = self._heading.get(cid, 0)
        corners = _car_corners(x, y, heading)
        # Check if ANY corner of the car rectangle is outside the track boundary
        outer_pts = self.track.get_outer_points()
        inner_pts = self.track.get_inner_points()
        wall = None
        for cx, cy in corners:
            od = dist_to_boundary(cx, cy, outer_pts)
            id_ = dist_to_boundary(cx, cy, inner_pts)
            if od <= WALL_TOLERANCE_M:
                wall = 'outer'; break
            if id_ <= WALL_TOLERANCE_M:
                wall = 'inner'; break
        if not wall: return None
        self._wall_cd[cid] = now
        self.scoring.wall_hit(cid)
        name = self._names.get(cid, f"Car{cid}")
        if PRINT_WALL_EVENTS:
            print(f"🚧 WALL | {name} {wall} Lap{lap}  +{WALL_HIT_PENALTY}s")
        return dict(type='wall', car_id=cid, car_name=name, wall=wall, lap=lap, time=now)

    def _anomaly(self, cid, spd, now):
        n = self._names.get(cid, f"Car{cid}")
        self.anomalies.append(dict(car_id=cid, name=n, speed=spd, time=now))
        if PRINT_ANOMALIES:
            print(f"⚠️ ANOMALY | {n} speed={spd:.2f}cm/s ({spd*0.036:.1f}km/h)")

    def wall_hits(self, cid):
        return [e for e in self.events if e['type']=='wall' and e['car_id']==cid]

    def car_events(self, cid):
        return [e for e in self.events if e['type']=='car' and
                (e['attacker']==cid or e['victim']==cid)]

    def reset(self):
        self.events.clear(); self.anomalies.clear()
        self._car_cd.clear(); self._wall_cd.clear()
        print("✓ Collision reset")


# ═══════════════════════════════════════════════════════════════════════
# GLOBAL STATE
# ═══════════════════════════════════════════════════════════════════════
tags     = {i: TagState(i) for i in range(TAG_COUNT)}
scoring  = ScoringEngine()
race_mgr = RaceManager(scoring)
track    = create_oval_track()
col_eng  = CollisionEngine(scoring, track)

for tid, tag in tags.items():
    race_mgr.register(tid, tag.name)
    col_eng.register(tid, tag.name)

connected_clients = set()
event_loop        = None
running           = True
race_armed        = False

stats = {'udp_total':0,'udp_valid':0,'udp_invalid':0,'ws_sent':0,'ws_clients':0,
         'tags_seen':set(),'start':datetime.now()}


# ═══════════════════════════════════════════════════════════════════════
# RACE UPDATE HELPERS
# ═══════════════════════════════════════════════════════════════════════

def process_race_update(tid, now):
    tag = tags.get(tid)
    if not tag or not tag.is_active(): return []
    evts = []
    ev = race_mgr.update(tid, tag.x, tag.y, tag.speed_ms, now)
    if ev: evts.append(ev)
    cars = {}
    for t_id, t in tags.items():
        if t.is_active():
            li = race_mgr.get_info(t_id, now)
            cars[t_id] = dict(x=t.x, y=t.y, speed=t.speed_ms,
                               lap=li['current_lap'] if li else 0,
                               racing=li['is_racing'] if li else False)
    if cars: evts.extend(col_eng.update(cars, now))
    return evts


def build_state(now):
    cars = []
    for tid, tag in tags.items():
        if not tag.is_active(): continue
        li = race_mgr.get_info(tid, now); sc = scoring.get_car_summary(tid)
        cars.append(dict(
            tag_id=tid, name=tag.name,
            x=round(tag.x, 4), y=round(tag.y, 4),
            speed=round(tag.speed_display(), 2), speed_unit=SPEED_DISPLAY_UNIT,
            speed_ms=round(tag.speed_ms, 3),
            quality=tag.quality, anchor_count=tag.anchor_count,
            last_ranges=tag.last_ranges,
            trail=[(round(h[0],4), round(h[1],4)) for h in tag.history],
            lap_info=li,
            scoring=dict(best_elp=sc['best_elp'] if sc['best_elp']<float('inf') else None,
                         laps_done=sc['laps_done'], qualifies=sc['qualifies'],
                         history=sc['history']),
            wall_hits=len(col_eng.wall_hits(tid)),
            car_collisions=len(col_eng.car_events(tid)),
            pkt_accepted=tag.pkt_accepted, pkt_rejected=tag.pkt_rejected))
    return json.dumps(dict(
        type="state_update", timestamp=now,
        race_active=race_mgr.race_active, race_armed=race_armed,
        total_laps=TOTAL_LAPS, group_id=current_group_id,
        race_config=dict(wall_hit_penalty=WALL_HIT_PENALTY,
                         attacker_penalty=CAR_COLLISION_ATTACKER_PENALTY,
                         victim_bonus=CAR_COLLISION_VICTIM_BONUS),
        track=current_track.to_dict(),
        cars=cars, leaderboard=race_mgr.get_leaderboard(),
        feed=scoring.get_feed(10),
        checkpoint_touches=_serialize_cp_touches(), cp_active_lap=_serialize_cp_active()))


def _serialize_cp_touches() -> dict:
    result = {}
    for cp_id, touches in checkpoint_touch_history.items():
        result[str(cp_id)] = [
            {"car_id": t["car_id"], "car_name": t["car_name"], "lap": t["lap"]}
            for t in touches
        ]
    return result


def _serialize_cp_active() -> dict:
    """Per-lap active touches: only cars that touched this CP this lap.
    Resets per car when their lap completes. Used by canvas dot coloring."""
    result = {}
    for cp_id, touches in checkpoint_active_lap.items():
        if touches:
            result[str(cp_id)] = [
                {"car_id": t["car_id"], "car_name": t["car_name"]}
                for t in touches
            ]
    return result


# ═══════════════════════════════════════════════════════════════════════
# UDP RECEIVER  — AT+RANGE parser with full three-layer filtering
# ═══════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════
# IMU UDP LISTENER  — T0,IMU,heading,gz,ax,ay,az,gx,gy,ts
# ═══════════════════════════════════════════════════════════════════════

def imu_listener():
    """
    Receives IMU UDP packets from the tag on port 4211.
    Format: T0,IMU,293.54,0.0903,16.856,7.353,7.377,0.0314,-0.0887,105585
    Fields: T{id},IMU,heading_deg,gz,ax,ay,az,gx,gy,timestamp_ms
    Writes to imu_store[tag_id] — dict assignment is GIL-safe in CPython.
    """
    global running
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('', IMU_UDP_PORT))
    sock.settimeout(0.1)
    print(f"[IMU] Listening on port {IMU_UDP_PORT}")
    while running:
        try:
            data, _ = sock.recvfrom(512)
            line    = data.decode('utf-8', errors='ignore').strip()
            parts   = line.split(',')
            if len(parts) < 10 or parts[1] != 'IMU':
                continue
            tid = int(parts[0][1:])   # 'T0' → 0
            imu_store[tid] = {
                'heading': float(parts[2]),
                'gz':      float(parts[3]),
                'ax':      float(parts[4]),
                'ay':      float(parts[5]),
                'az':      float(parts[6]),
                'gx':      float(parts[7]),
                'gy':      float(parts[8]),
                'ts':      int(parts[9]),
            }
            # Update tag heading immediately (doesn't wait for UWB packet)
            if tid in tags:
                tags[tid].heading = float(parts[2])
        except socket.timeout:
            continue
        except Exception as e:
            if running:
                print(f"[IMU] Error: {e}")
    sock.close()
    print("[IMU] Stopped")


def udp_receiver():
    global running
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('', UDP_PORT))
    sock.settimeout(0.1)
    print(f"[UDP] Listening on port {UDP_PORT}")
    print(f"[UDP] Filtering: L1 jump>{JUMP_THRESHOLD_CM}cm | L2 median(w={MEDIAN_WINDOW}) | L3 Kalman(R={KALMAN_R},Q={KALMAN_Q})")
    while running:
        try:
            data, addr = sock.recvfrom(2048)
            try:
                sock.sendto(data, ('192.168.29.27', UDP_PORT))
                # print(f"[FWD] Forwarded {len(data)}B to 192.168.29.27:{UDP_PORT}")  # uncomment to debug
            except Exception as fwd_err:
                if stats['udp_total'] % 50 == 1:
                    print(f"[FWD] Forward to 192.168.29.27:{UDP_PORT} failed: {fwd_err}")
            stats['udp_total'] += 1
            # ── Parse AT+RANGE format ──────────────────────────────────
            try:
                tid, ranges_m, ancid = parse_at_range(data)
            except ValueError as e:
                stats['udp_invalid'] += 1
                if stats['udp_total'] % 50 == 1:
                    print(f"[UDP] Parse fail from {addr}: {e}")
                continue
            if tid not in tags:
                stats['udp_invalid'] += 1
                continue
            # Reorder by ancid so index matches ANCHOR_POSITIONS
            raw_ranges = reorder_by_ancid(ranges_m, ancid, ANCHOR_COUNT)
            now = time.time()
            tag = tags[tid]; tag.pkt_total += 1
            # ── LAYER 1 + 2: filter per-anchor ranges ─────────────────
            filtered_ranges = tag._filter.filter_ranges(raw_ranges)
            # ── Trilaterate on filtered ranges ─────────────────────────
            pos, quality, anc_count = Positioning.calculate(filtered_ranges, ANCHOR_POSITIONS)
            if pos is None:
                tag.pkt_rejected += 1; stats['udp_invalid'] += 1
                continue
            # ── LAYER 3: Kalman on (x, y) ─────────────────────────────
            rx, ry = tag._filter.filter_position(pos[0], pos[1], now)

            # ── CLAMP POSITION TO ANCHOR BOUNDING BOX ─────────────────
            rx = max(0.0, min(rx, 655.0))
            ry = max(0.0, min(ry, 920.0))

            # ── LAYER 4: ZUPT — freeze when stationary ─────────────────
            # Kalman speed in cm/s for fallback detection
            kalman_speed = tag.speed_ms   # last known speed
            imu          = imu_store.get(tid)
            is_frozen    = tag._gate.update(imu, kalman_speed)

            if is_frozen:
                # Store freeze position on first freeze frame
                if not tag._gate.frozen_x and not tag._gate.frozen_y:
                    tag._gate.freeze(rx, ry)
                # Zero Kalman velocity so no burst on restart
                tag._filter._kf.x[2, 0] = 0.0
                tag._filter._kf.x[3, 0] = 0.0
                rx, ry = tag._gate.frozen_x, tag._gate.frozen_y
                is_static = True
            else:
                # Update freeze anchor to current position while moving
                # (ready for next freeze event)
                tag._gate.freeze(rx, ry)
                is_static = False

            tag.update_position(rx, ry, quality, anc_count, now)
            # Store filtered ranges for display (raw would be misleading)
            tag.last_ranges = [round(r, 1) for r in filtered_ranges]
            stats['udp_valid'] += 1; stats['tags_seen'].add(tid)
            print(f"[UWB] Tag{tid}  pos=({rx:.1f},{ry:.1f})cm  "
                  f"raw=[{','.join(f'{r:.0f}' for r in raw_ranges)}]  "
                  f"flt=[{','.join(f'{r:.0f}' for r in filtered_ranges)}]  "
                  f"{quality}  {tag.speed_display():.1f}{SPEED_DISPLAY_UNIT}  "
                  f"L1rej={tag._filter.l1_rejects}")
            game_evts = process_race_update(tid, now)
            if connected_clients and event_loop:
                li = race_mgr.get_info(tid, now)
                open_lap = scoring._open.get(tid)
                imu = imu_store.get(tid)
                msg = json.dumps(dict(
                    type="tag_position", tag_id=tid,
                    x=round(rx, 4), y=round(ry, 4),
                    heading=round(tag.heading, 2),
                    is_static=is_static,
                    range=tag.last_ranges,
                    speed=round(0.0 if is_static else tag.speed_display(), 2),
                    speed_ms=round(0.0 if is_static else tag.speed_ms, 3),
                    speed_unit=SPEED_DISPLAY_UNIT,
                    quality=quality, anchor_count=anc_count,
                    timestamp=now, game_events=game_evts,
                    wall_hits=len(col_eng.wall_hits(tid)),
                    car_collisions=len(col_eng.car_events(tid)),
                    current_penalty=round(open_lap._pen,2) if open_lap else 0.0,
                    current_bonus=round(open_lap._bon,2) if open_lap else 0.0,
                    lap_info=li,
                    imu=dict(
                        gz=round(imu.get('gz', 0.0), 4),
                        ax=round(imu.get('ax', 0.0), 3),
                        ay=round(imu.get('ay', 0.0), 3),
                        heading=round(imu.get('heading', 0.0), 2),
                        static=is_static,
                    ) if imu else None,
                    checkpoint_touches=_serialize_cp_touches(), cp_active_lap=_serialize_cp_active()))
                asyncio.run_coroutine_threadsafe(broadcast(msg), event_loop)
            if game_evts:
                asyncio.run_coroutine_threadsafe(broadcast(build_state(now)), event_loop)
        except socket.timeout:
            continue
        except Exception as e:
            if running:
                print(f"[UDP] Error: {e}")
    sock.close()
    print("[UDP] Stopped")

# ═══════════════════════════════════════════════════════════════════════
# WEBSOCKET
# ═══════════════════════════════════════════════════════════════════════

async def broadcast(msg):
    if not connected_clients: return
    stats['ws_sent'] += 1; dead = set()
    for c in connected_clients:
        try: await c.send(msg)
        except: dead.add(c)
    connected_clients.difference_update(dead)


async def handle_client(ws):
    global race_armed, TOTAL_LAPS, tag_to_gp, current_group_id, current_track, track
    cid = f"{ws.remote_address[0]}:{ws.remote_address[1]}"
    print(f"[WS] Connected: {cid}")
    connected_clients.add(ws); stats['ws_clients'] += 1

    try:
        now = time.time()
        await ws.send(json.dumps(dict(
            type="connection", status="connected",
            message="UWB Racing — AT+RANGE format, centimetres, 3-layer filter active",
            timestamp=now,
            server_info=dict(
                udp_port=UDP_PORT, ws_port=WS_PORT,
                anchor_count=ANCHOR_COUNT, tag_count=TAG_COUNT,
                total_laps=TOTAL_LAPS,
                units='centimetres',
                filter=dict(
                    l1_jump_threshold_cm=JUMP_THRESHOLD_CM,
                    l2_median_window=MEDIAN_WINDOW,
                    l3_kalman_R=KALMAN_R,
                    l3_kalman_Q=KALMAN_Q,
                ),
                uptime_seconds=(datetime.now()-stats['start']).total_seconds()),
            anchors={str(k):{"x":v[0],"y":v[1]} for k,v in ANCHOR_POSITIONS.items()},
            track=current_track.to_dict(),
            stats=dict(packets_received=stats['udp_valid'],
                       tags_seen=sorted(list(stats['tags_seen']))))))
        await ws.send(build_state(now))

        async for message in ws:
            try:
                d = json.loads(message); mt = d.get('type')

                if mt == 'ping':
                    await ws.send(json.dumps({"type":"pong","timestamp":time.time()}))

                elif mt == 'admin_start':
                    apply_race_config(d.get('race_config', {}), d.get('total_laps'))

                    nm = d.get('tag_map', {})
                    if nm:
                        tag_to_gp = {}
                        for k, v in nm.items():
                            tag_to_gp[int(k)] = int(v)
                            tag_to_gp[str(k)] = int(v)
                        print(f"[MAP] tag_to_gp = {tag_to_gp}")
                    current_group_id      = d.get('group_id')
                    current_tournament_id = d.get('tournament_id')

                    track_csv = d.get('track_csv', '')
                    if track_csv:
                        td = parse_track_csv(track_csv)
                        if td.is_loaded():
                            current_track = td
                            track = create_track_from_data(td)
                            col_eng.set_track(track)
                            apply_track_data(td)
                            print(f"[TRACK] ✓ center={len(td.center)} inner={len(td.inner)} "
                                  f"outer={len(td.outer)} cp={len(td.checkpoints)}")
                        else:
                            print("[TRACK] CSV parsed but no CENTER points — using defaults")
                    else:
                        print("[TRACK] No track_csv — using current/default track")

                    checkpoint_touch_history.clear(); checkpoint_active_lap.clear()
                    race_mgr.reset(); race_mgr.admin_start(); race_armed = True

                    await broadcast(json.dumps(dict(
                        type="admin_event", event="race_armed",
                        message=f"Race armed – {TOTAL_LAPS} laps",
                        total_laps=TOTAL_LAPS, group_id=current_group_id,
                        track=current_track.to_dict(),
                        race_config=dict(wall_hit_penalty=WALL_HIT_PENALTY,
                                         attacker_penalty=CAR_COLLISION_ATTACKER_PENALTY,
                                         victim_bonus=CAR_COLLISION_VICTIM_BONUS),
                        timestamp=time.time())))
                    print(f"[CMD] Start  group={current_group_id}  laps={TOTAL_LAPS}  "
                          f"wall={WALL_HIT_PENALTY}s  atk={CAR_COLLISION_ATTACKER_PENALTY}s  "
                          f"vic_bonus={CAR_COLLISION_VICTIM_BONUS}s")

                elif mt == 'reset':
                    race_mgr.reset(); col_eng.reset(); race_armed = False
                    tag_to_gp = {}; current_group_id = None; current_tournament_id = None
                    checkpoint_touch_history.clear(); checkpoint_active_lap.clear()
                    for t in tags.values(): t.reset()
                    reset_race_config()
                    await broadcast(json.dumps(dict(type="admin_event", event="race_reset",
                        message="Race reset", timestamp=time.time())))
                    print("[CMD] Reset")

                elif mt == 'get_stats':
                    uptime = (datetime.now()-stats['start']).total_seconds()
                    ts = {t_id: dict(
                              total=t.pkt_total, accepted=t.pkt_accepted,
                              rejected=t.pkt_rejected,
                              accept_pct=round(t.pkt_accepted/t.pkt_total*100,1),
                              last_ranges=t.last_ranges,
                              filter=dict(
                                  l1_rejects=t._filter.l1_rejects,
                                  l2_smoothed=t._filter.l2_smoothed,
                                  l3_updates=t._filter.l3_updates,
                              ))
                          for t_id, t in tags.items() if t.pkt_total > 0}
                    await ws.send(json.dumps(dict(
                        type="stats", udp_total=stats['udp_total'],
                        udp_valid=stats['udp_valid'], udp_invalid=stats['udp_invalid'],
                        ws_sent=stats['ws_sent'], ws_clients=len(connected_clients),
                        tags_seen=sorted(list(stats['tags_seen'])), tag_stats=ts,
                        uptime_seconds=uptime, total_laps=TOTAL_LAPS,
                        group_id=current_group_id, tag_to_gp=tag_to_gp,
                        race_config=dict(wall_hit_penalty=WALL_HIT_PENALTY,
                                         attacker_penalty=CAR_COLLISION_ATTACKER_PENALTY,
                                         victim_bonus=CAR_COLLISION_VICTIM_BONUS),
                        leaderboard=race_mgr.get_leaderboard(),
                        feed=scoring.get_feed(20), timestamp=time.time())))

                elif mt == 'get_state':
                    await ws.send(build_state(time.time()))

                else:
                    print(f"[WS] Unknown cmd '{mt}' from {cid}")

            except json.JSONDecodeError:
                print(f"[WS] Bad JSON from {cid}")
            except Exception as e:
                print(f"[WS] Handler error: {e}")

    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        print(f"[WS] Client error: {e}")
    finally:
        connected_clients.discard(ws)
        print(f"[WS] Disconnected: {cid} | active={len(connected_clients)}")


async def stats_reporter():
    while running:
        await asyncio.sleep(60)
        if not running: break
        up = (datetime.now()-stats['start']).total_seconds()
        tot = stats['udp_total']; val = stats['udp_valid']
        print(f"\n{'═'*60}\nSTATS  uptime={up:.0f}s  "
              f"UDP {val}/{tot} ({val/tot*100 if tot else 0:.0f}%)")
        for tid, t in tags.items():
            if t.pkt_total > 0:
                print(f"  Tag{tid}: {t.pkt_accepted}/{t.pkt_total} "
                      f"({t.pkt_accepted/t.pkt_total*100:.0f}%)  "
                      f"L1rej={t._filter.l1_rejects}  "
                      f"L3upd={t._filter.l3_updates}")
        for i, r in enumerate(race_mgr.get_leaderboard()):
            elp = f"{r['best_elp']:.2f}s" if r['best_elp'] < float('inf') else "—"
            print(f"  {i+1}. {r['car_name']:<8} ELP={elp} Laps={r['laps_done']}")
        print(f"{'═'*60}\n")


async def main():
    global event_loop, running
    event_loop = asyncio.get_event_loop()

    ax = [v[0] for v in ANCHOR_POSITIONS.values()]
    ay = [v[1] for v in ANCHOR_POSITIONS.values()]

    print(f"\n{'═'*60}")
    print(f"  UWB RACING — AT+RANGE parser  |  all units: CENTIMETRES")
    print(f"  UDP={UDP_PORT}  IMU={IMU_UDP_PORT}  WS={WS_PORT}")
    print(f"  Anchors: {dict(ANCHOR_POSITIONS)}")
    print(f"  Field: {max(ax)-min(ax):.2f}cm × {max(ay)-min(ay):.2f}cm")
    print(f"  Filtering pipeline:")
    print(f"    L1 spike rejection  : jump_threshold={JUMP_THRESHOLD_CM}cm")
    print(f"    L2 rolling median   : window={MEDIAN_WINDOW} packets/anchor")
    print(f"    L3 Kalman (x,y)     : R={KALMAN_R}  Q={KALMAN_Q}  dt={KALMAN_DT}s")
    print(f"{'═'*60}\n")

    threading.Thread(target=udp_receiver, daemon=True, name="UDP").start()
    threading.Thread(target=imu_listener, daemon=True, name="IMU").start()
    asyncio.create_task(stats_reporter())
    try:
        async with websockets.serve(handle_client, "0.0.0.0", WS_PORT):
            print(f"[WS] ws://0.0.0.0:{WS_PORT}  ready\n✓ READY\n")
            await asyncio.Future()
    except OSError as e:
        print(f"✗ Port error: {e}"); running = False


def signal_handler(sig, frame):
    global running; running = False
    up = (datetime.now()-stats['start']).total_seconds()
    tot = stats['udp_total']; val = stats['udp_valid']
    print(f"\n{'═'*60}\nSHUTDOWN  uptime={up:.0f}s"
          + (f"  UDP {val}/{tot} ({val/tot*100:.0f}%)" if tot else ""))
    for i, r in enumerate(race_mgr.get_leaderboard()):
        elp = f"{r['best_elp']:.2f}s" if r['best_elp'] < float('inf') else "—"
        print(f"  {i+1}. {r['car_name']}  ELP={elp}  Laps={r['laps_done']}")
    print(f"{'═'*60}\n")
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        signal_handler(None, None)
    except Exception as e:
        print(f"\n✗ FATAL: {e}")
        import traceback; traceback.print_exc()