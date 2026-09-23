#!/usr/bin/python3

import sys
from pathlib import Path

def add_schema_path():
    # Determine starting directory
    if "__file__" in globals():
        start = Path(__file__).resolve().parent
    else:
        # interactive python
        start = Path.cwd()

    for p in [start] + list(start.parents):
        candidate = p / "build" / "schema"
        if candidate.exists():
            sys.path.insert(0, str(candidate))
            return candidate

    raise RuntimeError("Could not locate build/schema directory")

sys.path.append("/usr/lib/python3.8/site-packages")
sys.path.append("/home/chris/hydro-one/components")
add_schema_path()

import time
import sys
import os
import signal
import random
import argparse
import csv
import fabrix
from datetime import datetime
import flatbuffers
from fabrix import rcu
from collections import deque

from CEMS.DSMR.DSMRData import DSMRData
from CEMS.Elkor.InstantReading import InstantReading as ElkorInstantReading
from CEMS.IME.InstantReading import InstantReading as IMEInstantReading
from CEMS.Circutor.InstantReading import InstantReading as EVMeterInstantReading
from CEMS.SolarMainRoof import InstantReading as SolarInstantReading
from CEMS.SolarMainRoof import EnergyReading as SolarEnergyReading
from CEMS.KVStore import KV as KVStoreKV
from CEMS.KVStore import Query as KVStoreQuery
from CEMS.KVStore import Values as KVStoreValues

exit_code = 0
stop = False

# Signal handler to handle Ctrl+C
def interrupt_handler(signum, frame):
    global stop
    if signum == signal.SIGINT or signum == signal.SIGTERM:
        stop = True


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Solar monitor component that calculates the solar production of the main roof and gathers statistics about the whole solar system.")
    parser.add_argument('-n', '--name', required=True, help="Component name")
    parser.add_argument('-r', '--realm', required=True, help="Component realm")
    parser.add_argument('-k', '--kv_store', default='', help="Key/value store name")
    parser.add_argument('-l', '--csv_log', default='', help="Log data to CSV file")
    parser.add_argument('-v', '--verbose', action='store_true', help="Enable verbose logging")
    return parser.parse_args()


class RobustSolarEstimator:
    """Robust estimate of the PV production on phase B.

    Model (call update() exactly once per second):
      1. y1 = ime_B + ev - dsmr_B ; y2 = ime_B + ev - elkor_B
         (a residual is only built when its grid meter value is available)
      2. Median of the last W seconds per residual -> removes glitches
         shorter than W/2 s.
      3. Fuse the available residuals (50/50; independent meter errors).
      4. Outlier gate: if |y - pv| > max(gate, slew) the sample is
         rejected (load-spike window) and pv is held. A deviation that
         persists for max_reject_s consecutive seconds cannot be a
         spike -> pv re-syncs to y and tracking resumes.
      5. Exponential tracking limited by a physical slew rate R (W/s),
         clamped to pv >= deadband (0 below).

    Tuning (defaults): window_s=5, slew_wps=400, gate_w=80, alpha=0.5,
    deadband_w=5, max_reject_s=10.
    """

    def __init__(self, window_s=5, slew_wps=400.0, gate_w=80.0, alpha=0.5,
                 deadband_w=5.0, max_reject_s=10):
        self.window_s = window_s
        self.slew = slew_wps
        self.gate = gate_w
        self.alpha = alpha
        self.deadband = deadband_w
        # Max consecutive seconds a deviation may be rejected. A real level
        # shift (fast cloud) persists; a measurement spike does not. After
        # this many rejections the estimate re-syncs to the measurement.
        self.max_reject = max_reject_s
        self._reject_count = 0
        self.q1 = deque(maxlen=window_s)
        self.q2 = deque(maxlen=window_s)
        self.pv = None  # current estimate in W

    def update(self, ime_b, ev, dsmr_b=None, elkor_b=None):
        """Feed the newest value of each meter once per second.
        dsmr_b / elkor_b may be None when that grid meter is not fresh.
        Returns the robust PV estimate in W (>= 0)."""
        if dsmr_b is not None:
            self.q1.append(ime_b + ev - dsmr_b)
        if elkor_b is not None:
            self.q2.append(ime_b + ev - elkor_b)
        full = [q for q in (self.q1, self.q2) if len(q) >= self.window_s]
        if not full:
            avail = list(self.q1) + list(self.q2)
            if not avail:
                return 0.0 if self.pv is None else self.pv
            self.pv = max(0.0, sum(avail) / len(avail))  # warm-up
            return self.pv
        medians = [sorted(q)[len(q) // 2] for q in full]
        y = sum(medians) / len(medians)
        if self.pv is None:
            self.pv = max(0.0, y)
            return self.pv
        if abs(y - self.pv) > max(self.gate, self.slew):
            self._reject_count += 1
            if self._reject_count < self.max_reject:
                # corrupted sample (load spike / meter glitch): hold
                pass
            else:
                # Sustained deviation: cannot be a spike -> re-sync.
                self._reject_count = 0
                self.pv = y
        else:
            self._reject_count = 0
            step = self.alpha * (y - self.pv)
            step = max(-self.slew, min(self.slew, step))
            self.pv += step
        if self.pv < self.deadband:
            self.pv = 0.0
        return self.pv


class Solar(fabrix.Component):
    """Solar monitor component: robust PV estimate, energy accumulation (Wh,
    persisted in the kv_store) and publishing of InstantReading/EnergyReading."""

    _COMPONENT_NAME_DSMR = "dsmr"
    _COMPONENT_NAME_ELKOR = "elkor"
    _COMPONENT_NAME_IME = "ime"
    _COMPONENT_NAME_CIRCUTOR = "ev_meter"
    _COMPONENT_NAME_KV = "kv_store"

    _AREA_NAME_INSTANT_READING = "InstantReading"
    _AREA_NAME_ENERGY_READING = "EnergyReading"
    _TOPIC_NAME_DSMR_DATA = "DSMRData"
    _TOPIC_INSTANT_READING = "InstantReading"
    _TOPIC_ENERGY_READING = "EnergyReading"

    # A meter value older than this is considered stale (not fed to the estimator).
    _METER_MAX_AGE_S = 5.0
    # Warn at most this often about stale meters (0 = disabled).
    _STALE_WARN_INTERVAL_S = 60.0
    # Persist energy values to the kv_store at most this often (s).
    # Each put costs the kv_store ~1 s (sqlite fsync), so keep this moderate.
    _ENERGY_PERSIST_S = 300.0
    # Retry the startup restore at most this often (s) until all responses arrive.
    _KV_RESTORE_RETRY_S = 30.0
    # Publish the EnergyReading RCU at most this often (s); independent of kv_store.
    _ENERGY_PUBLISH_S = 60.0
    # cr_identifiers used to link kv_store get-requests with their responses.
    _CR_GET_DAY = 1
    _CR_GET_MONTH = 2
    _CR_GET_YEAR = 3
    # Ignore integration gaps larger than this (s); the process was stalled.
    _MAX_INTEGRATION_GAP_S = 10.0

    def __init__(self, *args, **kwargs):
        """Constructor."""
        self._csv_log_name = kwargs.pop('csv_log', '')
        self._kv_store_name = kwargs.pop('kv_store', '')
        self._verbose = kwargs.pop('verbose', False)
        super().__init__(*args, **kwargs)
        self._estimator = RobustSolarEstimator()
        self._last_stale_warn = 0.0
        self._solar_dsmr = 0
        self._solar_elkor = 0
        self._dsmr_net = [0]
        self._dsmr_timestamp = 0
        self._elkor_net = [0, 0]
        self._elkor_timestamp = 0
        self._ime_net = 0
        self._ime_timestamp = 0
        self._circutor_net = 0
        self._circutor_timestamp = 0
        # Energy accumulation (Wh) and persistence via kv_store.
        # Each key has ONE fixed timestamp (start of its period), so every
        # put is an INSERT OR REPLACE onto the same row: exactly one row per
        # key in the table, no matter how often we persist.
        self._kv_endpoint = fabrix.Endpoint()
        self._last_command = ""
        self._day_key = ''
        self._day_ts = 0.0
        self._month_key = ''
        self._month_ts = 0.0
        self._year_key = ''
        self._year_ts = 0.0
        self._restore_ok = {self._CR_GET_DAY: False, self._CR_GET_MONTH: False, self._CR_GET_YEAR: False}
        self._restore_requested_ts = 0.0
        self._energy_day_wh = 0.0
        self._energy_month_wh = 0.0
        self._energy_year_wh = 0.0
        self._last_energy_ts = 0.0
        self._last_energy_persist = 0.0
        self._last_energy_publish = 0.0
        self._csv_file = None
        self._csv_writer = None

    def _fresh(self, timestamp, now, max_age=None):
        """True if the meter value at timestamp is younger than max_age."""
        if max_age is None:
            max_age = self._METER_MAX_AGE_S
        return timestamp != 0 and timestamp + max_age > now

    def _warn_stale_meters(self, now):
        """Warn once per interval about meters that have no fresh value."""
        if self._STALE_WARN_INTERVAL_S <= 0 or now - self._last_stale_warn < self._STALE_WARN_INTERVAL_S:
            return
        stale = []
        for label, ts in (('elkor', self._elkor_timestamp),
                          ('dsmr', self._dsmr_timestamp),
                          ('ime', self._ime_timestamp),
                          ('circutor', self._circutor_timestamp)):
            if not self._fresh(ts, now):
                stale.append(f"{label} (last message {now - ts:.0f}s ago)" if ts else f"{label} (no message yet)")
        if stale:
            self._last_stale_warn = now
            print(f"WARNING: stale or missing meters: {', '.join(stale)}", flush=True)

    def _period(self):
        """Current local day/month/year keys with their FIXED timestamps
        (start of period), used so each key maps to exactly one row."""
        now_dt = datetime.now()
        day_key = f"solar.energy.wh.day.{now_dt:%Y-%m-%d}"
        month_key = f"solar.energy.wh.month.{now_dt:%Y-%m}"
        year_key = f"solar.energy.wh.year.{now_dt:%Y}"
        return (day_key, datetime(now_dt.year, now_dt.month, now_dt.day).timestamp(),
                month_key, datetime(now_dt.year, now_dt.month, 1).timestamp(),
                year_key, datetime(now_dt.year, 1, 1).timestamp())

    def _request_kv_restore(self):
        """Ask the kv_store for the stored day/month/year energy values."""
        if not self._kv_endpoint:
            return
        self._day_key, self._day_ts, self._month_key, self._month_ts, self._year_key, self._year_ts = self._period()
        self._send_kv_get(self._CR_GET_DAY, self._day_key, self._day_ts)
        self._send_kv_get(self._CR_GET_MONTH, self._month_key, self._month_ts)
        self._send_kv_get(self._CR_GET_YEAR, self._year_key, self._year_ts)
        self._restore_requested_ts = time.time()
        if self._verbose:
            print(f"Restoring energy values: day={self._day_key} month={self._month_key} year={self._year_key}", flush=True)

    @staticmethod
    def _kv_value_bytes(entry):
        """Extract the raw value blob from a Value entry, tolerating the
        different flatc python codegen styles for [ubyte] fields."""
        fn = getattr(entry, "ValueAsNumpy", None)
        if fn:
            arr = fn()
            if arr is not None:
                return bytes(arr)
        try:
            v = entry.Value()          # object-api: whole vector
        except TypeError:
            n = entry.ValueLength()    # classic: element accessor Value(j)
            return bytes(entry.Value(i) for i in range(n))
        return bytes(v)

    def _send_kv_get(self, cr_identifier, key, from_ts):
        """Build the Query buffer (flatc function-style codegen;
        'from' field is exposed as From_ because it is a Python keyword)."""
        builder = flatbuffers.Builder(256)
        key_off = builder.CreateString(key)
        KVStoreQuery.QueryStart(builder)
        KVStoreQuery.QueryAddKey(builder, key_off)
        KVStoreQuery.QueryAddFrom_(builder, from_ts)
        q = KVStoreQuery.QueryEnd(builder)
        builder.Finish(q)
        self._last_command = f"get cr={cr_identifier} key={key}"
        self.command(self._kv_endpoint, "get", 0, cr_identifier, builder.Output())

    def _kv_put(self, key, wh, period_ts):
        """Store one energy value (text float, Wh) under key with the key's
        fixed period timestamp -> INSERT OR REPLACE onto the same row."""
        if not self._kv_endpoint:
            return
        builder = flatbuffers.Builder(256)
        key_off = builder.CreateString(key)
        val_off = builder.CreateByteVector(("%.1f" % wh).encode("ascii"))
        KVStoreKV.KVStart(builder)
        KVStoreKV.KVAddKey(builder, key_off)
        KVStoreKV.KVAddValue(builder, val_off)
        KVStoreKV.KVAddTimestamp(builder, period_ts)
        kv_off = KVStoreKV.KVEnd(builder)
        builder.Finish(kv_off)
        self._last_command = f"put key={key}"
        self.command(self._kv_endpoint, "put", 0, 0, builder.Output())

    def _persist_kv(self, now):
        """Write day/month/year to the kv_store (fixed timestamps per key)."""
        self._last_energy_persist = now  # schedule first: never hammer a busy kv_store
        try:
            self._kv_put(self._day_key, self._energy_day_wh, self._day_ts)
            self._kv_put(self._month_key, self._energy_month_wh, self._month_ts)
            self._kv_put(self._year_key, self._energy_year_wh, self._year_ts)
        except Exception as e:
            print(f"WARNING: kv_store persist failed: {e}", flush=True)
        if self._verbose:
            print(f"Energy persisted: day {self._energy_day_wh:.1f} Wh  month {self._energy_month_wh:.1f} Wh  year {self._energy_year_wh:.1f} Wh", flush=True)

    def _accumulate_energy(self, dt, pv_w):
        """Add pv_w * dt (Wh) to day/month/year, handling period rollovers."""
        day_key, day_ts, month_key, month_ts, year_key, year_ts = self._period()
        if self._day_key and day_key != self._day_key:
            # New day: freeze the old day's final value (same row: fixed ts).
            self._kv_put(self._day_key, self._energy_day_wh, self._day_ts)
            self._energy_day_wh = 0.0
        if self._month_key and month_key != self._month_key:
            # New month: freeze the old month's final value.
            self._kv_put(self._month_key, self._energy_month_wh, self._month_ts)
            self._energy_month_wh = 0.0
        if self._year_key and year_key != self._year_key:
            # New year: freeze the old year's final value.
            self._kv_put(self._year_key, self._energy_year_wh, self._year_ts)
            self._energy_year_wh = 0.0
        if not self._day_key:
            self._day_key, self._day_ts = day_key, day_ts
            self._month_key, self._month_ts = month_key, month_ts
            self._year_key, self._year_ts = year_key, year_ts
        add = pv_w * dt / 3600.0  # W * s -> Wh
        self._energy_day_wh += add
        self._energy_month_wh += add
        self._energy_year_wh += add

    def _publish_energy(self, ts):
        """Publish the EnergyReading area/topic with the Wh accumulations."""
        builder = flatbuffers.Builder(1024)
        offset = SolarEnergyReading.CreateEnergyReading(builder, ts,
                                                        self._energy_day_wh,
                                                        self._energy_month_wh,
                                                        self._energy_year_wh)
        builder.Finish(offset)
        storage = self._energy_reading_area.create_storage(builder.Output())
        self._energy_reading_area.publish_storage(storage)
        self._energy_reading_area.tick()
        self._energy_reading_area.reclaim()
        self.broadcast_topic(self._TOPIC_ENERGY_READING, builder.Output())

    def run(self):
        """Main loop: advance RCU, then tick the robust estimator once per second."""
        self._next_timepoint = time.time() + 1.0
        while not stop:
            self.process_until(self._next_timepoint)
            now = time.time()
            self._warn_stale_meters(now)
            # IME is the anchor (it carries the home load on phase B);
            # skip the tick while it has no fresh value.
            if self._fresh(self._ime_timestamp, now):
                ev = self._circutor_net if self._fresh(self._circutor_timestamp, now) else 0.0
                dsmr_b = self._dsmr_net[0] if self._fresh(self._dsmr_timestamp, now) else None
                elkor_b = self._elkor_net[0] if self._fresh(self._elkor_timestamp, now) else None
                self._estimator.update(self._ime_net, ev, dsmr_b, elkor_b)
                self._publish(self._estimator.pv)
                # Integrate production since the last tick.
                if self._last_energy_ts:
                    dt = now - self._last_energy_ts
                    self._last_energy_ts = now
                    if 0.0 < dt <= self._MAX_INTEGRATION_GAP_S:
                        self._accumulate_energy(dt, self._estimator.pv or 0.0)
                    else:
                        # Stalled or long gap: don't fabricate energy.
                        self._last_energy_ts = now
                else:
                    self._last_energy_ts = now
            # EnergyReading RCU: published on its own schedule, independent
            # of kv_store availability (kv_store problems must not break
            # the RCU or the topic).
            if now - self._last_energy_publish >= self._ENERGY_PUBLISH_S:
                self._publish_energy(now)
                self._last_energy_publish = now
            # kv_store persistence: only once the startup restore is verified,
            # otherwise a failed restore would overwrite the stored totals
            # with values accumulated since restart only.
            if self._kv_endpoint:
                if not all(self._restore_ok.values()):
                    if now - self._restore_requested_ts >= self._KV_RESTORE_RETRY_S:
                        self._request_kv_restore()  # retry until all responses arrive
                elif now - self._last_energy_persist >= self._ENERGY_PERSIST_S:
                    self._persist_kv(now)
            self._next_timepoint += 1.0

    def _on_start(self):
        """Only called once during start of the component."""
        print(f"Component {self.identifier().name()} is online with pid {os.getpid()}.")
        self._instant_reading_area = rcu.create_area(self.public_endpoint(), self._AREA_NAME_INSTANT_READING)
        self._energy_reading_area = rcu.create_area(self.public_endpoint(), self._AREA_NAME_ENERGY_READING)
        self._instant_reading_area.grace_period(60)
        self._energy_reading_area.grace_period(60)
        for name in self.list_components(True):
            if name == self._COMPONENT_NAME_DSMR and (endpoint := self._open_endpoint(name)).is_open():
                self.subscribe(endpoint, self._TOPIC_NAME_DSMR_DATA)
            elif name == self._COMPONENT_NAME_ELKOR and (endpoint := self._open_endpoint(name)).is_open():
                self.subscribe(endpoint, self._TOPIC_INSTANT_READING)
            elif name == self._COMPONENT_NAME_IME and (endpoint := self._open_endpoint(name)).is_open():
                self.subscribe(endpoint, self._TOPIC_INSTANT_READING)
            elif name == self._COMPONENT_NAME_CIRCUTOR and (endpoint := self._open_endpoint(name)).is_open():
                self.subscribe(endpoint, self._TOPIC_INSTANT_READING)
            elif name == self._COMPONENT_NAME_KV and (endpoint := self._open_endpoint(name)).is_open():
                self._kv_endpoint = endpoint
                self._request_kv_restore()
        if self._csv_log_name != '':
            self._csv_file = open(self._csv_log_name, 'a', encoding='utf-8-sig', newline='')
            self._csv_writer = csv.writer(self._csv_file, delimiter=';')
            if self._csv_file.tell() == 0:
                self._csv_writer.writerow(['timestamp', 'delta_t_dsmr', 'dsmr', 'delta_t_elkor', 'elkor', 'delta_t_ime', 'ime', 'delta_t_circutor', 'circutor', 'solar_dsmr', 'solar_elkor', 'solar_robust'])

    def _on_endpoint_create(self, name, is_private):
        """When a new known endpoint is created find RCU areas."""
        if is_private: return # Ignore private endpoints
        if name == self._COMPONENT_NAME_DSMR and (endpoint := self._open_endpoint(name)).is_open():
            self.subscribe(endpoint, self._TOPIC_NAME_DSMR_DATA)
        elif name == self._COMPONENT_NAME_ELKOR and (endpoint := self._open_endpoint(name)).is_open():
            self.subscribe(endpoint, self._TOPIC_INSTANT_READING)
        elif name == self._COMPONENT_NAME_IME and (endpoint := self._open_endpoint(name)).is_open():
            self.subscribe(endpoint, self._TOPIC_INSTANT_READING)
        elif name == self._COMPONENT_NAME_CIRCUTOR and (endpoint := self._open_endpoint(name)).is_open():
            self.subscribe(endpoint, self._TOPIC_INSTANT_READING)
        elif name == self._COMPONENT_NAME_KV and not self._kv_endpoint and (endpoint := self._open_endpoint(name)).is_open():
            self._kv_endpoint = endpoint
            self._request_kv_restore()

    def _on_endpoint_remove(self, name):
        """When an endpoint is removed, let the user know."""
        if name == self._COMPONENT_NAME_DSMR:
            self._dsmr_net = [0]
            self._dsmr_timestamp = 0
            self._solar_dsmr = 0
        elif name == self._COMPONENT_NAME_ELKOR:
            self._elkor_net = [0, 0]
            self._elkor_timestamp = 0
            self._solar_elkor = 0
        elif name == self._COMPONENT_NAME_IME:
            self._ime_net = 0
            self._ime_timestamp = 0
        elif name == self._COMPONENT_NAME_CIRCUTOR:
            self._circutor_net = 0
            self._circutor_timestamp = 0
        elif name == self._COMPONENT_NAME_KV:
            # In-memory accumulations stay; a re-create re-requests restore.
            self._kv_endpoint = fabrix.Endpoint()
            for cr in self._restore_ok:
                self._restore_ok[cr] = False

    def _on_command_response(self, *args):
        """Handle kv_store command responses.

        Expected signature: (sender_endpoint, timestamp, command, priority,
        cr_identifier, data, result_code). Tolerates a trailing size field
        in case the binding passes (data, size, result_code).
        """
        try:
            command = args[2]
            cr_identifier = args[4]
            data = args[5]
            result_code = args[6] if len(args) > 6 else (args[7] if len(args) > 7 else 0)
        except IndexError:
            return
        if command == "put":
            if result_code != 0:
                print(f"WARNING: kv_store put failed with result code {result_code}", flush=True)
            return
        if command != "get":
            return
        if cr_identifier in self._restore_ok:
            self._restore_ok[cr_identifier] = True  # a response == store is reachable
        if result_code != 0 or data is None:
            return  # -1 == key not found; nothing to restore
        data = bytes(data)  # normalise memoryview/numpy to plain bytes for flatbuffers
        values = KVStoreValues.Values.GetRootAs(data, 0)
        n = values.ValuesLength()
        if n == 0:
            return
        latest = None
        for i in range(n):
            entry = values.Values(i)
            if latest is None or entry.Timestamp() > latest.Timestamp():
                latest = entry
        try:
            wh = float(self._kv_value_bytes(latest).decode('ascii', 'ignore'))
        except ValueError:
            print(f"WARNING: unparseable kv value for cr {cr_identifier}", flush=True)
            return
        if cr_identifier == self._CR_GET_DAY:
            self._energy_day_wh = wh
        elif cr_identifier == self._CR_GET_MONTH:
            self._energy_month_wh = wh
        elif cr_identifier == self._CR_GET_YEAR:
            self._energy_year_wh = wh
        # Resume integration from now: energy between the last persisted
        # value and this moment is lost (bounded by the persist interval
        # plus the downtime), but we never double-count it.
        self._last_energy_ts = time.time()
        if self._verbose:
            print(f"Restored cr={cr_identifier}: {wh:.1f} Wh (row ts {latest.Timestamp():.0f})", flush=True)

    def _on_subscribe_request(self, sender_endpoint, delivery_endpoint, topic_name):
        print(f"Received subscribe request from '{sender_endpoint.identifier().name()}' for topic '{topic_name}'")
        return topic_name == self._TOPIC_INSTANT_READING or topic_name == self._TOPIC_ENERGY_READING

    def _on_unsubscribe_request(self, sender_endpoint, delivery_endpoint, topic_name):
        print(f"Received unsubscribe request from '{sender_endpoint.identifier().name()}' for topic '{topic_name}'")
        return topic_name == self._TOPIC_INSTANT_READING or topic_name == self._TOPIC_ENERGY_READING

    def _on_list_topics_request(self, sender_endpoint, topics):
        topics.append(self._TOPIC_INSTANT_READING)
        topics.append(self._TOPIC_ENERGY_READING)

    def _on_topic(self, sender_endpoint, timestamp, topic_name, data):
        """Store the newest meter values; the estimator is ticked in run()."""
        sender_name = sender_endpoint.identifier().name()
        if sender_name == self._COMPONENT_NAME_CIRCUTOR and topic_name == self._TOPIC_INSTANT_READING:
            ir = EVMeterInstantReading()
            ir.Init(data, 0)
            self._circutor_net = ir.Phase2ActivePower()
            self._circutor_timestamp = ir.Timestamp()
            self._solar_elkor = self._circutor_net + self._ime_net - self._elkor_net[0]
            self._solar_dsmr = self._ime_net + self._circutor_net - self._dsmr_net[0]
        elif sender_name == self._COMPONENT_NAME_DSMR and topic_name == self._TOPIC_NAME_DSMR_DATA:
            dsmr_data = DSMRData.GetRootAs(data, 0)
            dsmr_instant = dsmr_data.Instant()
            self._dsmr_net = self._dsmr_net[1:] + [(dsmr_instant.Phase3Consumption() - dsmr_instant.Phase3Injection()) * 1000.0] # L3 == Phase B
            self._dsmr_timestamp = dsmr_data.Timestamp()
            self._solar_dsmr = self._ime_net + self._circutor_net - self._dsmr_net[0]
        elif sender_name == self._COMPONENT_NAME_ELKOR and topic_name == self._TOPIC_INSTANT_READING:
            ir = ElkorInstantReading()
            ir.Init(data, 0)
            self._elkor_net = self._elkor_net[1:] + [ir.PhaseBRealPower()]
            self._elkor_timestamp = ir.Timestamp()
            self._solar_elkor = self._circutor_net + self._ime_net - self._elkor_net[0]
        elif sender_name == self._COMPONENT_NAME_IME and topic_name == self._TOPIC_INSTANT_READING:
            ir = IMEInstantReading()
            ir.Init(data, 0)
            self._ime_net = ir.Phase2ActivePower()
            self._ime_timestamp = ir.Timestamp()
            self._solar_dsmr = self._ime_net + self._circutor_net - self._dsmr_net[0]
            self._solar_elkor = self._circutor_net + self._ime_net - self._elkor_net[0]
        if self._csv_log_name != '' and (self._dsmr_timestamp != 0 or self._elkor_timestamp != 0) and self._ime_timestamp != 0:
            delta_t_dsmr = timestamp - self._dsmr_timestamp
            delta_t_elkor = timestamp - self._elkor_timestamp
            delta_t_ime = timestamp - self._ime_timestamp
            delta_t_circutor = timestamp - self._circutor_timestamp
            self._csv_writer.writerow((timestamp, delta_t_dsmr, self._dsmr_net[-1], delta_t_elkor, self._elkor_net[-1], delta_t_ime, self._ime_net, delta_t_circutor, self._circutor_net, self._solar_dsmr, self._solar_elkor, self._estimator.pv if self._estimator.pv is not None else 0.0))

    def _publish(self, raw_production):
        """Publish solar production using the robust estimate with a night time-gate."""
        if raw_production is None:
            raw_production = 0.0

        # Prevent ghost readings at night via basic time-gating (we use the maxima of summer)
        now = datetime.now()
        current_hour = now.hour
        current_minute = now.minute
        if (current_hour < 4) or (current_hour == 4 and current_minute < 46) or (current_hour == 22 and current_minute >= 39) or (current_hour >= 23):
            solar_production = 0.0
        else:
            # The estimator already applies a deadband; keep a small one here as a safety net.
            DEADBAND_THRESHOLD_W = 5.0
            if raw_production > DEADBAND_THRESHOLD_W:
                solar_production = raw_production
            else:
                solar_production = 0.0

        # Build and publish FlatBuffer packet
        builder = flatbuffers.Builder(1024)
        offset = SolarInstantReading.CreateInstantReading(builder, self._next_timepoint, solar_production, self._solar_elkor, self._solar_dsmr)
        builder.Finish(offset)

        storage = self._instant_reading_area.create_storage(builder.Output())
        self._instant_reading_area.publish_storage(storage)
        self.broadcast_topic(self._TOPIC_INSTANT_READING, builder.Output())

        self._instant_reading_area.tick()
        self._instant_reading_area.reclaim()
        self._energy_reading_area.tick()
        self._energy_reading_area.reclaim()

        if self._verbose:
            print(f"Time: {self._next_timepoint}  Solar production: {solar_production}")

    def _on_error(self, other_end, error_code):
        """Print errors."""
        name = other_end.identifier().name() if other_end else '<>'
        msg = f"Error: {name} with error code {fabrix.EnumNameErrorCode(error_code)}"
        if name == self._COMPONENT_NAME_KV and self._last_command:
            msg += f" (last command: {self._last_command})"
        print(msg, flush=True)

    def _on_halt_component_request(self, sender_endpoint):
        """Accept a halt request and persist the current energy values."""
        global stop
        if self._kv_endpoint and self._day_key:
            try:
                self._persist_kv(time.time())
            except Exception:
                pass
        stop = True
        return True

def main():
    """Main function"""
    global exit_code

    # Parse command line arguments
    try:
        args = parse_arguments()
    except SystemExit:
        return 1
    except Exception as e:
        print(f"Error parsing arguments: {e}", file=sys.stderr)
        return 1

    # Random seed
    random.seed()

    # Register 'break' handler
    signal.signal(signal.SIGINT, interrupt_handler)
    signal.signal(signal.SIGTERM, interrupt_handler)

    try:
        # Create and run component
        component = Solar(args.name, args.realm, verbose=args.verbose, kv_store=args.kv_store, csv_log=args.csv_log)
        component.run()
    except Exception as e:
        print(f"Component execution failed: {e}", file=sys.stderr)
        exit_code = 1
    finally:
        # Cleanup
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

    return exit_code

if __name__ == "__main__":
   sys.exit(main())
