# iperfer_client_agent_infinite.py
#
# Client-side iperfer agent with finite/infinite duration support.
#
# Architecture:
# - Sending interface: used for Firebase POST telemetry. It must have internet access.
# - Probing interface: used by iperf3 with -B. It can be Wi-Fi, 5G, Ethernet, USB, etc.
# - Target IP: actual iperf3 destination.
# - The website/dashboard receives Firebase records under iperf3_streams and plots by streamCode/interfaceName.
#
# Firebase REST endpoint:
#   https://test-c7bf3-default-rtdb.firebaseio.com/iperf3_streams.json
#
# Requirements:
#   pip install psutil requests
#
# Run:
#   python iperfer_client_agent_infinite.py

import csv
import os
import platform
import queue
import re
import shutil
import signal
import socket
import statistics
import subprocess
import threading
import time
import tkinter as tk
from dataclasses import dataclass
from datetime import datetime
from tkinter import messagebox, scrolledtext, ttk
from typing import List, Optional

import psutil
import requests
from requests.adapters import HTTPAdapter


# =========================
# Firebase configuration
# =========================

FIREBASE_DATABASE_URL = "https://test-c7bf3-default-rtdb.firebaseio.com"
FIREBASE_BRANCH = "iperf3_streams"
FIREBASE_POST_URL = f"{FIREBASE_DATABASE_URL}/{FIREBASE_BRANCH}.json"


# =========================
# iperf defaults
# =========================

DEFAULT_TARGET_IP = "192.168.0.138"
DEFAULT_TARGET_PORT = 5201
DEFAULT_DURATION_SECONDS = 60
DEFAULT_INTERVAL_SECONDS = 1.0
DEFAULT_PARALLEL_STREAMS = 1
DEFAULT_PROTOCOL = "TCP"
DEFAULT_UDP_BANDWIDTH = "10M"

LOG_DIR = "iperfer_client_logs"


# =========================
# Data models
# =========================

@dataclass
class InterfaceInfo:
    name: str
    ip: str
    netmask: str = ""

    @property
    def label(self) -> str:
        return f"{self.name} | {self.ip}"


@dataclass
class IperfSample:
    elapsed_start_sec: float
    elapsed_end_sec: float
    throughput_mbps: float
    transfer_mbytes: float
    raw_line: str


@dataclass
class RuntimeConfig:
    firebase_url: str
    stream_code: str

    sending_interface_name: str
    sending_interface_ip: str

    probing_interface_name: str
    probing_interface_ip: str

    target_ip: str
    target_port: int
    infinite_duration: bool
    duration_sec: Optional[int]
    interval_sec: float
    parallel_streams: int
    protocol: str
    udp_bandwidth: str


class RunningStats:
    def __init__(self):
        self.values = []

    def clear(self):
        self.values.clear()

    def add(self, value: float):
        self.values.append(float(value))

    @property
    def count(self):
        return len(self.values)

    @property
    def current(self):
        return self.values[-1] if self.values else 0.0

    @property
    def average(self):
        return statistics.fmean(self.values) if self.values else 0.0

    @property
    def minimum(self):
        return min(self.values) if self.values else 0.0

    @property
    def maximum(self):
        return max(self.values) if self.values else 0.0

    @property
    def std(self):
        return statistics.pstdev(self.values) if len(self.values) >= 2 else 0.0


# =========================
# HTTP adapter bound to source IP
# =========================

class SourceAddressAdapter(HTTPAdapter):
    """
    Forces requests/urllib3 sockets to bind to a specific local source IP.

    This is how the telemetry POST path can be forced through the selected
    sending interface, assuming the OS routing table permits that source IP
    to reach Firebase.
    """

    def __init__(self, source_ip: str, **kwargs):
        self.source_ip = source_ip
        self.source_address = (source_ip, 0)
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        pool_kwargs["source_address"] = self.source_address
        return super().init_poolmanager(
            connections,
            maxsize,
            block=block,
            **pool_kwargs
        )

    def proxy_manager_for(self, proxy, **proxy_kwargs):
        proxy_kwargs["source_address"] = self.source_address
        return super().proxy_manager_for(proxy, **proxy_kwargs)


# =========================
# Helper functions
# =========================

def is_loopback_ip(ip_address: str) -> bool:
    return ip_address.startswith("127.") or ip_address == "::1"


def validate_ipv4(ip_address: str, field_name: str) -> None:
    try:
        socket.inet_aton(ip_address)
    except OSError:
        raise ValueError(f"{field_name} is invalid: {ip_address}")

    if ip_address.count(".") != 3:
        raise ValueError(f"{field_name} is invalid: {ip_address}")


def discover_ipv4_interfaces(include_loopback: bool = False) -> List[InterfaceInfo]:
    interfaces = []
    stats_by_interface = psutil.net_if_stats()
    addrs_by_interface = psutil.net_if_addrs()

    for interface_name, addresses in addrs_by_interface.items():
        interface_stats = stats_by_interface.get(interface_name)

        if interface_stats is not None and not interface_stats.isup:
            continue

        for addr in addresses:
            if addr.family != socket.AF_INET:
                continue

            ip = addr.address

            if not include_loopback and is_loopback_ip(ip):
                continue

            interfaces.append(
                InterfaceInfo(
                    name=interface_name,
                    ip=ip,
                    netmask=addr.netmask or ""
                )
            )

    return interfaces


def convert_bandwidth_to_mbps(value: float, unit: str) -> float:
    unit = unit.lower()

    if unit == "bits":
        return value / 1_000_000.0

    if unit == "kbits":
        return value / 1_000.0

    if unit == "mbits":
        return value

    if unit == "gbits":
        return value * 1_000.0

    raise ValueError(f"Unsupported bandwidth unit: {unit}")


def convert_transfer_to_mbytes(value: float, unit: str) -> float:
    unit = unit.lower()

    if unit == "bytes":
        return value / 1_000_000.0

    if unit == "kbytes":
        return value / 1_000.0

    if unit == "mbytes":
        return value

    if unit == "gbytes":
        return value * 1_000.0

    raise ValueError(f"Unsupported transfer unit: {unit}")


IPERF_INTERVAL_PATTERN = re.compile(
    r"\[\s*(?P<stream_id>[^\]]+)\]\s+"
    r"(?P<start>[0-9]+(?:\.[0-9]+)?)\s*-\s*"
    r"(?P<end>[0-9]+(?:\.[0-9]+)?)\s+sec\s+"
    r"(?P<transfer>[0-9]+(?:\.[0-9]+)?)\s+"
    r"(?P<transfer_unit>[KMG]?Bytes)\s+"
    r"(?P<bandwidth>[0-9]+(?:\.[0-9]+)?)\s+"
    r"(?P<bandwidth_unit>[KMG]?bits)/sec",
    flags=re.IGNORECASE
)


def parse_iperf_interval_line(line: str, parallel_streams: int) -> Optional[IperfSample]:
    lower_line = line.lower()

    if "bits/sec" not in lower_line:
        return None

    if "sec" not in lower_line:
        return None

    # Ignore final summary rows. We only want interval rows.
    if "sender" in lower_line or "receiver" in lower_line:
        return None

    match = IPERF_INTERVAL_PATTERN.search(line)

    if not match:
        return None

    stream_id = match.group("stream_id").strip().lower()

    # With multiple parallel streams, iperf3 prints individual stream rows and [SUM].
    # We only send the [SUM] row to Firebase to avoid double counting.
    if parallel_streams > 1 and "sum" not in stream_id:
        return None

    if parallel_streams <= 1 and "sum" in stream_id:
        return None

    elapsed_start = float(match.group("start"))
    elapsed_end = float(match.group("end"))

    transfer_value = float(match.group("transfer"))
    transfer_unit = match.group("transfer_unit")

    bandwidth_value = float(match.group("bandwidth"))
    bandwidth_unit = match.group("bandwidth_unit")

    throughput_mbps = convert_bandwidth_to_mbps(bandwidth_value, bandwidth_unit)
    transfer_mbytes = convert_transfer_to_mbytes(transfer_value, transfer_unit)

    return IperfSample(
        elapsed_start_sec=elapsed_start,
        elapsed_end_sec=elapsed_end,
        throughput_mbps=throughput_mbps,
        transfer_mbytes=transfer_mbytes,
        raw_line=line
    )


def command_supports_forceflush() -> bool:
    try:
        result = subprocess.run(
            ["iperf3", "--help"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=5
        )
        return "--forceflush" in result.stdout
    except Exception:
        return False


def make_bound_session(source_ip: str) -> requests.Session:
    session = requests.Session()
    adapter = SourceAddressAdapter(source_ip)

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


# =========================
# Main GUI app
# =========================

class IperferClientAgentApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Iperfer Client Agent - Infinite Duration Supported")
        self.root.geometry("1320x900")
        self.root.minsize(1160, 790)

        self.interfaces: List[InterfaceInfo] = []

        self.running = False
        self.stop_event = threading.Event()

        self.iperf_process = None
        self.start_time = None

        self.gui_queue = queue.Queue()
        self.post_queue = queue.Queue()

        self.post_session = None
        self.runtime_config: Optional[RuntimeConfig] = None

        self.posted_count = 0
        self.failed_post_count = 0
        self.parsed_sample_count = 0
        self.stats = RunningStats()

        self.csv_file = None
        self.csv_writer = None
        self.csv_path = None

        self.forceflush_supported = command_supports_forceflush()

        self._configure_style()
        self._build_ui()
        self._refresh_interfaces()
        self._schedule_gui_queue()

    # =========================
    # UI
    # =========================

    def _configure_style(self):
        style = ttk.Style()

        try:
            style.theme_use("clam")
        except tk.TclError:
            pass

        style.configure("Title.TLabel", font=("Arial", 18, "bold"))
        style.configure("Section.TLabelframe.Label", font=("Arial", 11, "bold"))
        style.configure("Status.TLabel", font=("Arial", 10))
        style.configure("Good.TLabel", foreground="green", font=("Arial", 10, "bold"))
        style.configure("Bad.TLabel", foreground="red", font=("Arial", 10, "bold"))
        style.configure("Metric.TLabel", font=("Arial", 14, "bold"))

    def _build_ui(self):
        outer = ttk.Frame(self.root, padding=12)
        outer.pack(fill=tk.BOTH, expand=True)

        header = ttk.Frame(outer)
        header.pack(fill=tk.X)

        ttk.Label(
            header,
            text="Iperfer Client Agent",
            style="Title.TLabel"
        ).pack(side=tk.LEFT)

        self.status_var = tk.StringVar(value="Stopped")
        self.status_label = ttk.Label(header, textvariable=self.status_var, style="Bad.TLabel")
        self.status_label.pack(side=tk.RIGHT)

        self._build_config_panel(outer)
        self._build_metrics_panel(outer)
        self._build_command_panel(outer)
        self._build_output_panel(outer)

    def _build_config_panel(self, outer):
        panel = ttk.LabelFrame(
            outer,
            text="Configuration",
            padding=10,
            style="Section.TLabelframe"
        )
        panel.pack(fill=tk.X, pady=(12, 8))

        self.firebase_url_var = tk.StringVar(value=FIREBASE_POST_URL)
        self.stream_code_var = tk.StringVar(value="wifi_probe_stream")

        self.sending_interface_var = tk.StringVar()
        self.probing_interface_var = tk.StringVar()

        self.target_ip_var = tk.StringVar(value=DEFAULT_TARGET_IP)
        self.target_port_var = tk.StringVar(value=str(DEFAULT_TARGET_PORT))
        self.duration_var = tk.StringVar(value=str(DEFAULT_DURATION_SECONDS))
        self.infinite_duration_var = tk.BooleanVar(value=False)
        self.interval_var = tk.StringVar(value=str(DEFAULT_INTERVAL_SECONDS))
        self.parallel_var = tk.StringVar(value=str(DEFAULT_PARALLEL_STREAMS))
        self.protocol_var = tk.StringVar(value=DEFAULT_PROTOCOL)
        self.udp_bandwidth_var = tk.StringVar(value=DEFAULT_UDP_BANDWIDTH)

        row = 0

        ttk.Label(panel, text="Firebase POST URL").grid(row=row, column=0, sticky=tk.W, padx=4, pady=5)
        ttk.Entry(panel, textvariable=self.firebase_url_var, width=82).grid(
            row=row,
            column=1,
            columnspan=7,
            sticky=tk.EW,
            padx=4,
            pady=5
        )

        row += 1

        ttk.Label(panel, text="Stream Code").grid(row=row, column=0, sticky=tk.W, padx=4, pady=5)
        ttk.Entry(panel, textvariable=self.stream_code_var, width=28).grid(
            row=row,
            column=1,
            sticky=tk.W,
            padx=4,
            pady=5
        )

        ttk.Label(panel, text="Protocol").grid(row=row, column=2, sticky=tk.W, padx=4, pady=5)
        self.protocol_combo = ttk.Combobox(
            panel,
            textvariable=self.protocol_var,
            values=["TCP", "UDP"],
            width=10,
            state="readonly"
        )
        self.protocol_combo.grid(row=row, column=3, sticky=tk.W, padx=4, pady=5)

        ttk.Label(panel, text="UDP Bandwidth").grid(row=row, column=4, sticky=tk.W, padx=4, pady=5)
        ttk.Entry(panel, textvariable=self.udp_bandwidth_var, width=12).grid(
            row=row,
            column=5,
            sticky=tk.W,
            padx=4,
            pady=5
        )

        row += 1

        ttk.Label(panel, text="Sending Interface for POST").grid(row=row, column=0, sticky=tk.W, padx=4, pady=5)
        self.sending_combo = ttk.Combobox(
            panel,
            textvariable=self.sending_interface_var,
            width=45,
            state="readonly"
        )
        self.sending_combo.grid(row=row, column=1, columnspan=3, sticky=tk.EW, padx=4, pady=5)

        ttk.Label(panel, text="Must reach Firebase / internet").grid(row=row, column=4, columnspan=4, sticky=tk.W, padx=4, pady=5)

        row += 1

        ttk.Label(panel, text="Probing Interface for iperf3").grid(row=row, column=0, sticky=tk.W, padx=4, pady=5)
        self.probing_combo = ttk.Combobox(
            panel,
            textvariable=self.probing_interface_var,
            width=45,
            state="readonly"
        )
        self.probing_combo.grid(row=row, column=1, columnspan=3, sticky=tk.EW, padx=4, pady=5)

        ttk.Label(panel, text="Bound into iperf3 using -B").grid(row=row, column=4, columnspan=4, sticky=tk.W, padx=4, pady=5)

        row += 1

        ttk.Label(panel, text="Target IP").grid(row=row, column=0, sticky=tk.W, padx=4, pady=5)
        ttk.Entry(panel, textvariable=self.target_ip_var, width=18).grid(row=row, column=1, sticky=tk.W, padx=4, pady=5)

        ttk.Label(panel, text="Target Port").grid(row=row, column=2, sticky=tk.W, padx=4, pady=5)
        ttk.Entry(panel, textvariable=self.target_port_var, width=10).grid(row=row, column=3, sticky=tk.W, padx=4, pady=5)

        ttk.Label(panel, text="Interval s").grid(row=row, column=4, sticky=tk.W, padx=4, pady=5)
        ttk.Entry(panel, textvariable=self.interval_var, width=10).grid(row=row, column=5, sticky=tk.W, padx=4, pady=5)

        ttk.Label(panel, text="Streams").grid(row=row, column=6, sticky=tk.W, padx=4, pady=5)
        ttk.Entry(panel, textvariable=self.parallel_var, width=10).grid(row=row, column=7, sticky=tk.W, padx=4, pady=5)

        row += 1

        ttk.Label(panel, text="Finite Duration s").grid(row=row, column=0, sticky=tk.W, padx=4, pady=5)
        self.duration_entry = ttk.Entry(panel, textvariable=self.duration_var, width=12)
        self.duration_entry.grid(row=row, column=1, sticky=tk.W, padx=4, pady=5)

        self.infinite_checkbox = ttk.Checkbutton(
            panel,
            text="Infinite duration: run until Stop is pressed",
            variable=self.infinite_duration_var,
            command=self._on_infinite_toggle
        )
        self.infinite_checkbox.grid(row=row, column=2, columnspan=4, sticky=tk.W, padx=4, pady=5)

        ttk.Label(
            panel,
            text="Infinite mode uses iperf3 -t 0 and keeps POSTing parsed samples.",
            style="Status.TLabel"
        ).grid(row=row, column=5, columnspan=3, sticky=tk.W, padx=4, pady=5)

        row += 1

        button_frame = ttk.Frame(panel)
        button_frame.grid(row=row, column=0, columnspan=8, sticky=tk.W, pady=(8, 0))

        self.refresh_button = ttk.Button(
            button_frame,
            text="Refresh Interfaces",
            command=self._refresh_interfaces
        )
        self.refresh_button.pack(side=tk.LEFT, padx=(0, 8))

        self.test_post_button = ttk.Button(
            button_frame,
            text="Test Firebase POST Through Sending Interface",
            command=self._test_post
        )
        self.test_post_button.pack(side=tk.LEFT, padx=(0, 8))

        self.start_button = ttk.Button(
            button_frame,
            text="Start Iperfer",
            command=self.start_iperfer
        )
        self.start_button.pack(side=tk.LEFT, padx=(0, 8))

        self.stop_button = ttk.Button(
            button_frame,
            text="Stop",
            command=self.stop_iperfer,
            state=tk.DISABLED
        )
        self.stop_button.pack(side=tk.LEFT, padx=(0, 8))

        for column in range(8):
            panel.columnconfigure(column, weight=1)

    def _build_metrics_panel(self, outer):
        panel = ttk.LabelFrame(
            outer,
            text="Live Agent Metrics",
            padding=10,
            style="Section.TLabelframe"
        )
        panel.pack(fill=tk.X, pady=8)

        self.latest_throughput_var = tk.StringVar(value="Latest: 0.000 Mbps")
        self.avg_throughput_var = tk.StringVar(value="Avg: 0.000 Mbps")
        self.minmax_throughput_var = tk.StringVar(value="Min/Max: 0.000 / 0.000 Mbps")
        self.std_throughput_var = tk.StringVar(value="Std: 0.000 Mbps")
        self.parsed_count_var = tk.StringVar(value="Parsed: 0")
        self.posted_count_var = tk.StringVar(value="POST success: 0")
        self.failed_count_var = tk.StringVar(value="POST failed: 0")
        self.duration_mode_var = tk.StringVar(value="Duration mode: finite")

        self.post_route_var = tk.StringVar(value="POST route: not started")
        self.probe_route_var = tk.StringVar(value="Probe route: not started")

        metric_vars = [
            self.latest_throughput_var,
            self.avg_throughput_var,
            self.minmax_throughput_var,
            self.std_throughput_var,
            self.parsed_count_var,
            self.posted_count_var,
            self.failed_count_var,
            self.duration_mode_var,
        ]

        for i, var in enumerate(metric_vars):
            ttk.Label(panel, textvariable=var, style="Metric.TLabel").grid(
                row=i // 4,
                column=i % 4,
                sticky=tk.W,
                padx=8,
                pady=5
            )

        ttk.Label(panel, textvariable=self.post_route_var, style="Status.TLabel").grid(
            row=2,
            column=0,
            columnspan=4,
            sticky=tk.W,
            padx=8,
            pady=3
        )
        ttk.Label(panel, textvariable=self.probe_route_var, style="Status.TLabel").grid(
            row=3,
            column=0,
            columnspan=4,
            sticky=tk.W,
            padx=8,
            pady=3
        )

        for column in range(4):
            panel.columnconfigure(column, weight=1)

    def _build_command_panel(self, outer):
        panel = ttk.LabelFrame(
            outer,
            text="Generated iperf3 Command",
            padding=10,
            style="Section.TLabelframe"
        )
        panel.pack(fill=tk.X, pady=8)

        self.command_var = tk.StringVar(value="Not started")
        command_label = ttk.Label(
            panel,
            textvariable=self.command_var,
            style="Status.TLabel",
            wraplength=1200
        )
        command_label.pack(anchor=tk.W)

    def _build_output_panel(self, outer):
        panel = ttk.LabelFrame(
            outer,
            text="Raw iperf3 / POST Log",
            padding=10,
            style="Section.TLabelframe"
        )
        panel.pack(fill=tk.BOTH, expand=True, pady=(8, 0))

        self.output_box = scrolledtext.ScrolledText(panel, height=18, wrap=tk.WORD)
        self.output_box.pack(fill=tk.BOTH, expand=True)

    def _on_infinite_toggle(self):
        if self.infinite_duration_var.get():
            self.duration_entry.configure(state=tk.DISABLED)
            self.duration_mode_var.set("Duration mode: infinite")
        else:
            self.duration_entry.configure(state=tk.NORMAL)
            self.duration_mode_var.set("Duration mode: finite")

    # =========================
    # Interface selection
    # =========================

    def _refresh_interfaces(self):
        old_sending = self.sending_interface_var.get()
        old_probing = self.probing_interface_var.get()

        self.interfaces = discover_ipv4_interfaces(include_loopback=False)
        labels = [interface.label for interface in self.interfaces]

        self.sending_combo["values"] = labels
        self.probing_combo["values"] = labels

        if old_sending in labels:
            self.sending_interface_var.set(old_sending)
        elif labels:
            self.sending_interface_var.set(labels[0])
        else:
            self.sending_interface_var.set("")

        if old_probing in labels:
            self.probing_interface_var.set(old_probing)
        elif labels:
            self.probing_interface_var.set(labels[0])
        else:
            self.probing_interface_var.set("")

        self._append_log(f"Detected {len(labels)} active non-loopback IPv4 interface(s).\n")

    def _get_interface_by_label(self, label: str) -> InterfaceInfo:
        for interface in self.interfaces:
            if interface.label == label:
                return interface

        raise ValueError(f"Interface selection is invalid or stale: {label}")

    # =========================
    # Validation / config
    # =========================

    def _build_runtime_config(self) -> RuntimeConfig:
        firebase_url = self.firebase_url_var.get().strip()
        stream_code = self.stream_code_var.get().strip()

        if not firebase_url.startswith("https://") and not firebase_url.startswith("http://"):
            raise ValueError("Firebase POST URL must start with http:// or https://")

        if not stream_code:
            raise ValueError("Stream Code cannot be empty.")

        sending_interface = self._get_interface_by_label(self.sending_interface_var.get())
        probing_interface = self._get_interface_by_label(self.probing_interface_var.get())

        target_ip = self.target_ip_var.get().strip()
        validate_ipv4(target_ip, "Target IP")

        try:
            target_port = int(self.target_port_var.get().strip())
            if not 1 <= target_port <= 65535:
                raise ValueError
        except ValueError:
            raise ValueError("Target Port must be an integer from 1 to 65535.")

        infinite_duration = bool(self.infinite_duration_var.get())

        if infinite_duration:
            duration_sec = None
        else:
            try:
                duration_sec = int(self.duration_var.get().strip())
                if duration_sec <= 0:
                    raise ValueError
            except ValueError:
                raise ValueError("Duration must be a positive integer, or enable Infinite duration.")

        try:
            interval_sec = float(self.interval_var.get().strip())
            if interval_sec <= 0:
                raise ValueError
        except ValueError:
            raise ValueError("Interval must be a positive number.")

        try:
            parallel_streams = int(self.parallel_var.get().strip())
            if parallel_streams <= 0:
                raise ValueError
        except ValueError:
            raise ValueError("Streams must be a positive integer.")

        protocol = self.protocol_var.get().strip().upper()

        if protocol not in {"TCP", "UDP"}:
            raise ValueError("Protocol must be TCP or UDP.")

        udp_bandwidth = self.udp_bandwidth_var.get().strip() or DEFAULT_UDP_BANDWIDTH

        return RuntimeConfig(
            firebase_url=firebase_url,
            stream_code=stream_code,

            sending_interface_name=sending_interface.name,
            sending_interface_ip=sending_interface.ip,

            probing_interface_name=probing_interface.name,
            probing_interface_ip=probing_interface.ip,

            target_ip=target_ip,
            target_port=target_port,
            infinite_duration=infinite_duration,
            duration_sec=duration_sec,
            interval_sec=interval_sec,
            parallel_streams=parallel_streams,
            protocol=protocol,
            udp_bandwidth=udp_bandwidth
        )

    def _build_iperf_command(self, config: RuntimeConfig):
        command = [
            "iperf3",
            "-c", config.target_ip,
            "-p", str(config.target_port),
            "-i", str(config.interval_sec),
            "-f", "m",
            "-B", config.probing_interface_ip,
            "-P", str(config.parallel_streams)
        ]

        if config.infinite_duration:
            command.extend(["-t", "0"])
        else:
            command.extend(["-t", str(config.duration_sec)])

        if self.forceflush_supported:
            command.append("--forceflush")

        if config.protocol == "UDP":
            command.extend(["-u", "-b", config.udp_bandwidth])

        return command

    # =========================
    # Firebase POST
    # =========================

    def _build_post_headers(self, sample: IperfSample, config: RuntimeConfig):
        duration_mode = "infinite" if config.infinite_duration else "finite"

        return {
            "Content-Type": "application/json",

            # Requested interface header:
            "X-Interface-Name": config.probing_interface_name,

            # Extra trace headers:
            "X-Iperfer-Stream-Code": config.stream_code,
            "X-Iperfer-Probing-Interface": config.probing_interface_name,
            "X-Iperfer-Probing-IP": config.probing_interface_ip,
            "X-Iperfer-Sending-Interface": config.sending_interface_name,
            "X-Iperfer-Sending-IP": config.sending_interface_ip,
            "X-Iperfer-Target-IP": config.target_ip,
            "X-Iperfer-Target-Port": str(config.target_port),
            "X-Iperfer-Duration-Mode": duration_mode,
            "X-Iperfer-Throughput-Mbps": f"{sample.throughput_mbps:.6f}",
        }

    def _build_post_payload(self, sample: IperfSample, config: RuntimeConfig):
        now = datetime.now()
        duration_mode = "infinite" if config.infinite_duration else "finite"

        return {
            # Fields the website already expects:
            "streamCode": config.stream_code,
            "interfaceName": config.probing_interface_name,
            "throughputMbps": sample.throughput_mbps,
            "sentAt": {
                ".sv": "timestamp"
            },

            # Interface model:
            "probingInterfaceName": config.probing_interface_name,
            "probingInterfaceIp": config.probing_interface_ip,
            "sendingInterfaceName": config.sending_interface_name,
            "sendingInterfaceIp": config.sending_interface_ip,

            # Test target:
            "targetIp": config.target_ip,
            "targetPort": config.target_port,
            "protocol": config.protocol,

            # Duration:
            "durationMode": duration_mode,
            "infiniteDuration": config.infinite_duration,
            "durationSec": config.duration_sec if config.duration_sec is not None else "infinite",

            # iperf interval:
            "elapsedStartSec": sample.elapsed_start_sec,
            "elapsedEndSec": sample.elapsed_end_sec,
            "intervalDurationSec": sample.elapsed_end_sec - sample.elapsed_start_sec,
            "transferMBytes": sample.transfer_mbytes,

            # Running stats for optional web-side display:
            "runningCurrentMbps": self.stats.current,
            "runningAverageMbps": self.stats.average,
            "runningMinMbps": self.stats.minimum,
            "runningMaxMbps": self.stats.maximum,
            "runningStdMbps": self.stats.std,
            "runningSampleCount": self.stats.count,

            # Client metadata:
            "clientHost": platform.node(),
            "clientSystem": platform.system(),
            "clientRelease": platform.release(),
            "localCreatedAtIso": now.isoformat(timespec="milliseconds"),

            # Raw line for traceability:
            "rawIperfLine": sample.raw_line,

            # Compatibility aliases:
            "mbps": sample.throughput_mbps,
            "iface": config.probing_interface_name,
            "interfaceCode": config.stream_code,
        }

    def _post_worker_thread(self):
        config = self.runtime_config

        if config is None:
            return

        while self.running or not self.post_queue.empty():
            try:
                sample = self.post_queue.get(timeout=0.25)
            except queue.Empty:
                continue

            try:
                payload = self._build_post_payload(sample, config)
                headers = self._build_post_headers(sample, config)

                response = self.post_session.post(
                    config.firebase_url,
                    json=payload,
                    headers=headers,
                    timeout=4.0
                )

                if 200 <= response.status_code < 300:
                    self.gui_queue.put((
                        "post_success",
                        {
                            "status_code": response.status_code,
                            "response_text": response.text,
                            "sample": sample,
                        }
                    ))
                else:
                    self.gui_queue.put((
                        "post_failure",
                        {
                            "status_code": response.status_code,
                            "response_text": response.text,
                            "sample": sample,
                        }
                    ))

            except Exception as error:
                self.gui_queue.put((
                    "post_exception",
                    {
                        "error": str(error),
                        "sample": sample,
                    }
                ))

    def _test_post(self):
        if self.running:
            messagebox.showinfo("Running", "Stop the current run before testing POST.")
            return

        try:
            config = self._build_runtime_config()
            session = make_bound_session(config.sending_interface_ip)

            sample = IperfSample(
                elapsed_start_sec=0.0,
                elapsed_end_sec=0.0,
                throughput_mbps=0.0,
                transfer_mbytes=0.0,
                raw_line="manual Firebase POST test"
            )

            # Do not pollute the main stream name unless you want to test the plot.
            payload = self._build_post_payload(sample, config)
            payload["streamCode"] = f"{config.stream_code}_post_test"
            payload["interfaceCode"] = payload["streamCode"]
            payload["rawIperfLine"] = "manual Firebase POST test from GUI"

            headers = self._build_post_headers(sample, config)
            headers["X-Iperfer-Stream-Code"] = payload["streamCode"]

            response = session.post(
                config.firebase_url,
                json=payload,
                headers=headers,
                timeout=6.0
            )

            if 200 <= response.status_code < 300:
                self._append_log(
                    f"POST test success through {config.sending_interface_name} "
                    f"({config.sending_interface_ip}) | HTTP {response.status_code} | {response.text}\n"
                )
                messagebox.showinfo("POST Test Success", "Firebase POST test succeeded.")
            else:
                self._append_log(
                    f"POST test failed | HTTP {response.status_code} | {response.text}\n"
                )
                messagebox.showerror("POST Test Failed", response.text)

        except Exception as error:
            self._append_log(f"POST test exception: {error}\n")
            messagebox.showerror("POST Test Exception", str(error))

    # =========================
    # Run / stop
    # =========================

    def start_iperfer(self):
        if self.running:
            return

        if shutil.which("iperf3") is None:
            messagebox.showerror("iperf3 Not Found", "iperf3 is not installed or not available in PATH.")
            return

        try:
            config = self._build_runtime_config()
            command = self._build_iperf_command(config)
        except Exception as error:
            messagebox.showerror("Configuration Error", str(error))
            return

        self.runtime_config = config
        self.post_session = make_bound_session(config.sending_interface_ip)

        self._reset_runtime_state()
        self._open_csv_log(config)

        self.running = True
        self.stop_event.clear()
        self.start_time = time.time()

        self.status_var.set("Running")
        self.status_label.configure(style="Good.TLabel")

        self.start_button.configure(state=tk.DISABLED)
        self.stop_button.configure(state=tk.NORMAL)
        self.refresh_button.configure(state=tk.DISABLED)
        self.test_post_button.configure(state=tk.DISABLED)

        self.command_var.set(" ".join(command))

        duration_text = "infinite / until Stop" if config.infinite_duration else f"{config.duration_sec} s"
        self.duration_mode_var.set(f"Duration mode: {duration_text}")

        self.post_route_var.set(
            f"POST sending interface: {config.sending_interface_name} | {config.sending_interface_ip} → Firebase"
        )

        self.probe_route_var.set(
            f"Probing interface: {config.probing_interface_name} | {config.probing_interface_ip} → {config.target_ip}:{config.target_port}"
        )

        self._append_log("Starting iperfer client agent...\n")
        self._append_log(f"Firebase POST URL: {config.firebase_url}\n")
        self._append_log(f"Stream code: {config.stream_code}\n")
        self._append_log(f"Duration mode: {duration_text}\n")
        self._append_log(f"POST interface: {config.sending_interface_name} | {config.sending_interface_ip}\n")
        self._append_log(f"Probe interface: {config.probing_interface_name} | {config.probing_interface_ip}\n")
        self._append_log(f"iperf3 command: {' '.join(command)}\n\n")

        try:
            self.iperf_process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
        except Exception as error:
            self.running = False
            self._close_csv_log()
            self._set_stopped_state()
            messagebox.showerror("iperf3 Start Failed", str(error))
            return

        threading.Thread(target=self._iperf_reader_thread, daemon=True).start()
        threading.Thread(target=self._post_worker_thread, daemon=True).start()

    def stop_iperfer(self):
        if not self.running and self.iperf_process is None:
            return

        self.running = False
        self.stop_event.set()

        if self.iperf_process and self.iperf_process.poll() is None:
            try:
                if os.name == "nt":
                    self.iperf_process.terminate()
                else:
                    self.iperf_process.send_signal(signal.SIGTERM)

                try:
                    self.iperf_process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    self.iperf_process.kill()

            except Exception:
                pass

        self.iperf_process = None
        self._close_csv_log()
        self._set_stopped_state()
        self.gui_queue.put(("log", "\nStopped by user.\n"))

    def _set_stopped_state(self):
        self.status_var.set("Stopped")
        self.status_label.configure(style="Bad.TLabel")

        self.start_button.configure(state=tk.NORMAL)
        self.stop_button.configure(state=tk.DISABLED)
        self.refresh_button.configure(state=tk.NORMAL)
        self.test_post_button.configure(state=tk.NORMAL)

    def _reset_runtime_state(self):
        self.posted_count = 0
        self.failed_post_count = 0
        self.parsed_sample_count = 0
        self.stats.clear()

        self.latest_throughput_var.set("Latest: 0.000 Mbps")
        self.avg_throughput_var.set("Avg: 0.000 Mbps")
        self.minmax_throughput_var.set("Min/Max: 0.000 / 0.000 Mbps")
        self.std_throughput_var.set("Std: 0.000 Mbps")
        self.parsed_count_var.set("Parsed: 0")
        self.posted_count_var.set("POST success: 0")
        self.failed_count_var.set("POST failed: 0")

        self.output_box.delete("1.0", tk.END)

        while not self.post_queue.empty():
            try:
                self.post_queue.get_nowait()
            except queue.Empty:
                break

    # =========================
    # iperf parser thread
    # =========================

    def _iperf_reader_thread(self):
        config = self.runtime_config

        if config is None:
            return

        try:
            for raw_line in self.iperf_process.stdout:
                line = raw_line.rstrip("\n")
                self.gui_queue.put(("log", line + "\n"))

                sample = parse_iperf_interval_line(
                    line=line,
                    parallel_streams=config.parallel_streams
                )

                if sample is None:
                    continue

                self.gui_queue.put(("sample_parsed", sample))
                self.post_queue.put(sample)

            return_code = self.iperf_process.wait()
            self.gui_queue.put(("log", f"\niperf3 exited with return code: {return_code}\n"))

        except Exception as error:
            self.gui_queue.put(("log", f"\niperf reader error: {error}\n"))

        finally:
            self.running = False
            self.gui_queue.put(("finished", None))

    # =========================
    # CSV local trace
    # =========================

    def _open_csv_log(self, config: RuntimeConfig):
        os.makedirs(LOG_DIR, exist_ok=True)

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        safe_stream = re.sub(r"[^a-zA-Z0-9_-]+", "_", config.stream_code)

        self.csv_path = os.path.join(
            LOG_DIR,
            f"iperfer_client_{safe_stream}_{timestamp}.csv"
        )

        self.csv_file = open(self.csv_path, "w", newline="", encoding="utf-8")
        self.csv_writer = csv.writer(self.csv_file)

        self.csv_writer.writerow([
            "local_timestamp",
            "event_type",
            "stream_code",
            "throughput_mbps",
            "transfer_mbytes",
            "elapsed_start_sec",
            "elapsed_end_sec",
            "running_avg_mbps",
            "running_min_mbps",
            "running_max_mbps",
            "running_std_mbps",
            "running_sample_count",
            "duration_mode",
            "duration_sec",
            "sending_interface_name",
            "sending_interface_ip",
            "probing_interface_name",
            "probing_interface_ip",
            "target_ip",
            "target_port",
            "protocol",
            "firebase_url",
            "post_status",
            "post_response",
            "raw_iperf_line"
        ])

        self._append_log(f"Local CSV trace: {self.csv_path}\n")

    def _write_csv_row(
        self,
        event_type: str,
        sample: IperfSample,
        post_status: str = "",
        post_response: str = ""
    ):
        if self.csv_writer is None or self.runtime_config is None:
            return

        config = self.runtime_config
        duration_mode = "infinite" if config.infinite_duration else "finite"

        self.csv_writer.writerow([
            datetime.now().isoformat(timespec="milliseconds"),
            event_type,
            config.stream_code,
            f"{sample.throughput_mbps:.6f}",
            f"{sample.transfer_mbytes:.6f}",
            f"{sample.elapsed_start_sec:.3f}",
            f"{sample.elapsed_end_sec:.3f}",
            f"{self.stats.average:.6f}",
            f"{self.stats.minimum:.6f}",
            f"{self.stats.maximum:.6f}",
            f"{self.stats.std:.6f}",
            self.stats.count,
            duration_mode,
            config.duration_sec if config.duration_sec is not None else "infinite",
            config.sending_interface_name,
            config.sending_interface_ip,
            config.probing_interface_name,
            config.probing_interface_ip,
            config.target_ip,
            config.target_port,
            config.protocol,
            config.firebase_url,
            post_status,
            post_response,
            sample.raw_line
        ])

        self.csv_file.flush()

    def _close_csv_log(self):
        if self.csv_file:
            self.csv_file.flush()
            self.csv_file.close()

        self.csv_file = None
        self.csv_writer = None

    # =========================
    # GUI queue handling
    # =========================

    def _schedule_gui_queue(self):
        try:
            while True:
                event_type, payload = self.gui_queue.get_nowait()

                if event_type == "log":
                    self._append_log(payload)

                elif event_type == "sample_parsed":
                    self._handle_sample_parsed(payload)

                elif event_type == "post_success":
                    self._handle_post_success(payload)

                elif event_type == "post_failure":
                    self._handle_post_failure(payload)

                elif event_type == "post_exception":
                    self._handle_post_exception(payload)

                elif event_type == "finished":
                    self._close_csv_log()
                    self._set_stopped_state()

        except queue.Empty:
            pass

        self.root.after(150, self._schedule_gui_queue)

    def _handle_sample_parsed(self, sample: IperfSample):
        self.parsed_sample_count += 1
        self.stats.add(sample.throughput_mbps)

        self.latest_throughput_var.set(
            f"Latest: {sample.throughput_mbps:.3f} Mbps"
        )
        self.avg_throughput_var.set(
            f"Avg: {self.stats.average:.3f} Mbps"
        )
        self.minmax_throughput_var.set(
            f"Min/Max: {self.stats.minimum:.3f} / {self.stats.maximum:.3f} Mbps"
        )
        self.std_throughput_var.set(
            f"Std: {self.stats.std:.3f} Mbps"
        )
        self.parsed_count_var.set(
            f"Parsed: {self.parsed_sample_count}"
        )

        self._write_csv_row(
            event_type="parsed",
            sample=sample,
            post_status="queued",
            post_response=""
        )

    def _handle_post_success(self, payload):
        self.posted_count += 1

        sample = payload["sample"]
        status_code = payload["status_code"]
        response_text = payload["response_text"]

        self.posted_count_var.set(f"POST success: {self.posted_count}")

        self._append_log(
            f"POST success | HTTP {status_code} | "
            f"{sample.throughput_mbps:.3f} Mbps | {response_text}\n"
        )

        self._write_csv_row(
            event_type="post_success",
            sample=sample,
            post_status=str(status_code),
            post_response=response_text
        )

    def _handle_post_failure(self, payload):
        self.failed_post_count += 1

        sample = payload["sample"]
        status_code = payload["status_code"]
        response_text = payload["response_text"]

        self.failed_count_var.set(f"POST failed: {self.failed_post_count}")

        self._append_log(
            f"POST failed | HTTP {status_code} | "
            f"{sample.throughput_mbps:.3f} Mbps | {response_text}\n"
        )

        self._write_csv_row(
            event_type="post_failure",
            sample=sample,
            post_status=str(status_code),
            post_response=response_text
        )

    def _handle_post_exception(self, payload):
        self.failed_post_count += 1

        sample = payload["sample"]
        error = payload["error"]

        self.failed_count_var.set(f"POST failed: {self.failed_post_count}")

        self._append_log(
            f"POST exception | {sample.throughput_mbps:.3f} Mbps | {error}\n"
        )

        self._write_csv_row(
            event_type="post_exception",
            sample=sample,
            post_status="exception",
            post_response=error
        )

    def _append_log(self, text: str):
        self.output_box.insert(tk.END, text)
        self.output_box.see(tk.END)

    def on_close(self):
        if self.running:
            self.stop_iperfer()

        self.root.destroy()


def main():
    root = tk.Tk()
    app = IperferClientAgentApp(root)
    root.protocol("WM_DELETE_WINDOW", app.on_close)
    root.mainloop()


if __name__ == "__main__":
    main()
