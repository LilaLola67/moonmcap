"""Measure a streaming, CRC-checked scan using the official Python MCAP reader."""
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "_interop_deps"))
from mcap.records import DataEnd, Message
from mcap.stream_reader import StreamReader


def peak_rss_kib():
    if sys.platform != "win32":
        import resource

        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return peak // 1024 if sys.platform == "darwin" else peak

    import ctypes
    from ctypes import wintypes

    class Counters(ctypes.Structure):
        _fields_ = [("size", wintypes.DWORD), ("page_faults", wintypes.DWORD)] + [
            (name, ctypes.c_size_t)
            for name in (
                "peak_working_set", "working_set", "peak_paged_pool", "paged_pool",
                "peak_nonpaged_pool", "nonpaged_pool", "pagefile", "peak_pagefile",
            )
        ]

    get_process = ctypes.windll.kernel32.GetCurrentProcess
    get_process.restype = wintypes.HANDLE
    get_memory = ctypes.windll.psapi.GetProcessMemoryInfo
    get_memory.argtypes = [wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD]
    get_memory.restype = wintypes.BOOL
    counters = Counters()
    counters.size = ctypes.sizeof(counters)
    if not get_memory(get_process(), ctypes.byref(counters), counters.size):
        raise ctypes.WinError()
    return counters.peak_working_set // 1024


def main():
    source = Path(sys.argv[1])
    count = 0
    first = last = None
    data_crc = False
    with source.open("rb") as stream:
        for record in StreamReader(stream, validate_crcs=True).records:
            if isinstance(record, DataEnd):
                data_crc = record.data_section_crc != 0
            if isinstance(record, Message):
                count += 1
                first = record.log_time if first is None else min(first, record.log_time)
                last = record.log_time if last is None else max(last, record.log_time)
    print(json.dumps({"messages": count, "start": first, "end": last,
                      "data_crc_verified": data_crc, "max_rss_kib": peak_rss_kib()}))


if __name__ == "__main__":
    main()
