"""Все эксперименты Lab 3 подряд (~15 мин): retained -> LWT -> захват Wireshark -> QoS × потери."""
import subprocess
import sys

for script in ("exp_retained.py", "exp_lwt.py", "exp_capture.py", "exp_qos.py"):
    print(f"\n######## {script} ########", flush=True)
    r = subprocess.run([sys.executable, script])
    if r.returncode:
        sys.exit(f"{script} завершился с ошибкой {r.returncode}")
print("\nГотово: результаты в /results, pcap в /captures")
