"""Агент lab3-netem: живёт в сетевом пространстве брокера (network_mode: service:mosquitto).

Команды по MQTT (через localhost, на который netem не влияет):
  lab3/ctl/netem    {"id", "args": "loss 20%"}         -> tc qdisc replace dev eth0 root netem <args> ("" = снять)
  lab3/ctl/capture  {"id", "action": "start", "file"}   -> tcpdump -i eth0 tcp port 1883 -w /captures/<file>
  lab3/ctl/capture  {"id", "action": "stop"}
  lab3/ctl/outage   {"id", "seconds": 10}              -> loss 100% на N секунд, затем прежние параметры
Ответ: lab3/ctl/ack/<id>  {"ok": true/false, ...}
"""
import json
import os
import signal
import subprocess
import threading
import time

import paho.mqtt.client as mqtt

IFACE = os.getenv("IFACE", "eth0")
tcpdump = None
current_args = ""


def sh(*cmd):
    r = subprocess.run(list(cmd), capture_output=True, text=True)
    return r.returncode, (r.stdout + r.stderr).strip()


def apply(args):
    sh("tc", "qdisc", "del", "dev", IFACE, "root")
    if args:
        rc, out = sh("tc", "qdisc", "add", "dev", IFACE, "root", "netem", *args.split())
        if rc:
            raise RuntimeError(out)


def outage(seconds, restore):
    time.sleep(0.5)                 # сначала уходит ответ, потом связь пропадает
    apply("loss 100%")
    time.sleep(seconds)
    apply(restore)
    print(time.strftime("%H:%M:%S"), f"outage {seconds}s done, restored '{restore}'", flush=True)


def handle(cmd, req):
    global tcpdump, current_args
    if cmd == "netem":
        apply(req.get("args", ""))
        current_args = req.get("args", "")
        return {"ok": True, "qdisc": sh("tc", "qdisc", "show", "dev", IFACE)[1]}
    if cmd == "outage":
        threading.Thread(target=outage, args=(float(req["seconds"]), current_args), daemon=True).start()
        return {"ok": True}
    if cmd == "capture":
        if req.get("action") == "start":
            if tcpdump:
                tcpdump.send_signal(signal.SIGINT)
                tcpdump.wait(5)
            path = os.path.join("/captures", os.path.basename(req.get("file", "mqtt.pcap")))
            tcpdump = subprocess.Popen(["tcpdump", "-i", IFACE, "-U", "-s", "0", "-w", path, "tcp", "port", "1883"],
                                       stderr=subprocess.DEVNULL)
            time.sleep(1.0)
            if tcpdump.poll() is not None:
                return {"ok": False, "error": "tcpdump не запустился"}
            return {"ok": True, "file": path}
        if tcpdump:
            time.sleep(1.0)
            tcpdump.send_signal(signal.SIGINT)
            tcpdump.wait(5)
            tcpdump = None
        return {"ok": True}
    return {"ok": False, "error": f"unknown command {cmd}"}


def on_message(c, u, m):
    cmd = m.topic.split("/")[2]
    try:
        req = json.loads(m.payload)
    except ValueError:
        req = {}
    try:
        resp = handle(cmd, req)
    except Exception as e:  # noqa: BLE001
        resp = {"ok": False, "error": str(e)}
    print(time.strftime("%H:%M:%S"), cmd, req, "->", resp, flush=True)
    c.publish(f"lab3/ctl/ack/{req.get('id', 'x')}", json.dumps(resp), qos=1)


def main():
    sh("tc", "qdisc", "del", "dev", IFACE, "root")
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="lab3-netem-agent")
    c.on_connect = lambda cl, u, f, rc, p: cl.subscribe([("lab3/ctl/netem", 1), ("lab3/ctl/capture", 1),
                                                         ("lab3/ctl/outage", 1)])
    c.on_message = on_message
    while True:
        try:
            c.connect("127.0.0.1", 1883, 30)
            break
        except OSError:
            time.sleep(1)
    print("agent ready", flush=True)
    c.loop_forever()


if __name__ == "__main__":
    main()
