"""Управление эмуляцией потерь.

Backend "tc" (Docker, по умолчанию):
    * сторона клиента: tc netem на eth0 контейнера lab3-client (исходящий трафик устройств);
    * сторона брокера: агент lab3-netem в сетевом пространстве брокера (tc netem на его eth0),
      команды получает по MQTT (топик lab3/ctl/netem), сам ходит к брокеру через localhost.
    Итого потери симметричны: теряются и PUBLISH от устройств, и PUBACK/доставка от брокера.
Backend "iptables" (проверка без Docker): случайный DROP пакетов TCP-порта брокера в обе стороны.
"""
import json
import os
import subprocess
import threading
import time

import paho.mqtt.client as mqtt

from common import BROKER, PORT, log, uid

BACKEND = os.getenv("NETEM_BACKEND", "tc")
IFACE = os.getenv("IFACE", "eth0")


def sh(*cmd, check=True):
    r = subprocess.run(list(cmd), capture_output=True, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)}: {r.stderr.strip()}")
    return r.stdout


class Agent:
    """RPC к агенту на стороне брокера через MQTT."""

    def __init__(self):
        self.c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=uid("ctl"))
        self.replies, self.ev = {}, threading.Event()
        self.c.on_message = self._on_msg
        self.c.connect(BROKER, PORT, 30)
        self.c.subscribe("lab3/ctl/ack/#", qos=1)
        self.c.loop_start()
        time.sleep(0.3)

    def _on_msg(self, c, u, m):
        self.replies[m.topic.rsplit("/", 1)[-1]] = json.loads(m.payload)
        self.ev.set()

    def call(self, cmd, timeout=30, **kw):
        rid = uid("r")
        self.c.publish(f"lab3/ctl/{cmd}", json.dumps({"id": rid, **kw}), qos=1)
        end = time.time() + timeout
        while rid not in self.replies and time.time() < end:
            self.ev.wait(0.2)
            self.ev.clear()
        if rid not in self.replies:
            raise TimeoutError(f"агент lab3-netem не ответил на {cmd} (контейнер запущен?)")
        r = self.replies.pop(rid)
        if not r.get("ok"):
            raise RuntimeError(f"агент: {r.get('error')}")
        return r

    def close(self):
        self.c.loop_stop()
        self.c.disconnect()


class Net:
    def __init__(self):
        self.agent = Agent() if BACKEND == "tc" else None
        self._ipt = []

    # ---------------------------------------------------------------- случайные потери
    def set_loss(self, pct):
        self.clear()
        if pct <= 0:
            return
        if BACKEND == "tc":
            self.agent.call("netem", args=f"loss {pct}%")                    # брокер -> клиенты
            sh("tc", "qdisc", "replace", "dev", IFACE, "root", "netem", "loss", f"{pct}%")   # клиенты -> брокер
        else:
            p = f"{pct / 100:.4f}"
            for d in ("--dport", "--sport"):
                self._ipt_add("-p", "tcp", d, str(PORT), "-m", "statistic", "--mode", "random",
                              "--probability", p, "-j", "DROP")
        log(f"  netem: loss {pct}% в обе стороны ({BACKEND})")

    # ---------------------------------------------------------------- полный обрыв связи на время
    def outage(self, seconds, restore_pct, direction="down"):
        """Обрыв на seconds, затем снова restore_pct % потерь. Не блокирует.
        direction="down": теряется только трафик брокер -> клиенты (PUBACK, PINGRESP, доставка),
                          PUBLISH от устройств доходят — классический источник дублей QoS 1;
        direction="both": полный обрыв в обе стороны."""
        def local():
            time.sleep(0.5)
            if BACKEND == "tc":
                if direction == "both":
                    sh("tc", "qdisc", "replace", "dev", IFACE, "root", "netem", "loss", "100%")
                time.sleep(seconds)
                if direction == "both":
                    if restore_pct > 0:
                        sh("tc", "qdisc", "replace", "dev", IFACE, "root", "netem", "loss", f"{restore_pct}%")
                    else:
                        sh("tc", "qdisc", "del", "dev", IFACE, "root", check=False)
            else:
                saved = list(self._ipt)
                for rule in saved:
                    sh("iptables", "-D", "INPUT", *rule, check=False)
                self._ipt = []
                for d in (("--sport",) if direction == "down" else ("--dport", "--sport")):
                    self._ipt_add("-p", "tcp", d, str(PORT), "-j", "DROP")
                time.sleep(seconds)
                for rule in self._ipt:
                    sh("iptables", "-D", "INPUT", *rule, check=False)
                self._ipt = []
                for rule in saved:
                    self._ipt_add(*rule[2:] if rule[:2] == ("-i", "lo") else rule)
            log(f"  обрыв закончился, потери снова {restore_pct}%")
        if BACKEND == "tc":
            self.agent.call("outage", seconds=seconds)     # агент режет исходящий трафик брокера
        what = "обратного канала (брокер -> клиенты)" if direction == "down" else "в обе стороны"
        log(f"  ОБРЫВ {what} на {seconds:g} с")
        t = threading.Thread(target=local, daemon=True)
        t.start()
        return t

    # ---------------------------------------------------------------- «потеря питания»: глухой обрыв одного клиента
    def block_port(self, local_port):
        """Пакеты клиента с исходящим портом local_port перестают доходить до брокера (FIN/RST тоже)."""
        if BACKEND == "tc":
            sh("tc", "qdisc", "del", "dev", IFACE, "root", check=False)
            sh("tc", "qdisc", "add", "dev", IFACE, "root", "handle", "1:", "prio")
            sh("tc", "qdisc", "add", "dev", IFACE, "parent", "1:3", "handle", "30:", "netem", "loss", "100%")
            sh("tc", "filter", "add", "dev", IFACE, "protocol", "ip", "parent", "1:0", "prio", "1",
               "u32", "match", "ip", "sport", str(local_port), "0xffff", "flowid", "1:3")
        else:
            self._ipt_add("-p", "tcp", "--sport", str(local_port), "-j", "DROP")
            self._ipt_add("-p", "tcp", "--dport", str(local_port), "-j", "DROP")

    def clear(self):
        if BACKEND == "tc":
            sh("tc", "qdisc", "del", "dev", IFACE, "root", check=False)
            self.agent.call("netem", args="")
        else:
            for rule in self._ipt:
                sh("iptables", "-D", "INPUT", *rule, check=False)
            self._ipt = []

    def _ipt_add(self, *rule):
        rule = ("-i", "lo", *rule) if BROKER in ("localhost", "127.0.0.1") else rule
        sh("iptables", "-A", "INPUT", *rule)
        self._ipt.append(rule)

    # ---------------------------------------------------------------- захват трафика на стороне брокера
    def capture_start(self, name):
        if BACKEND == "tc":
            return self.agent.call("capture", action="start", file=name)
        self._tcpdump = subprocess.Popen(["tcpdump", "-i", "lo", "-U", "-w", os.path.join(os.getenv("CAPTURES", "/captures"), name),
                                          "tcp", "port", str(PORT)], stderr=subprocess.DEVNULL)
        time.sleep(1)

    def capture_stop(self):
        if BACKEND == "tc":
            return self.agent.call("capture", action="stop")
        time.sleep(1)
        self._tcpdump.terminate()
        self._tcpdump.wait(5)

    def close(self):
        try:
            self.clear()
        finally:
            if self.agent:
                self.agent.close()
