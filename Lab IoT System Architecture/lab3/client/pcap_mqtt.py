"""Мини-анализатор pcap для MQTT: Ethernet/IPv4/TCP -> сборка TCP-потока -> пакеты MQTT 3.1.1.

Без внешних зависимостей. Для каждого кадра — размер «на проводе» (Ethernet-кадр без FCS,
то же, что Wireshark показывает как Frame Length) и какие MQTT-пакеты в нём закончились.
"""
import struct

TYPES = {1: "CONNECT", 2: "CONNACK", 3: "PUBLISH", 4: "PUBACK", 5: "PUBREC", 6: "PUBREL", 7: "PUBCOMP",
         8: "SUBSCRIBE", 9: "SUBACK", 10: "UNSUBSCRIBE", 11: "UNSUBACK", 12: "PINGREQ", 13: "PINGRESP",
         14: "DISCONNECT"}


def read_pcap(path):
    with open(path, "rb") as f:
        gh = f.read(24)
        magic = gh[:4]
        if magic in (b"\xd4\xc3\xb2\xa1", b"\x4d\x3c\xb2\xa1"):
            end = "<"
        elif magic in (b"\xa1\xb2\xc3\xd4", b"\xa1\xb2\x3c\x4d"):
            end = ">"
        else:
            raise ValueError("не pcap (pcapng не поддерживается — сохраните как pcap)")
        nano = magic in (b"\x4d\x3c\xb2\xa1", b"\xa1\xb2\x3c\x4d")
        linktype = struct.unpack(end + "I", gh[20:24])[0]
        while True:
            h = f.read(16)
            if len(h) < 16:
                break
            ts, tfrac, incl, orig = struct.unpack(end + "IIII", h)
            data = f.read(incl)
            yield ts + tfrac / (1e9 if nano else 1e6), orig, linktype, data


def parse_frame(linktype, d):
    if linktype == 1:                         # Ethernet
        off, eth = 14, struct.unpack("!H", d[12:14])[0]
        if eth == 0x8100:
            off, eth = 18, struct.unpack("!H", d[16:18])[0]
        l2 = off
    elif linktype == 113:                     # Linux cooked v1
        off, eth, l2 = 16, struct.unpack("!H", d[14:16])[0], 16
    elif linktype == 276:                     # Linux cooked v2
        off, eth, l2 = 20, struct.unpack("!H", d[0:2])[0], 20
    else:
        return None
    if eth != 0x0800:
        return None
    ihl = (d[off] & 0x0F) * 4
    if d[off + 9] != 6:
        return None
    tot = struct.unpack("!H", d[off + 2:off + 4])[0]
    src, dst = d[off + 12:off + 16], d[off + 16:off + 20]
    t = off + ihl
    sport, dport, seq = struct.unpack("!HHI", d[t:t + 8])
    thl = (d[t + 12] >> 4) * 4
    flags = d[t + 13]
    payload = d[t + thl:off + tot]
    return {"src": ".".join(map(str, src)), "dst": ".".join(map(str, dst)), "sport": sport, "dport": dport,
            "seq": seq, "flags": flags, "l2": l2, "ip": ihl, "tcp": thl, "payload": payload}


def decode_mqtt(buf):
    """Возвращает (список пакетов, остаток)."""
    out, i = [], 0
    while i + 2 <= len(buf):
        b0 = buf[i]
        mult, rl, j = 1, 0, i + 1
        while j < len(buf):
            rl += (buf[j] & 0x7F) * mult
            mult *= 128
            j += 1
            if buf[j - 1] & 0x80 == 0:
                break
        else:
            break
        if j + rl > len(buf):
            break
        body = buf[j:j + rl]
        typ = b0 >> 4
        p = {"type": TYPES.get(typ, str(typ)), "fixed_header": j - i, "remaining": rl, "size": j - i + rl,
             "raw": buf[i:j + rl]}
        if typ == 3:
            qos = (b0 >> 1) & 3
            tl = struct.unpack("!H", body[:2])[0]
            p.update(qos=qos, dup=bool(b0 & 8), retain=bool(b0 & 1), topic=body[2:2 + tl].decode(errors="replace"),
                     topic_len=tl, packet_id=struct.unpack("!H", body[2 + tl:4 + tl])[0] if qos else None)
            p["payload_len"] = rl - 2 - tl - (2 if qos else 0)
            p["payload"] = body[rl - p["payload_len"]:].decode(errors="replace")
        elif typ in (4, 5, 6, 7, 11) and rl >= 2:
            p["packet_id"] = struct.unpack("!H", body[:2])[0]
        elif typ == 1:
            pl = struct.unpack("!H", body[:2])[0]
            proto = body[2:2 + pl].decode()
            lvl, cf = body[2 + pl], body[3 + pl]
            ka = struct.unpack("!H", body[4 + pl:6 + pl])[0]
            p.update(protocol=proto, level=lvl, clean_session=bool(cf & 2), will=bool(cf & 4), keepalive=ka)
        out.append(p)
        i = j + rl
    return out, buf[i:]


def analyze(path, broker_port=1883):
    flows, frames = {}, []
    for ts, orig, lt, data in read_pcap(path):
        f = parse_frame(lt, data)
        if not f or broker_port not in (f["sport"], f["dport"]):
            continue
        key = (f["src"], f["sport"], f["dst"], f["dport"])
        st = flows.setdefault(key, {"next": None, "buf": b""})
        pl = f["payload"]
        if f["flags"] & 0x02:                          # SYN
            st["next"] = (f["seq"] + 1) & 0xFFFFFFFF
        elif pl and st["next"] is not None:
            delta = (f["seq"] - st["next"]) & 0xFFFFFFFF
            if delta > 0x7FFFFFFF:                     # ретрансмиссия уже полученных байт
                skip = (st["next"] - f["seq"]) & 0xFFFFFFFF
                pl = pl[skip:] if skip < len(pl) else b""
            if pl:
                st["next"] = (st["next"] + len(pl)) & 0xFFFFFFFF
        elif pl and st["next"] is None:
            st["next"] = (f["seq"] + len(pl)) & 0xFFFFFFFF
        pkts = []
        if pl:
            st["buf"] += pl
            pkts, st["buf"] = decode_mqtt(st["buf"])
        client_port = f["sport"] if f["dport"] == broker_port else f["dport"]
        frames.append({"t": ts, "frame_len": orig, "dir": "->broker" if f["dport"] == broker_port else "<-broker",
                       "client_port": client_port, "l2": f["l2"], "ip": f["ip"], "tcp": f["tcp"],
                       "tcp_payload": len(f["payload"]), "flags": f["flags"], "mqtt": pkts})
    return frames
