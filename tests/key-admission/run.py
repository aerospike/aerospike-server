import copy
import json
import os
import socket
import struct
import subprocess
import sys
import tempfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from pathlib import Path

ASD, FIXTURE, SCHEMA = (Path(arg).resolve() for arg in sys.argv[1:])
NS = "test"
PREFIX = "single-record-"
DIGEST = bytes(range(20))
CLUSTER_NAME = uuid.uuid4().hex[:12]


def local_ports():
    with ExitStack() as stack:
        ports = []
        for _ in range(3):
            sock = stack.enter_context(socket.socket())
            sock.bind(("127.0.0.1", 0))
            ports.append(sock.getsockname()[1])
        return ports


SERVICE, HEARTBEAT, FABRIC = local_ports()
CFG = """service {
 cluster-name CLUSTER_NAME
 work-directory WORK_DIR
 service-threads 64
 ticker-interval 1
 proto-fd-max 1024
}
mod-lua {
 user-path WORK_DIR/lua
}
logging {
 console {
  context any info
  context key-busy detail
 }
}
network {
 service {
  address 127.0.0.1
  port SERVICE_PORT
 }
 heartbeat {
  mode mesh
  address 127.0.0.1
  port HEARTBEAT_PORT
  interval 150
  timeout 10
 }
 fabric {
  address 127.0.0.1
  port FABRIC_PORT
 }
}
namespace test {
 replication-factor 1
 storage-engine memory {
  data-size 512M
 }
}
"""
YAML = {
    "service": {
        "cluster-name": CLUSTER_NAME,
        "service-threads": 64,
        "proto-fd-max": 1024,
    },
    "network": {
        "service": {"port": SERVICE, "addresses": ["127.0.0.1"]},
        "heartbeat": {
            "mode": "mesh",
            "port": HEARTBEAT,
            "addresses": ["127.0.0.1"],
            "interval": 150,
            "timeout": 10,
        },
        "fabric": {"port": FABRIC, "addresses": ["127.0.0.1"]},
    },
    "namespaces": {
        "test": {
            "replication-factor": 1,
            "storage-engine": {"type": "memory", "data-size": 536870912},
            "single-record-read-key-concurrency-limit": 5,
            "single-record-write-key-concurrency-limit": 4294967295,
        }
    },
}


def receive(s, n):
    result = bytearray()
    while len(result) < n:
        piece = s.recv(n - len(result))
        if not piece:
            raise EOFError("truncated response")
        result.extend(piece)
    return bytes(result)


def frame(s):
    length = int.from_bytes(receive(s, 8)[2:], "big")
    return receive(s, length)


def connect(kind, data):
    s = socket.create_connection(("127.0.0.1", SERVICE), timeout=4)
    s.sendall(bytes((2, kind)) + len(data).to_bytes(6, "big") + data)
    return s


def info(command):
    with connect(1, (command + "\n").encode()) as s:
        return frame(s).decode().strip().partition("\t")[2]


def fields(response):
    return {
        k: v
        for item in response.split(";")
        if "=" in item
        for k, v in [item.split("=", 1)]
    }


def config():
    return fields(info("get-config:context=namespace;id=test"))


def stats():
    return fields(info("namespace/test"))


def count(kind, stat="active"):
    k = f"single_record_{kind}_active" if stat == "active" else f"fail_{kind}_admission"
    return int(stats()[k])


def until(fn, timeout=12):
    stop = time.monotonic() + timeout
    while time.monotonic() < stop:
        try:
            if fn():
                return
        except (OSError, EOFError, KeyError):
            pass
        time.sleep(0.03)
    raise AssertionError("condition timed out")


def setcap(kind, limit, namespace=NS):
    return info(
        f"set-config:context=namespace;id={namespace};{PREFIX}{kind}-key-concurrency-limit={limit}"
    )


@contextmanager
def server(binary, config_path, log_path, yaml=False):
    command = [binary, "--foreground", "--config-file", str(config_path)]
    if yaml:
        command += ["--experimental", "--schema-file", SCHEMA]
    with log_path.open("wb") as stream:
        p = subprocess.Popen(command, stdout=stream, stderr=stream)
        try:

            def ready():
                if p.poll() is not None:
                    raise AssertionError(f"server exited with code {p.returncode}")
                return info("cluster-name") == CLUSTER_NAME

            until(ready)
            yield
        finally:
            if p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    p.kill()
                    p.wait()


def field(kind, value):
    return struct.pack(">IB", len(value) + 1, kind) + value


def msg(info1, info2, info4, fields_, ops):
    return struct.pack(
        ">BBBBBBIIIHH",
        22,
        info1,
        info2,
        0,
        info4,
        0,
        0,
        0,
        5000,
        len(fields_),
        len(ops),
    ) + b"".join(fields_ + ops)


def send(kind, flag1=0, flag4=0, digest=DIGEST, namespace=NS):
    f = [field(0, namespace.encode()), field(4, digest)]
    ops = []
    if kind == "write":
        value = struct.pack(">q", 1)
        ops = [struct.pack(">IBBBB", 5 + len(value), 2, 1, 0, 1) + b"x" + value]
    b = msg((3 if kind == "read" else 0) | flag1, int(kind == "write"), flag4, f, ops)
    with connect(3, b) as s:
        return frame(s)[5]


def batch_read():
    row = (
        struct.pack(">I", 0)
        + DIGEST
        + struct.pack(">BBHH", 0, 3, 1, 0)
        + field(0, b"test")
    )
    f = field(41, struct.pack(">IB", 1, 1) + row)
    with connect(3, msg(8, 0, 0, [f], [])) as s:
        result = frame(s)
        assert not result[3] & 1 and result[5] == 0, result.hex()
        # The batch index occupies transaction_ttl. The one seeded bin is x=1.
        assert struct.unpack_from(">IHH", result, 14) == (0, 0, 1)
        expected_bin = struct.pack(">IBBBB", 13, 1, 1, 0, 1) + b"x" + struct.pack(">q", 1)
        assert result[result[0] :] == expected_bin, result.hex()
        trailer = frame(s)
        assert trailer[3] & 1 and trailer[5] == 0, trailer.hex()


@contextmanager
def hold(marker):
    marker = Path(marker)
    marker.touch()
    try:
        yield
    finally:
        marker.unlink()


def test_lanes():
    until(lambda: send("write") == 0)
    assert send("read") == 0

    # info1 bit 4 marks XDR. info4 bits 0 and 1 mark MRT verify and roll.
    for op, wire_flags in [
        ("read", [(0, 0), (0, 1)]),
        ("write", [(0, 0), (16, 0), (0, 2)]),
    ]:
        marker = "hold-" + op
        with ThreadPoolExecutor(1) as pool:
            with hold(marker):
                active = pool.submit(send, op)
                until(Path("entered-" + op).exists)
                assert not active.done() and count(op) == 0
            assert active.result(timeout=5) == 0

            assert setcap(op, 1) == "ok"
            with hold(marker):
                active = pool.submit(send, op)
                until(lambda op=op: count(op) == 1)
                # The digests share shard bytes 12 and 13, but differ in byte 19.
                other_digest = DIGEST[:-1] + bytes([255])
                assert send("write", digest=other_digest) == 0
                assert send("read", digest=other_digest) == 0

                expected = count(op, "reject") + len(wire_flags)
                errors = int(stats()["client_tsvc_error"]) + len(wire_flags)
                for flag1, flag4 in wire_flags:
                    assert send(op, flag1, flag4) == 14, (op, flag1, flag4)
                assert count(op, "reject") == expected
                assert int(stats()["client_tsvc_error"]) == errors
                if op == "read":
                    batch_read()
                    assert count(op) == 1 and count(op, "reject") == expected
            assert active.result(timeout=5) == 0
            until(lambda op=op: count(op) == 0)

    with hold("async-write"):
        before = count("write", "reject")
        for _ in range(2):
            assert send("write") == 0
            until(lambda: count("write") == 0)
        assert count("write", "reject") == before
    print(
        "Per-digest admission, error accounting, batch read and simulated async handoff passed",
        flush=True,
    )


def test_live_limit():
    for op in ("read", "write"):
        assert setcap(op, 2) == "ok"
        assert setcap(op, 1, namespace="second") == "ok"
        entered = Path("entered-" + op)
        entered.unlink(missing_ok=True)
        with ThreadPoolExecutor(3) as pool:
            with hold("hold-" + op):
                first = pool.submit(send, op)
                second = pool.submit(send, op)
                until(lambda entered=entered: entered.stat().st_size == 2)
                assert count(op) == 2
                rejected = count(op, "reject")
                key_busy = int(stats()["fail_key_busy"])
                assert send(op) == 14
                assert send("write" if op == "read" else "read") == 0
                assert send(op, namespace="second") == 0
                assert setcap(op, 1) == "ok"
                assert send(op) == 14
                assert count(op, "reject") == rejected + 2
                assert int(stats()["fail_key_busy"]) == key_busy
                assert setcap(op, 0) == "ok"
                third = pool.submit(send, op)
                until(lambda entered=entered: entered.stat().st_size == 3)
                assert not third.done() and count(op) == 2
            for future in (first, second, third):
                assert future.result(timeout=5) == 0
            until(lambda op=op: count(op) == 0)
    print("Live lowering, disable while occupied and lane drain passed", flush=True)


def exercise_config():
    keys = [f"{PREFIX}{op}-key-concurrency-limit" for op in ("read", "write")]
    assert [config()[k] for k in keys] == ["0", "0"]
    for op in ("read", "write"):
        assert setcap(op, 7) == "ok"
    for op in ("read", "write"):
        for invalid in (
            "4294967296",
            "4294967297",
            str(2**64),
            "4G",
            "-1",
            "+1",
            "abc",
            "",
        ):
            assert "invalid set-config parameter" in setcap(op, invalid)
            assert [config()[k] for k in keys] == ["7", "7"]
    for op, k in zip(("read", "write"), keys):
        for limit in (5, 4294967295, 0):
            assert setcap(op, limit) == "ok"
            assert config()[k] == str(limit)
    print("Info set/get defaults, limits and overflow rejection passed", flush=True)


def test_requeue():
    # Pause the owner only after the server inserts its real rw_request.
    assert setcap("write", 2) == "ok"
    insertion = Path("hold-insertion")
    restart = Path("hold-restart")
    insertion.touch()
    restart.touch()
    with ThreadPoolExecutor(2) as pool:
        try:
            owner = pool.submit(send, "write")
            until(Path("inserted").exists)
            waiter = pool.submit(send, "write")
            until(Path("queued").exists)
            until(lambda: count("write") == 1)
            assert not waiter.done()
            insertion.unlink()
            assert owner.result(timeout=5) == 0
            until(Path("restarted").exists)
            until(lambda: count("write") == 1)
            assert setcap("write", 1) == "ok"
            assert send("write") == 14
        finally:
            insertion.unlink(missing_ok=True)
            restart.unlink(missing_ok=True)
        assert waiter.result(timeout=5) == 0
    until(lambda: count("write") == 0)
    print(
        "Waiting write released its slot and reacquired it on service requeue",
        flush=True,
    )


def rejected_startup(path, value, key, yaml=False):
    command = [ASD, "--foreground", "--config-file", path]
    if yaml:
        command.extend(["--experimental", "--schema-file", SCHEMA])
    result = subprocess.run(command, check=False, capture_output=True, timeout=8)
    log = (result.stdout + result.stderr).decode()
    assert result.returncode != 0 and str(value) in log and key in log, log[-3000:]


def test_startup(log):
    for op in ("read", "write"):
        key = f"{PREFIX}{op}-key-concurrency-limit"
        other = "write" if op == "read" else "read"
        other_key = f"{PREFIX}{other}-key-concurrency-limit"
        legacy = Path("test.conf")
        yaml = Path("test.yaml")
        values = copy.deepcopy(YAML)
        namespace = values["namespaces"]["test"]

        # Exercise both endpoints in both file formats.
        namespace[key], namespace[other_key] = 0, 2**32 - 1
        yaml.write_text(json.dumps(values))
        lines = f" {key} 0 # unlimited\n {other_key} {2**32 - 1}\n replication-factor 1"
        legacy.write_text(CFG.replace(" replication-factor 1", lines))
        for path in (legacy, yaml):
            with server(ASD, path, log, yaml=path == yaml):
                c = config()
                assert (c[key], c[other_key]) == ("0", str(2**32 - 1))
        for value in (2**32, 2**64, "1 K"):
            legacy.write_text(
                CFG.replace(
                    " replication-factor 1", f" {key} {value}\n replication-factor 1"
                )
            )
            rejected_startup(
                legacy, "single decimal value" if value == "1 K" else value, key
            )
        namespace[key] = 2**32
        yaml.write_text(json.dumps(values))
        rejected_startup(yaml, 2**32, key, yaml=True)
    print("Both legacy and YAML startup directives enforce their bounds", flush=True)


def run():
    log = Path("asd.log")
    second = CFG[CFG.index("namespace test {") :].replace(
        "namespace test", "namespace second"
    )
    config_path = Path("test.conf")
    config_path.write_text(CFG + second)
    try:
        with server(FIXTURE, config_path, log):
            exercise_config()
            test_lanes()
            until(lambda: "admission (2,3)" in log.read_text())
            assert "read key admission limit reached" in log.read_text()
            assert send("write", namespace="second") == 0
            test_live_limit()
            test_requeue()
        test_startup(log)
    except BaseException:
        if log.exists():
            print(log.read_text()[-5000:], file=sys.stderr)
        raise


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as work:
        os.chdir(work)
        Path("smd").mkdir()
        Path("lua").mkdir()
        CFG = (
            CFG.replace("SERVICE_PORT", str(SERVICE))
            .replace("HEARTBEAT_PORT", str(HEARTBEAT))
            .replace("FABRIC_PORT", str(FABRIC))
            .replace("CLUSTER_NAME", CLUSTER_NAME)
            .replace("WORK_DIR", work)
        )
        YAML["service"]["work-directory"] = work
        YAML["mod-lua"] = {"user-path": work + "/lua"}
        run()
