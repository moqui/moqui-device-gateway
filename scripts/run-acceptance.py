#!/usr/bin/env python3
# This software is in the public domain under CC0 1.0 Universal plus a
# Grant of Patent License.
# 
# To the extent possible under law, the author(s) have dedicated all
# copyright and related and neighboring rights to this software to the
# public domain worldwide. This software is distributed without any
# warranty.
# 
# You should have received a copy of the CC0 Public Domain Dedication
# along with this software (see the LICENSE.md file). If not, see
# <http://creativecommons.org/publicdomain/zero/1.0/>.

"""Isolated acceptance controller; only its own broker and scratch Postgres may be interrupted."""
import json
import os
import pathlib
import re
import shlex
import signal
import socket
import subprocess
import tempfile
import threading
import time
import secrets
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import unquote

COMPONENT = pathlib.Path(__file__).resolve().parents[1]
FRAMEWORK = COMPONENT.parents[2]
ROOT = pathlib.Path(tempfile.mkdtemp(prefix="moqui-gateway-acceptance-"))
BROKER = "moqui-gateway-acceptance-" + str(os.getpid())
ENV = dict(os.environ)
SCRATCH = None
POSTGRES = None
PAUSED = []
STOP = threading.Event()
FAILURES = []
RESPONSES = {}
TOKEN = secrets.token_hex(24)

def command(*args, **kwargs):
    return subprocess.run(args, check=True, text=True, capture_output=True, timeout=120, **kwargs).stdout.strip()

def broker_port():
    return int(command("docker","port",BROKER,"1883/tcp").rsplit(":",1)[1])

def await_port(port):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1",port),timeout=1): return
        except OSError: time.sleep(.1)
    raise RuntimeError("Listener did not start on " + str(port))

def pg_identity():
    global POSTGRES
    if POSTGRES: return POSTGRES
    if SCRATCH is None: raise RuntimeError("No owned scratch runtime was identified")
    candidates = list(SCRATCH.rglob("postmaster.pid"))
    if len(candidates) != 1: raise RuntimeError("Expected exactly one owned Postgres cluster")
    data = candidates[0].parent
    pid = int(candidates[0].read_text().splitlines()[0])
    exe = pathlib.Path(os.readlink("/proc/" + str(pid) + "/exe"))
    args = pathlib.Path("/proc/" + str(pid) + "/cmdline").read_bytes().split(b"\0")
    if exe.name != "postgres" or str(data).encode() not in args:
        raise RuntimeError("Refusing to control a process outside the owned scratch cluster")
    opts = shlex.split((data/"postmaster.opts").read_text())[1:]
    POSTGRES = (exe.with_name("pg_ctl"),data,opts,pid)
    return POSTGRES

def pause_pg():
    global PAUSED
    _,data,_,parent = pg_identity()
    parents = {}
    for path in pathlib.Path("/proc").iterdir():
        if not path.name.isdecimal(): continue
        try:
            raw = (path/"status").read_text()
            parents[int(path.name)] = int(re.search(r"^PPid:\s+(\d+)",raw,re.M)[1])
        except (OSError, TypeError): pass
    owned = {parent}
    for _ in range(10):
        children = {pid for pid,ppid in parents.items() if ppid in owned}
        if children <= owned: break
        owned |= children
    # The postmaster identity was checked against this run's scratch path; descendants belong to that cluster.
    PAUSED = sorted(owned, reverse=True)
    for pid in PAUSED:
        try: os.kill(pid,signal.SIGSTOP)
        except ProcessLookupError: pass

def resume_pg():
    global PAUSED
    for pid in PAUSED:
        try: os.kill(pid,signal.SIGCONT)
        except ProcessLookupError: pass
    PAUSED = []

def publish(message):
    count = int(message.get("count",message.get("repeat",1)))
    start = message.get("start")
    lines = "\n".join(json.dumps({"value":start+i if start is not None else message["value"]}) for i in range(count))+"\n"
    args = ("mosquitto_pub","-h","127.0.0.1","-p",str(broker_port()),"-u","artemis","-P","artemis",
            "-q","1","-t",message["topic"],"-l")
    if start is None and count > 1:
        for line in lines.splitlines():
            command(*args,input=line+"\n")
            time.sleep(.5)
    else: command(*args,input=lines)

class LinkProxy:
    """The datacenter link between the gateway and the broker; the publisher reaches the broker directly."""
    def __init__(self):
        self.server = socket.socket(); self.server.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        self.server.bind(("127.0.0.1",0)); self.server.listen(16)
        self.port = self.server.getsockname()[1]
        # The broker's published port is fixed for the container; docker cannot report it while it is stopped.
        self.upstream = broker_port()
        self.cut = threading.Event(); self.lock = threading.Lock(); self.live = []
        threading.Thread(target=self.accept,daemon=True).start()
    def accept(self):
        while not STOP.is_set():
            try: client,_ = self.server.accept()
            except OSError: return
            if self.cut.is_set(): client.close(); continue
            try: upstream = socket.create_connection(("127.0.0.1",self.upstream),timeout=5)
            except OSError: client.close(); continue
            with self.lock: self.live += [client,upstream]
            for a,b in ((client,upstream),(upstream,client)):
                threading.Thread(target=self.pipe,args=(a,b),daemon=True).start()
    def pipe(self,a,b):
        try:
            while data := a.recv(65536): b.sendall(data)
        except OSError: pass
        finally:
            for s in (a,b):
                try: s.shutdown(socket.SHUT_RDWR)
                except OSError: pass
    def set_cut(self,cut):
        if not cut: self.cut.clear(); return
        self.cut.set()
        with self.lock: live,self.live = self.live,[]
        for s in live:
            try: s.shutdown(socket.SHUT_RDWR); s.close()
            except OSError: pass

LINK = None

def evidence():
    logs = "\n".join(p.read_text(errors="replace") for p in ROOT.glob("*.log"))
    return {"queueDrops":sum("persistence queue is full" in line for line in logs.splitlines()),
            "storeErrors":logs.count("Device gateway samples were not saved"),
            "storeWaits":logs.count("Device gateway samples wait for the database")}

def perform(message):
    global POSTGRES
    action = message["action"]
    if action in ("publish","burst"): publish(message)
    elif action == "broker_stop": command("docker","stop","-t","3",BROKER)
    elif action == "broker_start": command("docker","start",BROKER); await_port(broker_port())
    elif action == "db_stop":
        ctl,data,_,_ = pg_identity(); command(str(ctl),"-D",str(data),"-m","immediate","-w","stop")
    elif action == "db_start":
        ctl,data,opts,_ = pg_identity()
        command(str(ctl),"-D",str(data),"-l",str(ROOT/"postgres-recovery.log"),"-o",shlex.join(opts),"-w","start")
        POSTGRES=None
    elif action == "net_cut": LINK.set_cut(True)
    elif action == "net_restore": LINK.set_cut(False)
    elif action == "db_pause": pause_pg()
    elif action == "db_resume": resume_pg()
    elif action == "wait": time.sleep(min(float(message["seconds"]),5))
    elif action != "evidence": raise ValueError("Unknown test action")
    return evidence()

class Controller(BaseHTTPRequestHandler):
    def do_GET(self):
        prefix = "/"+TOKEN+"/"
        if not self.path.startswith(prefix): self.send_error(403); return
        message=json.loads(unquote(self.path[len(prefix):]))
        identity=(message.get("phase"),message["id"])
        if identity in RESPONSES: response=RESPONSES[identity]
        else:
            try: response={"id":message["id"],"ok":True,**perform(message)}
            except Exception as error:
                detail=str(error)+": "+str(getattr(error,"stderr",""))
                FAILURES.append(detail); response={"id":message["id"],"ok":False,"error":detail}
            RESPONSES[identity]=response
        self.send_response(200); self.send_header("Content-Type","application/json"); self.end_headers()
        self.wfile.write(json.dumps(response).encode())
    def log_message(self,*args): pass

def run_specs(phase,runtime,mode):
    global SCRATCH, POSTGRES
    ENV.update(MOQUI_RUNTIME=str(runtime),gateway_acceptance_phase=phase)
    for name in ("request.json","response.json"):
        (ROOT/name).unlink(missing_ok=True)
    binary = os.environ.get("MOQUI_BINARY",str(FRAMEWORK/"target/debug/moqui"))
    with (ROOT/(phase+".log")).open("w") as log:
        child = subprocess.Popen([binary,"test","moqui-device-gateway","--acceptance","--feature",phase+":",
                                  "--db",mode,"--keep"],env=ENV,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
        watchdog=threading.Timer(300,child.kill); watchdog.start()
        try:
            for line in child.stdout:
                log.write(line); log.flush()
                match = re.search(r"Loading the test database .* in (/tmp/\S+)",line)
                if match:
                    if not match[1].startswith("/tmp/mt-"+str(child.pid)+"-"):
                        raise RuntimeError("Unexpected scratch identity; refusing process control")
                    SCRATCH=pathlib.Path(match[1]); POSTGRES=None
                print(line,end="",flush=True)
            if child.wait(timeout=30): raise RuntimeError("Acceptance phase failed: "+phase)
        finally:
            watchdog.cancel()
            resume_pg()
            if child.poll() is None:
                child.terminate()
                try: child.wait(timeout=30)
                except subprocess.TimeoutExpired: child.kill(); child.wait()

def main():
    runtime=ROOT/"runtime"; (runtime/"component").mkdir(parents=True)
    (runtime/"base-component").symlink_to(FRAMEWORK/"runtime/base-component",target_is_directory=True)
    (runtime/"conf").symlink_to(FRAMEWORK/"runtime/conf",target_is_directory=True)
    for name in ("moqui-math","moqui-device","moqui-device-gateway"):
        (runtime/"component"/name).symlink_to(FRAMEWORK/"runtime/component"/name,target_is_directory=True)
    print("Acceptance artifacts: "+str(ROOT),flush=True)
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1",0))
        port=reservation.getsockname()[1]
    command("docker","run","--pull=never","--name",BROKER,"--label","moqui.gateway.acceptance="+str(os.getpid()),
            "-d","-p","127.0.0.1:"+str(port)+":1883","-e","ARTEMIS_USER=artemis","-e","ARTEMIS_PASSWORD=artemis",
            "apache/activemq-artemis:latest")
    await_port(broker_port())
    ENV.update(gateway_acceptance_control_dir=str(ROOT),
               gateway_acceptance_broker_uri="mqtt://127.0.0.1:"+str(broker_port())+"?userName=artemis&password=artemis",
               device_gateway_device_id="GW_EDGE_01",device_gateway_error_notification_seconds="0")
    # Native mqtt:// options belong to DeviceConnection, so use the established legacy URI envelope here.
    global LINK
    LINK=LinkProxy()
    ENV["gateway_acceptance_broker_uri"]="paho-mqtt5:?brokerUrl=tcp://127.0.0.1:"+str(LINK.port)+"&userName=artemis&password=artemis&maxReconnectDelay=1000"
    controller=HTTPServer(("127.0.0.1",0),Controller)
    ENV["gateway_acceptance_control_url"]="http://127.0.0.1:"+str(controller.server_address[1])+"/"+TOKEN+"/"
    thread=threading.Thread(target=controller.serve_forever,daemon=True); thread.start()
    try:
        run_specs("main",runtime,"fresh")
        if SCRATCH is None: raise RuntimeError("Scratch runtime was not identified")
        POSTGRES=None
        run_specs("restart",SCRATCH,"existing")
        if FAILURES: raise RuntimeError("; ".join(FAILURES))
        print("Acceptance metrics: "+(ROOT/"metrics.json").read_text(),flush=True)
    finally:
        STOP.set(); resume_pg(); controller.shutdown(); controller.server_close(); thread.join(timeout=10)

def stop_owned_pg():
    global POSTGRES
    if SCRATCH is None or not list(SCRATCH.rglob("postmaster.pid")): return
    POSTGRES=None
    try:
        ctl,data,_,_=pg_identity()
        command(str(ctl),"-D",str(data),"-m","fast","-w","stop")
    except Exception as error: print("Owned Postgres cleanup: "+str(error),flush=True)

if __name__ == "__main__":
    try: main()
    finally:
        resume_pg()
        stop_owned_pg()
        subprocess.run(["docker","rm","-f","-v",BROKER],capture_output=True,timeout=30)
        print("Artifacts retained at "+str(ROOT),flush=True)

