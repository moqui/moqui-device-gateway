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

"""Own local Modbus/OPC UA/SFTP peers, and an EtherNet/IP Logix simulator when CPPPO_PYTHON names a Python with cpppo;
never stops an existing broker or database."""
import json
import os
import pathlib
import socket
import subprocess
import tempfile
import threading
import time
from modbus_peer import Server

COMPONENT=pathlib.Path(__file__).resolve().parents[1]
FRAMEWORK=COMPONENT.parents[2]
ROOT=pathlib.Path(tempfile.mkdtemp(prefix="moqui-driver-tests-"))
SFTP="moqui-driver-sftp-"+str(os.getpid())
BROKER="moqui-driver-broker-"+str(os.getpid())

def run(*args,**kwargs):
    return subprocess.run(args,check=True,text=True,capture_output=True,timeout=600,**kwargs).stdout.strip()

def free(port):
    with socket.socket() as test:
        test.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        test.bind(("127.0.0.1",port))

def ready(port):
    until=time.monotonic()+60
    while time.monotonic() < until:
        try:
            with socket.create_connection(("127.0.0.1",port),timeout=.2) as connection:
                if port != 2223 or connection.recv(128).startswith(b"SSH-"): return
        except OSError: pass
        time.sleep(.1)
    raise RuntimeError("Server did not start on "+str(port))

def main():
    free(12686); free(2223)
    print("Driver artifacts: "+str(ROOT),flush=True)
    runtime=ROOT/"runtime"; (runtime/"component").mkdir(parents=True)
    (runtime/"base-component").symlink_to(FRAMEWORK/"runtime/base-component",target_is_directory=True)
    (runtime/"conf").symlink_to(FRAMEWORK/"runtime/conf",target_is_directory=True)
    for name in ("moqui-math","moqui-device","moqui-device-gateway"):
        (runtime/"component"/name).symlink_to(FRAMEWORK/"runtime/component"/name,target_is_directory=True)
    environment=dict(os.environ,MOQUI_RUNTIME=str(runtime),CARGO_TARGET_DIR=str(FRAMEWORK/"target"))
    # The example's dev dependencies contain the local OPC UA server.
    build=subprocess.run(["cargo","build","--manifest-path",str(COMPONENT/"Cargo.toml"),"--example","opcua_test_server","-j","4"],
        cwd=FRAMEWORK,env=environment,text=True,stdout=(ROOT/"server-build.log").open("w"),stderr=subprocess.STDOUT)
    if build.returncode: raise RuntimeError("OPC UA example build failed; see server-build.log")
    server=subprocess.Popen([str(FRAMEWORK/"target/debug/examples/opcua_test_server"),"12686",str(ROOT/"server-pki")],
        stdout=(ROOT/"opcua.log").open("w"),stderr=subprocess.STDOUT)
    eip=None
    try:
        ready(12686)
        cpppo=os.environ.get("CPPPO_PYTHON")
        if cpppo:
            with socket.socket() as s: s.bind(("127.0.0.1",0)); eip_port=s.getsockname()[1]
            eip=subprocess.Popen([cpppo,"-m","cpppo.server.enip","--address","127.0.0.1:"+str(eip_port),"Speed=DINT","Temp=REAL","Level=INT"],
                stdout=(ROOT/"cpppo.log").open("w"),stderr=subprocess.STDOUT)
            ready(eip_port)
            environment.update(gateway_eip_endpoint="logix://127.0.0.1:"+str(eip_port))
        upload=ROOT/"upload"; upload.mkdir()
        run("docker","run","--pull=never","--name",SFTP,"--label","moqui.gateway.acceptance="+str(os.getpid()),"-d",
            "-p","127.0.0.1:2223:22","-v",str(upload)+":/home/foo/upload","atmoz/sftp:alpine","foo:pass:::upload")
        ready(2223)
        fingerprint=run("docker","exec",SFTP,"ssh-keygen","-lf","/etc/ssh/ssh_host_ed25519_key.pub","-E","sha256").split()[1]
        run("docker","run","--pull=never","--name",BROKER,"--label","moqui.gateway.acceptance="+str(os.getpid()),"-d",
            "-p","127.0.0.1::1883","-e","ARTEMIS_USER=artemis","-e","ARTEMIS_PASSWORD=artemis","apache/activemq-artemis:latest")
        broker_port=int(run("docker","port",BROKER,"1883/tcp").rsplit(":",1)[1]); ready(broker_port)
        with Server() as peer:
            thread=threading.Thread(target=peer.serve_forever,daemon=True); thread.start()
            environment.update(gateway_mqtt_test="1",gateway_opcua_test="1",gateway_sftp_test="1",
                gateway_sftp_fingerprint=fingerprint,gateway_sftp_output_dir=str(upload),gateway_modbus_test="1",
                gateway_mqtt_test_broker="tcp://127.0.0.1:"+str(broker_port),
                gateway_modbus_endpoint="modbus-tcp://127.0.0.1:"+str(peer.server_address[1]),
                device_gateway_device_id="GW_EDGE_01",device_gateway_error_notification_seconds="0",
                MQTT_USERNAME="artemis",MQTT_PASSWORD="artemis")
            binary=os.environ.get("MOQUI_BINARY",str(FRAMEWORK/"target/debug/moqui"))
            with (ROOT/"driver-tests.log").open("w") as log:
                result=subprocess.run([binary,"test","moqui-device","moqui-device-gateway","--db","fresh"],env=environment,stdout=log,stderr=subprocess.STDOUT)
            print((ROOT/"driver-tests.log").read_text(),flush=True)
            if result.returncode: raise RuntimeError("Driver round-trip tests failed")
            peer.shutdown(); thread.join(timeout=5)
    finally:
        if eip: eip.terminate(); eip.wait(timeout=20)
        server.terminate()
        try: server.wait(timeout=20)
        except subprocess.TimeoutExpired: server.kill(); server.wait()

if __name__ == "__main__":
    try: main()
    finally:
        with (ROOT/"broker.log").open("w") as log:
            subprocess.run(["docker","logs",BROKER],stdout=log,stderr=subprocess.STDOUT,timeout=30)
        subprocess.run(["docker","rm","-f","-v",SFTP],capture_output=True,timeout=30)
        subprocess.run(["docker","rm","-f","-v",BROKER],capture_output=True,timeout=30)
        print("Artifacts retained at "+str(ROOT),flush=True)

