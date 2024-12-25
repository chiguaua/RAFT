from contextlib import asynccontextmanager
from fastapi import Body
import json
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi import FastAPI, BackgroundTasks, Request, Depends,WebSocket, WebSocketDisconnect, Query, Response
from typing import Set, Dict, Optional, Union, Any
from pydantic import BaseModel
import fastapi_jsonrpc as jsonrpc
import jinja2
import io
import base64
from threading import Timer
from datetime import datetime, timedelta
import time
import random
import asyncio
# import httpx БАН
import requests
import sys
import signal


initial_port = sys.argv[1]

api_v1 = jsonrpc.Entrypoint('/api/v1/jsonrpc')

ping_timeout = 0.1
vote_timeout = 0.2
action_timeout = 0.01
hb_timer = 0.04
drop_timeout = 0.5
sleep_max_time = 0.6

class RepeatTimer(Timer):
    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)
class MyError(jsonrpc.BaseError):
    CODE = 5000
    MESSAGE = 'My error'

    class DataModel(BaseModel):
        details: str
class AddNodeIn(BaseModel):
    item: int
class AddNodeOut(BaseModel):
    leader: int
class UpdateValIn(BaseModel):
    term: int
    node_list: list[int]
    leader: int
class UpdateValOut(BaseModel):
    status:bool
class VotingIn(BaseModel):
    term: int
    port: int

class MySyncObj():
    cur_host = 'localhost'
    cur_port = int(initial_port) #8010
    role = "follower" #'leader'
    term = 1
    nodes : Dict[int, Any] = {cur_port:datetime.now()}#, 8016:0} # 8010
    leader_port = 8010
    last_ping = datetime.now()
    vote_for = None
    vote_start = datetime.now()
    already_sleep = False

    #HB
    steps = (drop_timeout//hb_timer)//3
    curr_step = 0

    def __init__(self):
        self.timer = RepeatTimer(action_timeout, self.do_work)
        self.heartbeat = RepeatTimer(hb_timer, self.hearthbit)

    async def connect_to_net(self, port):
        url = "http://"+ self.cur_host + ":" + str(port) + "/api/v1/jsonrpc"
        headers = {'content-type': 'application/json'}
        data = {
            "jsonrpc": "2.0",
            "method": "add_to_list",
            "id": 1,
            "params": {
            "in_params": {
                "item": self.cur_port 
            }
        }
        }
        leader_get = port

        try:
            response = requests.post(url, json=data, headers=headers, timeout=vote_timeout)
            leader_get = response.json()["result"]["leader"]
            print("Succ connect")
        except Exception as e:
            print("Fail connect: ", e)
   
        if(leader_get != port and leader_get != self.cur_port):
            self.connect_to_net(leader_get)

    async def update_follower(self, node):
        url = "http://"+ self.cur_host + ":" + str(node) + "/api/v1/jsonrpc"
        headers = {'content-type': 'application/json'}
        node_list = list(self.nodes.keys())
        data = {
            "jsonrpc": "2.0",
            "method": "update",
            "id": 1,
            "params": {
            "in_params": {
                "term": self.term,
                "node_list": node_list,
                "leader": self.cur_port
            }
        }
        }
        try:
            response = requests.post(url, json=data, headers=headers, timeout=ping_timeout)
        except Exception as e:
            print(f"Node {node} update failed")


    def notificate_followers(self):
        tasks = [asyncio.create_task(self.update_follower(x)) for x in self.nodes if x != self.cur_port]
               
    async def ping_node(self, node):
        url = f"http://{self.cur_host}:{node}/api/v1/jsonrpc"
        headers = {'content-type': 'application/json'}
        data = {
            "jsonrpc": "2.0",
            "method": "ping",
            "id": 1,
            "params": {}
        }
        status = 400
          
        try:
            response = requests.post(url, json=data, headers=headers, timeout=ping_timeout)
            status = response.status_code
        except Exception as e:
            print(f"Node {node} ping failed")

        if status == 200:
            self.nodes[node] = datetime.now()
            if response.json().get("result") != self.term:
                print("desinchrone with ", node)
                await self.update_follower(node)  
        
        if node in self.nodes and self.nodes[node] + timedelta(seconds=drop_timeout) <= datetime.now():
            return node

    async def send_ping_requests(self, nodes_hb):
        if (self.role != "leader"):
            self.heartbeat.cancel()

        tasks = []
        for node in nodes_hb:
            if node != self.cur_port:
                tasks.append(self.ping_node(node))

        results = await asyncio.gather(*tasks)
        dropped_nodes = [node for node in results if node is not None]
        if len(dropped_nodes) > 0:
            for x in dropped_nodes:
                if x in self.nodes:
                    del self.nodes[x]  
            print(f"Drop Node {dropped_nodes}")
            self.term = self.term + 1
            self.notificate_followers()     
    
    def hearthbit(self):
        nodes_list = list(self.nodes.keys())
        nodes_hb = nodes_list[int(len(nodes_list)*self.curr_step//self.steps) : int(len(nodes_list)*(self.curr_step + 1)//self.steps)]
        self.curr_step = (self.curr_step + 1)%self.steps

        asyncio.run(self.send_ping_requests(nodes_hb))        
            
    async def send_vote_request(self, node):
        url = f"http://{self.cur_host}:{node}/api/v1/jsonrpc"
        headers = {'content-type': 'application/json'}
        data = {
            "jsonrpc": "2.0",
            "method": "vote",
            "id": 1,
            "params": {
                "in_params": {
                    "term": self.term,
                    "port": self.cur_port
                }
            }
        }

        try:
            response = requests.post(url, json=data, headers=headers, timeout=vote_timeout)
            print(f"{node} vote responded {response.json().get('result')}")
            if response.status_code == 200 and response.json().get("result"):
                return 1
        except requests.exceptions.RequestException as e:
            print(f"{node} vote failed")
        return 0  
    
    async def coronation(self):
        self.already_sleep = True
        sleep_duration = random.uniform(0, sleep_max_time) 
        print("sleep ", sleep_duration*1000//1)
        await asyncio.sleep(sleep_duration)
        if (self.vote_for == None and self.last_ping + timedelta(seconds=drop_timeout) <= datetime.now()) :
            print("try to lead")
            self.role = "candidat"
            votes = 1
            self.term = self.term + 1
            # self.send_vote_request(8016)
            tasks = [asyncio.create_task(self.send_vote_request(node)) for node in self.nodes if node != self.cur_port]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            votes = sum(results) + 1
            print(f"Election finished\n{votes}/{len(tasks)}")

            if (votes >len(tasks)/2 and self.role == "candidat"):               
                self.role = "leader"
                self.leader_port = self.cur_port
                my_raft.notificate_followers()
                try :
                    self.heartbeat.start()
                except Exception:
                    print("Double timer!!")
                print(f"hello im {self.leader_port}, {self.role}, {self.term}")
            else:
                print("folower...")
                self.role = "follower"
        
        self.already_sleep = False
        
    def do_work(self):
        # print(f"hello im {self.cur_port} ({self.leader_port}), {self.role}, {self.term}")
        if self.leader_port not in self.nodes and self.role != "leader":
            asyncio.run(self.connect_to_net(self.leader_port))
        if self.role == "follower":
            if  self.vote_start + timedelta(seconds=vote_timeout) <= datetime.now() and self.vote_for != None:
                print("end vote round")
                self.vote_for = None
            if self.last_ping + timedelta(seconds=drop_timeout) <= datetime.now() and self.vote_for == None and not self.already_sleep:
                print("leader Drop")
                asyncio.run(self.coronation())

    def start(self):
        self.timer.start()
    
    def stop(self):
        self.timer.cancel()
        self.heartbeat.cancel()

@api_v1.method(errors=[MyError])
async def add_to_list(in_params: AddNodeIn) -> AddNodeOut:
    if (my_raft.role != "follower"):
        print("new node ", in_params.item)
        my_raft.nodes[in_params.item] = datetime.now()
        my_raft.term = my_raft.term + 1
        print("All node ", my_raft.nodes.keys())
        my_raft.notificate_followers()        
    else:
        print(f"route {in_params.item} to leader {my_raft.leader_port}")
    return AddNodeOut(leader=my_raft.leader_port) 

@api_v1.method(errors=[MyError])
async def update(in_params: UpdateValIn) -> UpdateValOut:
    if(my_raft.term > in_params.term):
        print("Update term problem")
        return UpdateValOut(status= False) 

    my_raft.role = "follower"
    my_raft.leader_port = in_params.leader
    my_raft.last_ping = datetime.now()
    my_raft.nodes[my_raft.leader_port] = datetime.now()

    my_raft.term = in_params.term
    print(in_params.node_list)
    if in_params.node_list is not None and isinstance(in_params.node_list, list):
        my_raft.nodes = {key: 0 for key in in_params.node_list}
        print(f"hello im {my_raft.cur_port}({my_raft.leader_port}), {my_raft.role}, {my_raft.term}")

    return UpdateValOut(status= True) 

@api_v1.method(errors=[MyError])
async def ping() -> int:
    my_raft.last_ping = datetime.now()
    my_raft.nodes[my_raft.leader_port] = datetime.now()
    return my_raft.term

@api_v1.method(errors=[MyError])
async def vote(in_params: VotingIn) -> bool:
    if my_raft.role == "follower" and my_raft.term < in_params.term and my_raft.vote_for == None and my_raft.last_ping + timedelta(seconds=drop_timeout) <= datetime.now():
        my_raft.vote_for = in_params.port
        my_raft.vote_start = datetime.now()
        print("request vote for ", in_params.port)
        return True
    print("Denied", in_params.port)#, my_raft.role == "follower", my_raft.term <= in_params.term, my_raft.vote_for == None, my_raft.last_ping + timedelta(drop_timeout) <= datetime.now(), datetime.now())
    return False

def signal_handler(sig, frame):
    my_raft.stop()
    sys.exit(0) 

if __name__ == '__main__':
    my_raft = MySyncObj()

    @asynccontextmanager
    async def lifespan(app: jsonrpc.API):
        my_raft.start()
        yield

    app = jsonrpc.API(lifespan=lifespan)
    app.bind_entrypoint(api_v1)

    signal.signal(signal.SIGINT, signal_handler)  # Add signal handler
    
    import uvicorn
    uvicorn.run(app, host=my_raft.cur_host, port=my_raft.cur_port, log_level='critical')