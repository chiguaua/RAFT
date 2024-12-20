from contextlib import asynccontextmanager
from fastapi import Body
import requests
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
import httpx

api_v1 = jsonrpc.Entrypoint('/api/v1/jsonrpc')

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
    node_list: Dict[int, Any]
    leader: int

class UpdateValOut(BaseModel):
    status:bool

class VotingIn(BaseModel):
    term: int
    port: int

class MySyncObj():
    cur_host = 'localhost'
    cur_port = 8014 #8010
    role = "follower" #'leader'
    term = 1
    nodes : Dict[int, Any] = {cur_port:0} # 8010
    leader_port = 8010
    last_ping = datetime.now()
    vote_for = None
    vote_start = datetime.now()

    def __init__(self):
        self.timer = RepeatTimer(5.0, self.do_work)
    
    def connect_to_net(self, port):
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
        
        try:
            response = requests.post(url, json=data, headers=headers)
        except Exception as err:
            print("No answer from ", str(port))
            return {'datas': 'error connection'}

        if response.status_code == 200:
            response = response.json()
            if(response["result"]["leader"] != port):
                self.connect_to_net(response["result"]["leader"])
        else:
            print('status code is not 200')
            return {'datas': 'error response'}

    def notificate_followers(self):
        for x, val in self.nodes.items():
            if (x != self.cur_port):
                url = "http://"+ self.cur_host + ":" + str(x) + "/api/v1/jsonrpc"
                headers = {'content-type': 'application/json'}
                data = {
                    "jsonrpc": "2.0",
                    "method": "update",
                    "id": 1,
                    "params": {
                    "in_params": {
                        "term": self.term,
                        "node_list": self.nodes,
                        "leader": self.cur_port
                    }
                }
                }
                
                try:
                    response = requests.post(url, json=data, headers=headers)
                except Exception as err:
                    print("No answer from ", str(x))
                    return {'datas': 'error connection'}

                if response.status_code != 200:
                    print('status code is not 200')
                    return {'datas': 'error response'}

    def ping_nodes(self):
        droped_nodes = []
        for node, val in my_raft.nodes.items():
            if node != my_raft.cur_port:
                url = f"http://{my_raft.cur_host}:{node}/api/v1/jsonrpc"
                headers = {'content-type': 'application/json'}
                data = {
                    "jsonrpc": "2.0",
                    "method": "ping",
                    "id": 1,
                    "params": {}
                }

                try:
                    response = requests.post(url, json=data, headers=headers)
                    if response.status_code == 200:
                        self.nodes[node] = 0
                    else:
                        self.nodes[node] = self.nodes[node] + 1
                except Exception as e:
                    print(f"Node {node} ping failed")
                    self.nodes[node] = self.nodes[node] + 1

                if (self.nodes[node] >= 3):
                    droped_nodes.append(node)
        if len(droped_nodes) > 0:
            for x in droped_nodes:
                del self.nodes[x]  
            print(f"Drop Node {droped_nodes}")
            self.term = my_raft.term + 1
            self.notificate_followers()            
            
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
            async with httpx.AsyncClient(timeout=1) as client:
                response = await client.post(url, json=data, headers=headers)
                print(f"Node {node} responded with status {response.json().get("result")}")
                if response.status_code == 200 and response.json().get("result"):
                    return 1
        except Exception as e:
            print(f"Node {node} vote failed")
            self.nodes[node] = self.nodes.get(node, 0) + 1  
        return 0  
    
    async def coronation(self):
        self.nodes[self.leader_port] = 1
        sleep_duration = random.uniform(0, 3) 
        print("sleep ", sleep_duration)
        time.sleep(sleep_duration)
        if (self.vote_for == None) :
            print("try to lead")
            self.role = "coron"
            votes = 1
            self.term = self.term + 1
            
            tasks = [self.send_vote_request(node) for node in self.nodes if node != self.cur_port]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            votes = sum(results) + 1
            print(f"Election finished. Received {votes} votes out of {len(tasks)} requests.")

            if (votes >len(tasks)/2):
                print("im a leader!")
                self.role = "leader"
                self.leader_port = self.cur_port
                my_raft.notificate_followers()
            else:
                print("folower...")
                self.role = "follower"
            
            return votes

    def do_work(self):
        print(f"hello im {self.cur_port} ({self.leader_port}), {self.role}, {self.term}")
        if self.leader_port not in self.nodes and self.role != "leader":
            self.connect_to_net(self.leader_port)
        if self.role == "leader":
            self.ping_nodes()
        if self.role == "follower":
            if  self.vote_start + timedelta(seconds=1) <= datetime.now():
                self.vote_for = None
            if self.last_ping + timedelta(seconds=7) <= datetime.now():
                print("leader Drop")
                asyncio.run(self.coronation())

    def start(self):
        self.timer.start()

@api_v1.method(errors=[MyError])
async def add_to_list(in_params: AddNodeIn) -> AddNodeOut:
    if (my_raft.role != "follower"):
        print("new node ", in_params.item)
        my_raft.nodes[in_params.item] = 0  
        my_raft.term = my_raft.term + 1
        print("All node ", my_raft.nodes)
        my_raft.notificate_followers()        
    else:
        print(f"route {in_params.item} to leader {my_raft.leader_port}")
    return AddNodeOut(leader=my_raft.leader_port) 

@api_v1.method(errors=[MyError])
async def update(in_params: UpdateValIn) -> UpdateValOut:
    if(my_raft.term >= in_params.term):
        raise MyError
    my_raft.term = in_params.term

    my_raft.leader_port = in_params.leader

    if in_params.node_list is not None and isinstance(in_params.node_list, dict):
        my_raft.nodes = in_params.node_list
        print("All node ", my_raft.nodes)

    return UpdateValOut(status= True) 

@api_v1.method(errors=[MyError])
async def ping() -> bool:
    my_raft.last_ping = datetime.now()
    my_raft.nodes[my_raft.leader_port] = 0
    return True

@api_v1.method(errors=[MyError])
async def vote(in_params: VotingIn) -> bool:
    if my_raft.role == "follower" and my_raft.term < in_params.term and my_raft.vote_for == None and my_raft.last_ping + timedelta(seconds=7) <= datetime.now():
        my_raft.vote_for = in_params.port
        my_raft.vote_start = datetime.now()
        return True
    print("request vote denied")
    return False

if __name__ == '__main__':
    my_raft = MySyncObj()

    @asynccontextmanager
    async def lifespan(app: jsonrpc.API):
        my_raft.start()
        yield

    app = jsonrpc.API(lifespan=lifespan)
    app.bind_entrypoint(api_v1)


    import uvicorn
    uvicorn.run(app, host=my_raft.cur_host, port=my_raft.cur_port, log_level='critical')