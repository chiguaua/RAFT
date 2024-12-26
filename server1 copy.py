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

ping_timeout = 0.2
vote_timeout = 0.2
action_timeout = 0.1
hb_timer = 0.3
drop_timeout = 2
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

class VotingIn(BaseModel):
    term: int
    port: int

class AppendEntriesIn(BaseModel):
    term: int #term leader’sterm 
    leaderId: int # sofollowercanredirectclients
    prevLogIndex: int # index of log entry immediately preceding new ones
    prevLogTerm: int # term of prev Log Indexentry
    entries: list[tuple[str, int, int]] #logentriestostore(emptyforheartbeat; maysendmorethanoneforefficiency)
    leaderCommit: int #leader’s commitIndex

class AppendEntriesOut(BaseModel):
    term: int# currentTerm,forleadertoupdateitself
    success: bool# trueiffollowercontainedentrymatching prevLogIndexandprevLogTerm
    
class MySyncObj():
    cur_host = 'localhost'
    role = "follower" #'leader'

    cur_port = int(initial_port) #8010
    term = 0
    leader_port = 8010

    vote_for = None
    log : list[(str, int ,int)]= [("Born", cur_port, term)] # index (log, target ,term)
    commitIndex = 0
    lastApplied = 0 ####

    nextIndex = {} #####
    matchIndex : Dict[int,int] = {} # node, idx

    nodes : Dict[int, Any] = {}#, 8016:0} # 8010
    vote_start = datetime.now()
    already_sleep = False
    last_ping = datetime.now()
    #HB
    steps = (drop_timeout//hb_timer)//3
    curr_step = 0

    def __init__(self):
        self.timer = RepeatTimer(action_timeout, self.do_work)
        self.heartbeat = RepeatTimer(hb_timer, self.hearthbit)
        print("connect")
        asyncio.run(self.connect_to_net(self.leader_port))

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
            response = requests.post(url, json=data, headers=headers, timeout=1)
            leader_get = response.json()["result"]["leader"]
        except Exception as e:
            print("Fail connect ")
        
        if leader_get == self.cur_port:
            self.new_log("Add", self.cur_port, self.term)
            self.term = self.term + 1 
            self.new_log("Lead", self.cur_port, self.term)
        elif (leader_get == port):
            print("Succ connect ", port)
            self.leader_port = port
            self.last_ping = datetime.now()
        else:
            self.connect_to_net(leader_get)
           
    def new_log(self, act, target, term):
        if (act == "Add"):
            if target in self.nodes:
                return
            self.nodes[target] = datetime.now()
            self.matchIndex[target] = 0
        if (act == "Drop"):
            if  target in self.nodes:
                del self.nodes[target]
                del self.matchIndex[target]
            else:
                return
        if act == "Lead":
           self.leader_port = target
           if self.cur_port == target:
               self.role = "leader"
               self.heartbeat.start()
        
        self.commitIndex = self.commitIndex + 1
        self.log.append((act, target, term))
        print("New log ", act, target, term)

    async def append(self, node):
        url = "http://"+ self.cur_host + ":" + str(node) + "/api/v1/jsonrpc"
        headers = {'content-type': 'application/json'}
        
        prevLogIndex = self.matchIndex[node]
        prevLogTerm = self.log[prevLogIndex][2]
        entries = self.log[prevLogIndex+1:self.commitIndex+1] 
        data = {
            "jsonrpc": "2.0",
            "method": "append",
            "id": 1,
            "params": {
            "in_params": {
                "term": self.term,
                "leaderId": self.cur_port,
                "prevLogIndex": prevLogIndex,
                "prevLogTerm": prevLogTerm,
                "entries": entries,
                "leaderCommit": self.commitIndex
            }
        }
        }
        try:
            response = requests.post(url, json=data, headers=headers, timeout=ping_timeout)
            if response.status_code ==  200:
                self.nodes[node] = datetime.now()
                if response.json()["result"]["term"] > self.term:
                    self.role = "follower"
                elif response.json()["result"]["success"]:
                    self.matchIndex[node] = self.commitIndex
                else:
                    self.matchIndex[node] = self.matchIndex[node] - 1
                return
        except Exception as e:
            print(f"Node {node} append failed ")
        
        if (self.nodes[node] + timedelta(seconds=drop_timeout) < datetime.now()):
            self.new_log("Drop", node, self.term)
        
    
    def hearthbit(self):
        # nodes_list = list(self.nodes.keys())
        # nodes_hb = nodes_list[int(len(nodes_list)*self.curr_step//self.steps) : int(len(nodes_list)*(self.curr_step + 1)//self.steps)]
        # self.curr_step = (self.curr_step + 1)%self.steps

        # asyncio.run(self.send_ping_requests(nodes_hb))        
        # tasks = [asyncio.create_task(self.append(x)) for x in self.nodes.keys() if x != self.cur_port]
        asyncio.run(self.KostbILb())
    
    async def KostbILb(self):
        tasks = [asyncio.create_task(self.append(x)) for x in self.nodes.keys() if x != self.cur_port]
        # print("hb", self.log, self.commitIndex)
        results = await asyncio.gather(*tasks)
     
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
                if len(self.nodes) == 0:
                    self.new_log("Add", self.cur_port, self.term)
                self.new_log("Lead", self.cur_port, self.term)
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
          
        if self.role == "follower":
            if  self.vote_start + timedelta(seconds=vote_timeout) <= datetime.now() and self.vote_for != None:
                print("end vote round")
                self.vote_for = None
            if self.last_ping + timedelta(seconds=drop_timeout) <= datetime.now() and self.vote_for == None and not self.already_sleep:
                print("leader Drop ", datetime.now())
                # asyncio.run(self.coronation())

    def start(self):
        self.timer.start()
    
    def stop(self):
        self.timer.cancel()
        self.heartbeat.cancel()

@api_v1.method(errors=[MyError])
async def add_to_list(in_params: AddNodeIn) -> AddNodeOut:
    if (my_raft.role != "follower"):
        my_raft.new_log("Add", in_params.item, my_raft.term)
    else:
        print(f"route {in_params.item} to leader {my_raft.leader_port}")
    return AddNodeOut(leader=my_raft.leader_port) 

@api_v1.method(errors=[MyError])
async def append(in_params: AppendEntriesIn) -> AppendEntriesOut:
    # print("app get", my_raft.log, in_params)
    if(my_raft.term > in_params.term):
        return AppendEntriesOut(term= my_raft.term, success= False)
    my_raft.last_ping = datetime.now()
    if(my_raft.role != "follower"):
        print("WTF")
        my_raft.role = "follower"
    if(my_raft.commitIndex == in_params.prevLogIndex == in_params.leaderCommit and in_params.prevLogTerm == my_raft.log[in_params.prevLogIndex-1][2]):
        return AppendEntriesOut(term= my_raft.term, success= True)
        
    if(my_raft.commitIndex < in_params.prevLogIndex or 
       (my_raft.commitIndex == in_params.prevLogIndex and my_raft.log[in_params.prevLogIndex - 1][2] != in_params.prevLogTerm)):
        return AppendEntriesOut(term= my_raft.term, success= False) 
    

    my_raft.leader_port = in_params.leaderId
    my_raft.log = my_raft.log[:in_params.prevLogIndex+1]
    for x in in_params.entries:
        my_raft.new_log(x[0], x[1], x[2])

    my_raft.term = in_params.term
    my_raft.commitIndex = in_params.leaderCommit

    return AppendEntriesOut(term = my_raft.term, success= True) 

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