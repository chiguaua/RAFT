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
import threading
from threading import Timer
from datetime import datetime, timedelta
import time
import random
import asyncio
# import httpx
import requests
import sys
import signal

import json

initial_port = sys.argv[1]
load_logs = len(sys.argv) > 2

api_v1 = jsonrpc.Entrypoint('/api/v1/jsonrpc')

vote_timeout = 0.5
action_timeout = 0.1
hb_timer = 0.1 # 0.1
drop_timeout = 0.5#0.5
sleep_max_time = 0.5

class RepeatTimer(Timer):
    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)

class MyError(jsonrpc.BaseError):
    CODE = 5000
    MESSAGE = 'My error'
    class DataModel(BaseModel):
        details: str

class VoteIn(BaseModel):
    term: int
    candidateId: int
    lastLogIndex: int
    lastLogTerm: int
class VoteOut(BaseModel):
    term: int
    voteGranted: bool
class AppendEntriesIn(BaseModel):
    term: int
    leaderId: int 
    prevLogIndex: int
    prevLogTerm: int
    entries: list[tuple[str, Any, int]]
    leaderCommit: int
class AppendEntriesOut(BaseModel):
    term: int
    success: bool
    
class MySyncObj():
    cur_host = 'localhost'
    role = "follower" #'leader'

    cur_port = int(initial_port)
    term = 0
    leader_port = 8010

    vote_for = None
    log : list[(str, int ,int)] = [] # index (log, target ,term)
    commitIndex = -1
    lastApplied = -1

    nextIndex = {} #####
    matchIndex : Dict[int,int] = {} # node, idx

    nodes : Dict[int, Any] = {}
    vote_start = datetime.now()
    vote_state = "sleep" # sleep -> drop -> candidat/vote
    #HB
    steps = (drop_timeout//hb_timer)//3
    curr_step = 0

    def wait(self, time = drop_timeout):
        self.riot.cancel()
        self.riot = Timer(time, self.do_work)
        self.riot.start()

    def __init__(self, load_logs):
        self.heartbeat = RepeatTimer(hb_timer, self.hearthbit)
        self.riot = Timer(drop_timeout, self.wait)
        if load_logs == True:
            self.load_log_from_file()
            self.role = "follower"
        else:
            # self.new_log("Node build", self.cur_port, self.term)
            print("connect")
            # self.new_log("Add", self.leader_port, 0)
            for i in [8010, 8011, 8012, 8013, 8014]:
                self.new_log("Add", i, 0)
           
    def new_log(self, act, target, term):
        if (act == "Add"):
            self.nodes[target] = datetime.now()
            self.matchIndex[target] = self.lastApplied
        if (act == "Drop"):
            self.nodes[target] = -1
        if act == "Lead":
           self.leader_port = target
        self.lastApplied = self.lastApplied + 1
        self.log.append((act, target, term))
        print("New log ", act, target, term)

    def save_log_to_file(self):
        try:
            filename = str(self.cur_port) + "_log.json"
            with open(filename, 'w') as file:
                json.dump(self.log, file, indent=4)
            print(f"Лог успешно сохранен в {filename}")
        except Exception as e:
            print(f"Ошибка при сохранении лога: {e}")

    def load_log_from_file(self):
        try:
            filename = str(self.cur_port) + "_log.json"
            with open(filename, 'r') as file:
                log_json = json.load(file)
            print(f"Лог успешно загружен из {filename}")
        except Exception as e:
            print(f"Ошибка при загрузке лога: {e}")
        for (act, target, term) in log_json:
            self.new_log(act, target, term)

    async def ping(self, node):
        url = "http://"+ self.cur_host + ":" + str(node) + "/api/v1/jsonrpc"
        headers = {'content-type': 'application/json'}
        
        prevLogIndex = max(self.matchIndex[node], 0)
       
        prevLogTerm = self.log[prevLogIndex][2]
        entries = self.log[prevLogIndex + 1:self.lastApplied+1] 

        # print(node, prevLogIndex, self.commitIndex)
        
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
            response = requests.post(url, json=data, headers=headers, timeout=hb_timer)
            if response.status_code ==  200:
                self.nodes[node] = datetime.now()
                if response.json()["result"]["term"] > self.term:
                    self.role = "follower"
                    print("WTF App ", node)
                elif response.json()["result"]["success"]:
                    self.matchIndex[node] = self.lastApplied
                else:
                    self.matchIndex[node] = self.matchIndex[node] - 1
                return
        except Exception as e:
            if (-1 != self.nodes[node]):
                print(f"Node {node} hb failed")
        
        if (-1 != self.nodes[node] and node in self.nodes and self.nodes[node] + timedelta(seconds=drop_timeout) < datetime.now() ):
            self.new_log("Drop", node, self.term)
        
    def hearthbit(self):
        # nodes_list = list(self.nodes.keys())
        # nodes_hb = nodes_list[int(len(nodes_list)*self.curr_step//self.steps) : int(len(nodes_list)*(self.curr_step + 1)//self.steps)]
        # self.curr_step = (self.curr_step + 1)%self.steps

        # asyncio.run(self.send_ping_requests(nodes_hb))        
        # tasks = [asyncio.create_task(self.append(x)) for x in self.nodes.keys() if x != self.cur_port]
        asyncio.run(self.KostbILb())
    
    async def KostbILb(self):
        tasks = [asyncio.create_task(self.ping(x)) for x in self.nodes.keys() if x != self.cur_port]
        # print("hb", self.log, self.commitIndex)
        
        if (self.commitIndex < self.lastApplied and 
            sum(1 for value in self.matchIndex.values() if value > self.commitIndex) >= len(self.matchIndex.values()) / 2):
            self.commitIndex = self.commitIndex +1
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
                    "candidateId": self.cur_port,
                    "lastLogIndex": self.lastApplied,
                    "lastLogTerm": self.log[self.lastApplied][2]
                }
            }
        }

        try:
            response = requests.post(url, json=data, headers=headers, timeout=vote_timeout/2)
            print(f"{node} vote responded {response.json()['result']['voteGranted']}")
            if response.status_code == 200 and response.json()['result']['voteGranted']:
                return 1
            if response.status_code == 200 and response.json()['result']['term'] > self.term:
                self.role = "follower"
                # print("WTF Vote")
                return 0
        except requests.exceptions.RequestException as e:
            print(f"{node} vote failed")
        return 0  
    
    async def try_lead(self):
        if (self.vote_for == self.cur_port) :
            votes = 1
            self.term = self.term + 1
            tasks = [asyncio.create_task(self.send_vote_request(node)) for node in self.nodes if node != self.cur_port]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            votes = sum(results) + 1
            print(f"Election finished\n{votes}/{len(tasks) + 1}")

            if (votes * 2 > len(tasks) + 1  and self.role == "candidat"):               
                self.role = "leader"
                self.leader_port = self.cur_port
                self.vote_state = "sleep"
                if len(self.nodes) == 0:
                    self.new_log("Add", self.cur_port, self.term)
                self.new_log("Lead", self.cur_port, self.term)
                try :
                    self.heartbeat.start()
                except Exception:
                    print("Double timer!!")
                print(f"hello im {self.leader_port}, {self.role}, {self.term}, {self.log}")
                for x in self.nodes.keys():
                    self.nodes[x] = datetime.now()
                    self.matchIndex[x] = self.commitIndex
                await self.KostbILb()
            else:
                print("folower...")
                self.role = "follower"
        
    def do_work(self):        
        if self.role == "follower":
            if self.vote_state == "sleep":
                sleep_duration = random.uniform(0, sleep_max_time) 
                print("leader Drop. sleep ", sleep_duration*1000//1)
                self.vote_state = "drop"
                self.vote_for = None
                self.wait(time = sleep_duration)
            elif self.vote_state == "drop" and self.vote_for == None:
                print("try to lead")
                self.role = "candidat"
                self.vote_state = "vote"
                self.vote_for = self.cur_port
                asyncio.run(self.try_lead())
                self.wait(time = vote_timeout)
            else:
                sleep_duration = random.uniform(0, sleep_max_time) 
                print("end_vote. sleep ", sleep_duration*1000//1)
                self.vote_state = "sleep"
                self.vote_for = None
                self.wait(time = sleep_duration)
            return
        if self.role == "candidat":
            self.role = "follower"
            print("WTF")
        self.wait()

    def start(self):
        self.wait(time = drop_timeout)

    def stop(self):
        print(self.log)
        self.save_log_to_file()
        self.heartbeat.cancel()
        self.riot.cancel()

@api_v1.method(errors=[MyError])
async def append(in_params: AppendEntriesIn) -> AppendEntriesOut:
    print(in_params.term)
    if(my_raft.term > in_params.term):
        return AppendEntriesOut(term= my_raft.term, success= False)
    
    my_raft.wait(time = drop_timeout)
    my_raft.vote_state = "sleep"

    if(my_raft.role != "follower" and my_raft.term < in_params.term):
        print("WTF new leader")
        my_raft.role = "follower"

    if(my_raft.lastApplied == in_params.prevLogIndex == in_params.leaderCommit and in_params.prevLogTerm == my_raft.log[in_params.prevLogIndex][2]):
        return AppendEntriesOut(term= my_raft.term, success= True)
        
    if(my_raft.lastApplied < in_params.prevLogIndex or 
       (my_raft.lastApplied == in_params.prevLogIndex and my_raft.log[in_params.prevLogIndex][2] != in_params.prevLogTerm)):
        return AppendEntriesOut(term= my_raft.term, success= False) 
    
    my_raft.leader_port = in_params.leaderId
    my_raft.log = my_raft.log[:in_params.prevLogIndex+1]

    for x in in_params.entries:
        my_raft.new_log(x[0], x[1], x[2])

    my_raft.term = in_params.term
    my_raft.lastApplied = len(my_raft.log)-1
    my_raft.commitIndex = in_params.leaderCommit

    return AppendEntriesOut(term = my_raft.term, success= True) 

@api_v1.method(errors=[MyError])
async def vote(in_params: VoteIn) -> VoteOut:
    if ( 
        my_raft.lastApplied <= in_params.lastLogIndex
        and my_raft.log[-1][2] <= in_params.lastLogTerm 
        and my_raft.term <= in_params.term 
        and my_raft.vote_for == None):
        if (my_raft.role == "follower" 
            and my_raft.vote_state == "drop"):
            my_raft.vote_for = in_params.candidateId
            my_raft.vote_state = "vote"
            my_raft.wait(vote_timeout)
            my_raft.term = in_params.term
            print("request vote for ", in_params.candidateId)
            return VoteOut(term= my_raft.term, voteGranted= True)
    print("Denied", in_params.candidateId, my_raft.leader_port)
    if(my_raft.term >= in_params.term and my_raft.role == "leader" and in_params.candidateId not in my_raft.nodes.keys()):
        my_raft.new_log("Add", in_params.candidateId, my_raft.term)
    return VoteOut(term= my_raft.term, voteGranted= False)

@api_v1.method(errors=[MyError])
async def get_logs() -> list[Any]:
    return [
        my_raft.cur_port,
        my_raft.commitIndex,
        my_raft.lastApplied, 
        my_raft.leader_port,
        my_raft.term,
        my_raft.nodes,
        my_raft.log]

@api_v1.method(errors=[MyError])
async def custom_log(in_params: Any) -> bool:
    if my_raft.role == "leader":
        my_raft.new_log("Custom log", in_params, my_raft.term)
    else:
        url = f"http://{my_raft.cur_host}:{my_raft.leader_port}/api/v1/jsonrpc"
        headers = {'content-type': 'application/json'}
        data = {
            "jsonrpc": "2.0",
            "method": "custom_log",
            "id": 1,
            "params": {
                "in_params": in_params
            }
        }
        requests.post(url, json=data, headers=headers, timeout=1)
    return True

def signal_handler(sig, frame):
    my_raft.stop()
    sys.exit(0) 

if __name__ == '__main__':
    my_raft = MySyncObj(load_logs)

    @asynccontextmanager
    async def lifespan(app: jsonrpc.API):
        my_raft.start()
        yield

    app = jsonrpc.API(lifespan=lifespan)
    app.bind_entrypoint(api_v1)

    signal.signal(signal.SIGINT, signal_handler)
    
    import uvicorn
    uvicorn.run(app, host=my_raft.cur_host, port=my_raft.cur_port, log_level='critical')