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
import sys
import signal


initial_port = sys.argv[1]

api_v1 = jsonrpc.Entrypoint('/api/v1/jsonrpc')

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

class RepeatTimer(Timer):
    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)

class MySyncObj():
    cur_host = 'localhost'
    cur_port = int(initial_port) #8010
    role = "follower" #'leader'
    term = 1
    nodes : Dict[int, Any] = {cur_port:0} # 8010
    leader_port = 8010
    last_ping = datetime.now()
    vote_for = None
    vote_start = datetime.now()
     
    def nonee(self):
        i = 3
    def __init__(self):
        self.timer = RepeatTimer(5.0, self.nonee)


    def start(self):
        self.timer.start()
    def stop(self):
        self.timer.cancel()

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
        print("Update term problem")
    my_raft.term = in_params.term

    my_raft.leader_port = in_params.leader
    my_raft.last_ping = datetime.now()
    my_raft.nodes[my_raft.leader_port] = 0

    if in_params.node_list is not None and isinstance(in_params.node_list, dict):
        my_raft.nodes = in_params.node_list
        print(f"hello im ({my_raft.leader_port}), {my_raft.role}, {my_raft.term}")

    return UpdateValOut(status= True) 

@api_v1.method(errors=[MyError])
async def ping() -> int:
    my_raft.last_ping = datetime.now()
    my_raft.nodes[my_raft.leader_port] = 0
    return my_raft.term

@api_v1.method(errors=[MyError])
async def vote(in_params: VotingIn) -> bool:
    print("Denied", in_params.port, datetime.now())#, my_raft.role == "follower", my_raft.term <= in_params.term, my_raft.vote_for == None, my_raft.last_ping + timedelta(drop_timeout) <= datetime.now(), datetime.now())
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