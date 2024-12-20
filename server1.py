from contextlib import asynccontextmanager
from fastapi import Body
import requests
import json
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi import FastAPI, BackgroundTasks, Request, Depends,WebSocket, WebSocketDisconnect, Query, Response
from typing import List, Dict, Optional, Union
from pydantic import BaseModel
import fastapi_jsonrpc as jsonrpc
import jinja2
import io
import base64
from threading import Timer


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

class AppendInDataModel(BaseModel):
    term: float
    leader_id: str

class AppendOutDataModel(BaseModel):
    term: float
    success: bool


class MySyncObj():
    cur_host = 'localhost'
    cur_port = 8011
    role = 'leader'
    term = 1

    def __init__(self):
        self.timer = RepeatTimer(5.0, self.do_work)

    def call_rpc(self, method: str, rpc_params: BaseModel, dest_port: int = None):
        url = "http://"+ self.cur_host + ":" + str(dest_port) + "/api/v1/jsonrpc"
        headers = {'content-type': 'application/json'}

        loc_json_rpc = {"jsonrpc": "2.0",
                        "id": "0",
                        "method": method,
                        "params": {'in_params': rpc_params.dict()}
                        }

        try:
            response = requests.post(url, data=json.dumps(loc_json_rpc), headers=headers, timeout=0.5)
        except Exception as err:
            print("No answer from ", dest_port)
            return {'datas': 'error connection'}

        if response.status_code == 200:
            response = response.json()

            if 'result' in response:
                return response['result']
            else:
                return {'datas': 'error fnc not found'}
        else:
            print('status code is not 200')
            return {'datas': 'error response'}

    def append_entries_handler(self, in_params: AppendInDataModel) -> AppendOutDataModel:
        print('req', in_params.model_dump())
        return AppendOutDataModel(term=in_params.term, success=True)


    def do_work(self):
        # print('hello im ')
        if (self.role == 'leader'):
            params = AppendInDataModel(term=self.term, leader_id='leader:'+str(self.cur_port))
            ret = self.call_rpc('append_entries', params, 8011)
            print('ret', ret)
            self.term = self.term + 1

    def start(self):
        self.timer.start()

@api_v1.method(errors=[MyError])
async def append_entries(in_params: AppendInDataModel) -> AppendOutDataModel:
    return my_raft.append_entries_handler(in_params)

if __name__ == '__main__':
    my_raft = MySyncObj()

    @asynccontextmanager
    async def lifespan(app: jsonrpc.API):
        my_raft.start()
        yield

    app = jsonrpc.API(lifespan=lifespan)
    app.bind_entrypoint(api_v1)
    # app.add_api_route("/append_entries", append_entries, methods=["GET"])
    # app.add_api_route("/train", train_pinn, methods=["GET"])

    import uvicorn
    uvicorn.run(app, host=my_raft.cur_host, port=my_raft.cur_port, log_level='critical')