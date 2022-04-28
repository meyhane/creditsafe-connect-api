import os
import json
from pickle import FALSE
import time
import requests
from urllib.parse import urlencode
from flask import Flask, request, Response, abort

from sesamutils import sesam_logger
from sesamutils.flask import serve
from werkzeug.exceptions import HTTPException

app = Flask(__name__)

logger = sesam_logger("creditsafe-connect-api")

try:
    BASE_URL = os.environ["BASE_URL"]
    USERNAME = os.environ["USERNAME"]
    PASSWORD = os.environ["PASSWORD"]

except KeyError as err:
    exit("missing mandatory ENVVAR(s)")

ACCESS_TOKEN = None
ACCESS_TOKEN_REFRESHED_AT = None
DEFAULT_PAGESIZE = int(os.environ.get("DEFAULT_PAGESIZE",100))

def _get_token(doForceRenew=False):
    global ACCESS_TOKEN
    global ACCESS_TOKEN_REFRESHED_AT
    # fetch new token if expiry time has passed or token is empty
    current_epoch = time.time()*1000
    if (not ACCESS_TOKEN or doForceRenew):
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        data = {}
        data["username"] = USERNAME
        data["password"] = PASSWORD
        resp = requests.post(url=f"{BASE_URL}/authenticate", headers=headers, json=data)
        ACCESS_TOKEN = resp.json().get("token")
        ACCESS_TOKEN_REFRESHED_AT = current_epoch
        logger.debug(
            f"returning new access_token with ACCESS_TOKEN_REFRESHED_AT={ACCESS_TOKEN_REFRESHED_AT}")
    return ACCESS_TOKEN

def _get_session(doRenewToken=False, headers=None):
        headers = headers or {}
        headers["Authorization"] =  "Bearer " + _get_token(doForceRenew=doRenewToken)
        session = requests.Session()
        session.headers.update(headers)
        return session

def _sesamify(entity, params_to_keep):
    ms_updated_property = params_to_keep.get("ms_updated_property")
    if ms_updated_property:
        entity["_updated"] = entity[ms_updated_property]
    return entity

def fetch_and_yield(path, params_to_forward, params_to_keep):
    is_first_yield = True
    doPage = params_to_forward.get("page") is None    
    params_to_forward.setdefault("pageSize",DEFAULT_PAGESIZE)
    session = _get_session()    
    session_reauthed = False
    yield '['
    while True:
        logger.debug(f"performing GET on url={BASE_URL}/{path}, params={params_to_forward}")
        response = session.get(url=f"{BASE_URL}/{path}", params=params_to_forward)

        if response.status_code == 401 and not session_reauthed:
            session = _get_session(doRenewToken=True)
            session_reauthed = True
            continue
        elif not response.ok:
            yield json.dumps({"original_response_text": response.json()})
            abort(response.status_code,response.text)
        response_from_source = response.json()
        entities_to_return = []
        if isinstance(response_from_source, dict):
            data_property = params_to_keep.get("ms_data_property", "data")
            if data_property in response_from_source:
                if isinstance(response_from_source[data_property], dict):
                    entities_to_return = [response_from_source[data_property]]
                elif isinstance(response_from_source[data_property], list):
                    entities_to_return = response_from_source[data_property]
        elif isinstance(response_from_source, list):
            entities_to_return = response_from_source
        
        for entity in entities_to_return:
            if is_first_yield:
                is_first_yield = False
            else:
                yield ','
            yield json.dumps(_sesamify(entity, params_to_keep))
        
        paging = response_from_source.get("paging",{})
        if not doPage or len(entities_to_return) == 0 or paging.get("next") is None or paging.get("next") > paging.get("last"):
            break
        else:
            params_to_forward["page"] = paging.get("next")
                
    yield ']'

def _get_params(params):
    params_to_forward = {}
    params_to_keep = {}
    
    for k in params:
        if k in ["since", "limit", "ms_since_param_at_src", "ms_updated_property", "ms_data_property"]:
            params_to_keep[k] = params[k]
            if k == "ms_since_param_at_src":
                params_to_forward[params[k]] = params["since"]
        else:
            params_to_forward[k] = params[k]
    
    return params_to_forward, params_to_keep   

@app.route('/<path:path>', methods=["GET"])
def get(path):
    try:        
        params_to_forward, params_to_keep = _get_params(request.args.to_dict())
        response_data = fetch_and_yield(path, params_to_forward, params_to_keep)
        return Response(response=response_data, content_type="application/json")
    except Exception as err:
        logger.exception(err)
        return Response(str(err), mimetype='plain/text', status=500)



@app.route('/<path:path>', methods=["PUT", "POST", "DELETE", "PATCH"])
def post(path):
    try:        
        headers = {
            "Content-Type": "application/json"
        }
        params_to_forward, params_to_keep = _get_params(request.args)
        session = _get_session(headers=headers)
        data = request.get_json(silent=(request.method=="DELETE"))
        logger.debug(f"performing {request.method} on url={BASE_URL}/{path}, params={params_to_forward}")
        response = session.request(request.method, url=f"{BASE_URL}/{path}", json=data, params=params_to_forward)
        
        if response.status_code == 401:
            session = _get_session(doRenewToken=True)
            response = session.request(request.method, url=f"{BASE_URL}/{path}", data=data, params=params_to_forward)
        if not response.ok:
            return Response(response=response.text, content_type="text/plain", status=response.status_code )

        return Response(response=response.text, content_type=response.headers.get("content-type"))
    except Exception as err:
        logger.exception(err)
        return Response(str(err), mimetype='plain/text', status=500)

if __name__ == '__main__':
    PORT = int(os.environ.get("PORT", 5000))
    if os.environ.get("WEBFRAMEWORK", "") == "FLASK":
        app.run(debug=True, host='0.0.0.0', port=PORT)
    else:
        serve(app, port=PORT)
