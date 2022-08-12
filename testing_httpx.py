# -*- coding: utf-8 -*-
"""
Created on Wed Aug 10 11:10:54 2022

@author: seanm
"""

import asyncio
import httpx
import pandas as pd
import time
import requests


async def log_request(request):
    print(f"Request: {request.method} {request.url}" )
    
async def log_response(response):
    request = response.request
    print(f"Request: {request.method} {request.url} - Status {response.status_code}" )


async def main():
    tasks = []
    for hyperlink in hyperlink_df.hyperlink[:25]:
        tasks.append(get_monitoring_schedule(hyperlink))
    await asyncio.gather(*tasks)
    return
    
async def get_monitoring_schedule(hyperlink):
    async with httpx.AsyncClient(event_hooks=({'request':[log_request],'response':[log_response]})) as client:
        await client.get('https://www9.state.nj.us/DEP_WaterWatch_public/index.jsp')
        r = await client.post(hyperlink,timeout=10)
    while "You tried to reach a WaterWatch page directly from a bookmark (rather than starting from the main search page)" \
        in r.text and r.status_code != 200:
        await client.get('https://www9.state.nj.us/DEP_WaterWatch_public/index.jsp')
        r = await client.post(hyperlink,timeout=10)
    results.append(r)
    return

k = time.perf_counter()
hyperlink_df = pd.read_csv("C://Users//seanm//OneDrive//agra//dww_project//dww_hyperlinks3.csv",encoding_errors='ignore')
results = []
asyncio.create_task(main())
#print(r)
#print("You tried to reach a WaterWatch page directly from a bookmark (rather than starting from the main search page)" in r.text)
print(time.perf_counter() - k )
#print(results[0])

