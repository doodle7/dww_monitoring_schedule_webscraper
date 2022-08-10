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
    async with httpx.AsyncClient(event_hooks=({'request':[log_request],'response':[log_response]}),cookies = o.cookies) as client:
        r = await client.post(hyperlink,follow_redirects=True)
        while "You tried to reach a WaterWatch page directly from a bookmark (rather than starting from the main search page)" in r.text:
            r = client.post(hyperlink)
        results.append(r)
        return

k = time.perf_counter()
hyperlink_df = pd.read_csv("C://Users//seanm//OneDrive//agra//dww_project//dww_hyperlinks3.csv",encoding_errors='ignore')
o = httpx.get('https://www9.state.nj.us/DEP_WaterWatch_public/')
r = httpx.post(hyperlink_df.hyperlink[0],cookies=o.cookies)
results = []
asyncio.create_task(main())
print(o.cookies)
print(time.perf_counter() - k )
#print(results[0])

