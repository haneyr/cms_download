#Copyright 2022 Google LLC
#
#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.

import asyncio
import base64
import concurrent.futures
import functions_framework
import functools
import gcsfs
import os
import requests

from google.cloud import storage
from urllib.parse import urlparse

uploadKey = f"{os.environ.get('DEST_BUCKET')}/{os.environ.get('DEST_PATH')}"
print(f"Upload location is {uploadKey}")

def parse_filename(url):
    parsedUrl = urlparse(url)
    return os.path.basename(parsedUrl.path)

async def get_size(url):
    response = requests.head(url)
    size = int(response.headers['Content-Length'])
    return size

def download_range(url, start, end, output, payer):
    headers = {'Range': f'bytes={start}-{end}'}
    response = requests.get(url, headers=headers)
    fs = gcsfs.GCSFileSystem(project=os.environ.get('GCP_PROJECT'))
    with fs.open(f"{uploadKey}/{payer}/{output}", 'wb') as f: 
        for part in response.iter_content(1024):
            f.write(part)

async def download(run, loop, url, output, payer, chunk_size=10485760):
    file_size = await get_size(url)
    chunks = range(0, file_size, chunk_size)
    fs = gcsfs.GCSFileSystem(project=os.environ.get('GCP_PROJECT'))
    tasks = [
        run(
            download_range,
            url,
            start,
            start + chunk_size - 1,
            f'{output}.part{i}',
            payer,
        )
        for i, start in enumerate(chunks)
    ]

    await asyncio.wait(tasks)
    
    with fs.open(f"{uploadKey}/{payer}/{output}", 'wb') as o:
            for i in range(len(chunks)):
                chunk_path = f'{output}.part{i}'

                with fs.open(f"{uploadKey}/{payer}/{chunk_path}", 'rb') as s:
                    o.write(s.read())
                fs.rm(f"{uploadKey}/{payer}/{chunk_path}")

@functions_framework.cloud_event
def subscribe(pubsub_event):
    url = base64.b64decode(pubsub_event.data["message"]["data"]).decode()
    payer = pubsub_event.data["message"]["attributes"]["payer"]
    filename = parse_filename(url)
    print(f"Streaming {filename} from {url} to {uploadKey}/{payer}")
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)
    loop = asyncio.new_event_loop()
    run = functools.partial(loop.run_in_executor, executor)
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(
            download(run, loop, url, filename, payer)
        )
    finally:
        loop.close()
    return 'Ok', 200
    
