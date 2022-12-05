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

import base64
import functions_framework
import os
import requests
from google.cloud import storage
from urllib.parse import urlparse
import gcsfs

uploadKey = f"{os.environ.get('DEST_BUCKET')}/{os.environ.get('DEST_PATH')}"
print(f"Upload location is {uploadKey}")

def parse_filename(url):
    parsedUrl = urlparse(url)
    return os.path.basename(parsedUrl.path)

def download_file(url,destBlobName,payer):
    fs = gcsfs.GCSFileSystem(project=os.environ.get('GCP_PROJECT'))
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with fs.open(f"{uploadKey}/{payer}/{destBlobName}", 'wb') as f: 
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)

@functions_framework.cloud_event
def subscribe(pubsub_event):
    url = base64.b64decode(pubsub_event.data["message"]["data"]).decode()
    payer = pubsub_event.data["message"]["attributes"]["payer"]
    filename = parse_filename(url)
    print(f"Streaming {filename} from {url} to {uploadKey}/{payer}")
    download_file(url,filename,payer)
    print(f"Uploaded {filename} to {uploadKey}/{payer}")
    return (200)
