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


import functions_framework
import os
from google.cloud import pubsub_v1

project_id = os.environ.get('GCP_PROJECT')
topic_id = os.environ.get('PUBSUB_TOPIC')

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

@functions_framework.http
def json_input(request):
    json_data = request.get_json()
    if not json_data:
        return {"message": "Must provide a JSON payload"}, 400
    try:
        #print(json_data['files'])
        for payers in json_data['payers']:
            print("Payers")
            print(payers['payer'])
            print("Files")
            for url in payers['files']:
                print(url)
                future = publisher.publish(topic_path, url.encode("utf-8"), payer=payers['payer'])
                print(f'published message id {future.result()}')
        return "Ok", 200
    except KeyError as err:
        return {"message": "Unable to process"}, 422

