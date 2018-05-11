#
# Copyright 2018 PyWren Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import absolute_import

import json
import os
import sys
import time

from multiprocessing.pool import ThreadPool as Pool
import pywren.runtime as runtime
import random
from functools import partial

import botocore
import botocore.session
from pywren import local

if sys.version_info > (3, 0):
    from . import version  # pylint: disable=relative-import
else:
    import version  # pylint: disable=relative-import

SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))




class LambdaInvoker(object):
    def __init__(self, region_name, lambda_function_name):

        self.session = botocore.session.get_session()

        self.region_name = region_name
        self.lambda_function_name = lambda_function_name
        self.lambclient = self.session.create_client('lambda',
                                                     region_name=region_name)
        self.TIME_LIMIT = True

        self.queue = []
        self.limit = 128

    def invoke(self, payload):
        """
        Invoke -- return information about this invocation
        """
        self.queue.append(payload)
        if len(self.queue) >= self.limit:
            self.flush()
        # FIXME check response
        return {}

    def _invoke(self, payload):
        payload['host_submit_timestamp_2'] = time.time()
        self.lambclient.invoke(FunctionName=self.lambda_function_name,
                               Payload=json.dumps(payload),
                               InvocationType='Event')

    def flush(self):

        pid = os.fork()
        print('flushing!')

        if pid == 0: # child
            with Pool(128) as pool:
                for result in pool.imap_unordered(self._invoke, self.queue):
                    # print('invoked!')
                    pass
                # self.lambclient.invoke(FunctionName=self.lambda_function_name,
                #                        Payload=json.dumps(payload),
                #                        InvocationType='Event')
            os._exit(0) # kill the child

        self.queue = []

    def config(self):
        """
        Return config dict
        """
        return {'lambda_function_name' : self.lambda_function_name,
                'region_name' : self.region_name}

class WarmInvoker(LambdaInvoker):
    def __init__(self, region_name, lambda_function_name, config, num):
        super().__init__(region_name, lambda_function_name)

        self.num = num

        self.runtime_meta_info = runtime.get_runtime_info(config['runtime'])

        def warm(_):
            if ('urls' in self.runtime_meta_info and
                    isinstance(self.runtime_meta_info['urls'], list) and
                    len(self.runtime_meta_info['urls']) >= 1):
                num_shards = len(self.runtime_meta_info['urls'])
                random.seed()
                runtime_url = random.choice(self.runtime_meta_info['urls'])

            response = self.lambclient.invoke(
                FunctionName=self.lambda_function_name,
                Payload=json.dumps({
                    'status': 'warm',
                    'runtime': config['runtime'],
                    'runtime_url': runtime_url,
                    'pywren_version': version.__version__}),
                InvocationType='RequestResponse')

        with ThreadPool(num) as pool:
            pool.map(warm, range(num))

        print('warmed {} instances!'.format(num))




class DummyInvoker(object):
    """
    A mock invoker that simply appends payloads to a list. You must then
    call run()

    does not delete left-behind jobs

    """

    def __init__(self):
        self.payloads = []
        self.TIME_LIMIT = False

    def invoke(self, payload):
        self.payloads.append(payload)

    def config(self): # pylint: disable=no-self-use
        return {}


    def run_jobs(self, MAXJOBS=-1, run_dir="/tmp/task"):
        """
        run MAXJOBS in the queue
        MAXJOBS = -1  to run all

        # FIXME not multithreaded safe
        """

        jobn = len(self.payloads)
        if MAXJOBS != -1:
            jobn = MAXJOBS
        jobs = self.payloads[:jobn]

        local.local_handler(jobs, run_dir,
                            {'invoker' : 'DummyInvoker'})

        self.payloads = self.payloads[jobn:]
