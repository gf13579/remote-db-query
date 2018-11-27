#!/usr/bin/env python

import sys, time, json, shlex

from splunklib.searchcommands import Configuration
from splunklib.searchcommands import dispatch
from splunklib.searchcommands import GeneratingCommand
from splunklib.searchcommands import Option
from splunklib.searchcommands import validators
import splunklib.results as results

@Configuration()
class RemoteDbQueryCSCommand(GeneratingCommand):
    connection = Option(require=True)
    query = Option(require=True)

    def generate(self):

        collection = self.service.kvstore["scheduled_queries"]

        newkey = collection.data.insert(json.dumps({"LastUpdated":time.time(),"Connection":self.connection,"Status":"Scheduled","Query":self.query,"Results":""}))['_key']

        time.sleep(5)

        for i in range(5):
            x = collection.data.query_by_id(newkey)
            if (x['Status'] == 'Scheduled') or (x['Status'] == 'Fetched'):
                time.sleep(5)
            else:
                break

        if x['Status'] == 'Complete':
            j = json.loads(x['Results'])
            for item in j:
                yield item
        else:
            yield {'_time': time.time(), 'result': 'Unknown', '_raw': newkey }

dispatch(RemoteDbQueryCSCommand, sys.argv, sys.stdin, sys.stdout, __name__)
