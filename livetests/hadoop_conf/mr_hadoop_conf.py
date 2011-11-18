# Copyright 2009-2010 Yelp
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""The classic MapReduce job: count the frequency of words.

But have the combiner double the count so we can make sure it's run.
"""

try:
    import json
except ImportError:
    import simplejson as json

from mrjob.job import MRJob


class MRHadoopConfTest(MRJob):

    PARTITIONER = 'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner'

    JOBCONF = {
        'stream.map.output.field.separator': '.',
        'stream.num.map.output.key.fields': '4',
        'map.output.key.field.separator': '.',
        'num.key.fields.for.partition': '2',
        'mapred.reduce.tasks': '3',
    }

    def mapper(self, key, value):
        yield value, 'x'

    def reducer_init(self):
        self.keys_seen = []

    def reducer(self, key, value):
        self.increment_counter('k', key)
        self.increment_counter('v', value)
        self.keys_seen.append(key)

    def reducer_final(self):
        yield None, self.keys_seen


if __name__ == '__main__':
    MRHadoopConfTest.run()
