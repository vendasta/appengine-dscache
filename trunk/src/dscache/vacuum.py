""" appengine-dscache: A datastore-based implementation of memcache

Docs and examples: http://code.google.com/p/appengine-dscache/

Copyright 2010 VendAsta Technologies Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import datetime
from google.appengine.ext import webapp, ndb
from models import _DSCache

BATCH_DELETE_SIZE = 100

class Vacuum(webapp.RequestHandler):
    """ A vacuum to clean up old dscache entries. 
    
    Specify a url mapping to this handler, then add it to your cron.yaml (e.g., every 5 minutes synchronized).
    """
    
    def get(self):
        """ The GET method. """
        now = datetime.datetime.utcnow()
        
        query = _DSCache.query().filter(_DSCache.timeout < now)
        
        # this will just run until the rug gets pulled out (DeadlineExceededError)
        keys = query.fetch(BATCH_DELETE_SIZE, keys_only=True)

        ndb.delete_multi(keys)
        while len(keys) == BATCH_DELETE_SIZE:
            keys = query.fetch(BATCH_DELETE_SIZE, keys_only=True)
            ndb.delete_multi(keys)
