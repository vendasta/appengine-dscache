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

from google.appengine.ext import ndb

class _DSCache(ndb.Model):
    """ The actual dscache cache entry.
    
    Exactly one of the *_val items should have a non-None value.
    Timeout is a UTC absolute timeout.
    """

    int_val = ndb.IntegerProperty(indexed=False)
    float_val = ndb.FloatProperty(indexed=False)
    date_val = ndb.DateProperty(indexed=False)
    time_val = ndb.TimeProperty(indexed=False)
    datetime_val = ndb.DateTimeProperty(indexed=False)
    bool_val = ndb.BooleanProperty(indexed=False)
    str_val = ndb.StringProperty(indexed=False)
    text_val = ndb.TextProperty(indexed=False)
    json_val = ndb.TextProperty(indexed=False)
    blob_val = ndb.BlobProperty(indexed=False)
    cas_id = ndb.FloatProperty(indexed=False)
    
    timeout = ndb.DateTimeProperty()
