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

from google.appengine.ext import db

class _DSCache(db.Model):
    """ The actual dscache cache entry.
    
    Exactly one of the *_val items should have a non-None value.
    Timeout is a UTC absolute timeout.
    """
    
    int_val = db.IntegerProperty(indexed=False)
    float_val = db.FloatProperty(indexed=False)
    date_val = db.DateProperty(indexed=False)
    time_val = db.TimeProperty(indexed=False)
    datetime_val = db.DateTimeProperty(indexed=False)
    bool_val = db.BooleanProperty(indexed=False)
    str_val = db.StringProperty(indexed=False)
    text_val = db.TextProperty(indexed=False)
    json_val = db.TextProperty(indexed=False)
    blob_val = db.BlobProperty(indexed=False)
    
    timeout = db.DateTimeProperty()
