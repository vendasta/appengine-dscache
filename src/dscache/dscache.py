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

import sha
import pickle
import datetime
import logging
from google.appengine.ext import db
from models import _DSCache

try:
    import simplejson
    HAS_SIMPLEJSON = True
except ImportError:
    HAS_SIMPLEJSON = False

MAX_STR_LENGTH = 1024
MAX_KEY_SIZE = 500

__all__ = ['set', 'set_multi', 'get', 'get_multi', 'delete', 'delete_multi', 'add', 'add_multi', 
           'replace', 'replace_multi', 'incr', 'decr', 'offset_multi', 'flush_all', 'get_stats', 'Client',
           'STRONG_CONSISTENCY', 'EVENTUAL_CONSISTENCY']

STRONG_CONSISTENCY = db.STRONG_CONSISTENCY
EVENTUAL_CONSISTENCY = db.EVENTUAL_CONSISTENCY

def build_ds_key_name(key, key_prefix='', namespace=None):
    """ Builds a key_name. """
    if not key or not isinstance(key, (str, unicode)):
        raise ValueError('key "%s" must be string' % key)
    
    server_key = '%s%s' % (key_prefix, key)
    if namespace:
        server_key = '%s:%s' % (namespace, server_key)
    
    if len(server_key) > MAX_KEY_SIZE:
        server_key = sha.new(server_key).hexdigest()
    
    return server_key
    
def build_ds_key(key, key_prefix='', namespace=None):
    """ Builds a Key instance. """
    return db.Key.from_path('_DSCache', build_ds_key_name(key, key_prefix=key_prefix, namespace=namespace),
                            namespace='')
    
def set_value_on_entity(entity, value):
    """ Set a type-specific attribute on the entity. This is convenient when viewing the datastore
    and avoids (extra) casting overhead. """
    if isinstance(value, (str, unicode)):
        if len(value) < MAX_STR_LENGTH:
            entity.str_val = value
        else:
            entity.text_val = value
    elif isinstance(value, (bool)):
        entity.bool_val = value
    elif isinstance(value, (int, long)):
        entity.int_val = value
    elif isinstance(value, (float)):
        entity.float_val = value
    elif isinstance(value, (datetime.datetime)):
        entity.datetime_val = value
    elif isinstance(value, (datetime.date)):
        entity.date_val = value
    elif isinstance(value, (datetime.time)):
        entity.time_val = value
    else:
        # attempt to JSON encode - it's much faster than pickle and nicer to read in datastore viewer
        if HAS_SIMPLEJSON:
            try:
                json = simplejson.dumps(value)
                entity.json_val = json
            except Exception:
                entity.blob_val = pickle.dumps(value)
        else:
            entity.blob_val = pickle.dumps(value)

def get_value_from_entity(entity):
    """ Gets a value from the entity. Only one value should be non-None. """
    if not entity:
        return None
        
    if is_entity_expired(entity):
        return None
        
    if entity.int_val is not None:
        return entity.int_val
    if entity.float_val is not None:
        return entity.float_val
    if entity.date_val is not None:
        return entity.date_val
    if entity.time_val is not None:
        return entity.time_val
    if entity.datetime_val is not None:
        return entity.datetime_val
    if entity.bool_val is not None:
        return entity.bool_val
    if entity.str_val is not None:
        return entity.str_val
    if entity.text_val is not None:
        return entity.text_val
    if entity.json_val is not None:
        if HAS_SIMPLEJSON:
            return simplejson.loads(entity.json_val)
        raise Exception('JSON value in DSCache, but simplejson cannot be imported. Value can not be deserialized.')
    if entity.blob_val is not None:
        return pickle.loads(entity.blob_val)
    
    # entity had no non-None values, log this and dump the entity
    logging.warn('dscache: entity does not contain any values "%s".', entity.key().name())
    try:
        entity.delete()
    except Exception:
        logging.exception('dscache: error deleting bad cache entry "%s".', entity.key().name())
    return None
    
def compute_timeout(time):
    """ Computes a datetime [time] seconds from now. """
    timeout = datetime.datetime.utcnow() + datetime.timedelta(seconds=time)
    return timeout
    
def is_entity_expired(entity):
    """ Returns False if the entity's timeout was in the past. """
    if entity and entity.timeout and entity.timeout < datetime.datetime.utcnow():
        return True
    return False
    
def create_entity(key, value, time=0, key_prefix='', namespace=None):
    """ Creates a new _DSCache entity (without putting it). """
    key = build_ds_key(key, key_prefix=key_prefix, namespace=namespace)
    entity = _DSCache(key=key)
    set_value_on_entity(entity, value)
    if time:
        entity.timeout = compute_timeout(time)
    return entity

def set(key, value, time=0, namespace=None):
    """ Sets a key's value, regardless of previous contents in cache. 
    
    The return value is True if set, False on error.
    """
    entity = create_entity(key, value, time=time, namespace=namespace)
    try:
        entity.put()
    except Exception:
        logging.exception('dscache: error on dscache.set(). %s', key)
        return False
    else:
        return True
        
def _chunks(l, n):
    """ Breaks a list l into chunks of maximum size n. """
    return [l[i:i+n] for i in range(0, len(l), n)]
    
def set_multi(mapping, time=0, key_prefix='', namespace=None):
    """ Set multiple keys' values at once. Reduces the network latency of doing many requests in serial. 
    
    The return value is a list of keys whose values were NOT set. On total success, this list should be empty.
    
    Datastore has a limit of 500 entities at a time on put(), so if there are more than 500 passed, they
    are put() 500 at a time until the mapping is exhausted. If this causes too much delay, the client should 
    subset the mapping before calling this function.
    """
    # create a list of tuples: [ (entity_for_datastore, mapping_key), ... ]
    entity_key_tuples = [(create_entity(key, value, time=time, key_prefix=key_prefix, namespace=namespace), key)
                         for key, value in mapping.items()]

    # chunk tuples into lists of max 500
    entities = _chunks(entity_key_tuples, 500)
        
    failed_keys = []
    for sub_list in entities:
        entities, keys = zip(*sub_list)
        try:
            db.put(entities)
        except Exception:
            s = str(keys)
            if len(s) > 50:
                s = s[:50]
                s += '...'
            logging.exception('dscache: error on dscache.set_multi(). %s', s)
            failed_keys.extend(keys)
    return failed_keys

def get(key, namespace=None, read_policy=STRONG_CONSISTENCY):
    """ Looks up a single key in dscache. 
    
    The return value is the value of the key, if found in dscache, else None.
    """
    ds_key = build_ds_key(key, namespace=namespace)
    try:
        rpc = db.create_rpc(read_policy=read_policy)
        entity = db.get(ds_key, rpc=rpc)
    except Exception:
        logging.exception('dscache: error on dscache.get(). %s', key)
        return None
    else:
        return get_value_from_entity(entity)
    
def get_multi(keys, key_prefix='', namespace=None, read_policy=STRONG_CONSISTENCY):
    """ Looks up multiple keys from dscache in one operation. This is the recommended way to do bulk loads. 
    
    The returned value is a dictionary of the keys and values that were present in dscache. 
    Even if the key_prefix was specified, that key_prefix won't be on the keys in the returned dictionary.
    """
    ds_keys = [build_ds_key(key, key_prefix=key_prefix, namespace=namespace) for key in keys]
    try:
        rpc = db.create_rpc(read_policy=read_policy)
        entities = db.get(ds_keys, rpc=rpc)
    except Exception:
        s = str(keys)
        logging.exception('dscache: error on dscache.get_multi(). %s', s[:50])
        return {}
    else:
        entity_map = dict([(entity.key(), entity) for entity in entities if entity])
        result = {}
        for key in keys:
            ds_key = build_ds_key(key, key_prefix=key_prefix, namespace=namespace)
            if ds_key in entity_map:
                value = get_value_from_entity(entity_map[ds_key])
                if value:
                    result[key] = value
        return result
    
def delete(key, seconds=0, namespace=None):
    """ Deletes a key from dscache. 
    
    #The return value is 0 (DELETE_NETWORK_FAILURE) on network failure, 1 (DELETE_ITEM_MISSING) 
    #if the server tried to delete the item but didn't have it, and 2 (DELETE_SUCCESSFUL) if the 
    #item was actually deleted. This can be used as a boolean value, where a network failure is the only bad condition.
    
    Returns True if successful, False otherwise.
    """
    if seconds != 0:
        raise NotImplementedError('delete lock not implemented.')
    ds_key = build_ds_key(key, namespace=namespace)
    try:
        db.delete(ds_key)
    except Exception:
        logging.exception('dscache: error on dscache.delete() %s', key)
        return False
    else:
        return True
    
def delete_multi(keys, seconds=0, key_prefix='', namespace=None):
    """ Delete multiple keys at once. 
    
    The return value is True if all operations completed successfully. False if one or more failed to complete.
    """
    if seconds != 0:
        raise NotImplementedError('delete lock not implemented.')
    ds_keys = [build_ds_key(key, key_prefix=key_prefix, namespace=namespace) for key in keys]
    try:
        db.delete(ds_keys)
    except Exception:
        s = str(keys)
        logging.exception('dscache: error on dscache.delete_multi(). %s', s[:50])
        return False
    else:
        return True
    
def add(key, value, time=0, namespace=None):
    """ Sets a key's value, if and only if the item is not already in dscache. 
    
    The return value is True if added, False on error.
    """
    # this should use get_or_insert, but that doesn't provide the information necessary to see if inserted,
    # so we aren't able to return the correct response
    ds_key = build_ds_key(key, namespace=namespace)
    def tx():
        """ Tries to get an existing entity, and adds a new one if not found. """
        result = False
        existing_entity = db.get(ds_key)
        if (not existing_entity) or (is_entity_expired(existing_entity)):
            entity = create_entity(key, value, time=time, namespace=namespace)
            entity.put()
            result = True
        return result
    try:
        return db.run_in_transaction(tx)
    except Exception:
        logging.exception('dscache: error on dscache.add(). %s', key)
        return False
    
def add_multi(mapping, time=0, key_prefix='', namespace=None):
    """ Adds multiple values at once, with no effect for keys already in dscache. 
    
    The return value is a list of keys whose values were not set because they were already set in dscache, or an empty list.
    """
    # this is difficult to do efficiently
    raise NotImplementedError()
    
def replace(key, value, time=0, namespace=None):
    """ Replaces a key's value, failing if item isn't already in dscache. 
    
    The return value is True if replaced. False on error or cache miss.
    """
    raise NotImplementedError()
    
def replace_multi(mapping, time=0, key_prefix='', namespace=None):
    """ Replaces multiple values at once, with no effect for keys not in dscache. 
    
    The return value is a list of keys whose values were not set because they were not set in dscache, or an empty list.
    """
    raise NotImplementedError()

def incr(key, delta=1, namespace=None, initial_value=None):
    """ Atomically increments a key's value. Internally, the value is a unsigned 64-bit integer. 
    dscache doesn't check 64-bit overflows. The value, if too large, will wrap around.
    If the key does not yet exist in the cache and you specify an initial_value, 
    the key's value will be set to this initial value and then incremented. 
    If the key does not exist and no initial_value is specified, the key's value will not be set. 
    
    The return value is a new long integer value, or None if key was not in the cache or 
    could not be incremented for any other reason.
    """
    raise NotImplementedError()
    
def decr(key, delta=1, namespace=None, initial_value=None):
    """ Atomically decrements a key's value. Internally, the value is a unsigned 64-bit integer. 
    dscache doesn't check 64-bit overflows. The value, if too large, will wrap around.
    If the key does not yet exist in the cache and you specify an initial_value, 
    the key's value will be set to this initial value and then decremented. 
    If the key does not exist and no initial_value is specified, the key's value will not be set. 
    
    The return value is a new long integer value, or None if key was not in the cache or could not be 
    decremented for any other reason.
    """
    raise NotImplementedError()
    
def offset_multi(mapping, key_prefix='', namespace=None, initial_value=None):
    """ Increments or decrements multiple keys with integer values in a single service call. 
    Each key can have a separate offset. The offset can be positive or negative.
    Applying an offset to a single key is atomic. Applying an offset to multiple keys may 
    succeed for some keys and fail for others. 
    
    The return value is a mapping of the provided keys to their new values. 
    If there was an error applying an offset to a key, if a key doesn't exist in the cache and 
    no initial_value is provided, or if a key is set with a non-integer value, its return value is None.
    """
    raise NotImplementedError()
    
def flush_all():
    """ Deletes everything in dscache.
    
    The return value is True on success, False on RPC or server error."""
    raise NotImplementedError()
    
def get_stats():
    """ Gets dscache statistics for this application. All of these statistics may 
    reset due to various transient conditions. They provide the best information 
    available at the time of being called.

    The return value is a dictionary mapping statistic names to associated values. """
    raise NotImplementedError()


class Client(object):
    """ A Client() interface for memcached compatibility. """

    def set(self, key, value, time=0, namespace=None):
        """ Sets a key's value, regardless of previous contents in cache. 

        The return value is True if set, False on error.
        """
        return set(key, value, time=time, namespace=namespace)

    def set_multi(self, mapping, time=0, key_prefix='', namespace=None):
        """ Set multiple keys' values at once. Reduces the network latency of doing many requests in serial. 

        The return value is a list of keys whose values were NOT set. On total success, this list should be empty.
        """
        return set_multi(mapping, time=time, key_prefix=key_prefix, namespace=namespace)

    def get(self, key, namespace=None, read_policy=STRONG_CONSISTENCY):
        """ Looks up a single key in dscache. 

        The return value is the value of the key, if found in dscache, else None.
        """
        return get(key, namespace=namespace, read_policy=read_policy)

    def get_multi(self, keys, key_prefix='', namespace=None, read_policy=STRONG_CONSISTENCY):
        """ Looks up multiple keys from dscache in one operation. This is the recommended way to do bulk loads. 

        The returned value is a dictionary of the keys and values that were present in dscache. 
        Even if the key_prefix was specified, that key_prefix won't be on the keys in the returned dictionary.
        """
        return get_multi(keys, key_prefix=key_prefix, namespace=namespace, read_policy=read_policy)

    def delete(self, key, seconds=0, namespace=None):
        """ Deletes a key from dscache. 

        #The return value is 0 (DELETE_NETWORK_FAILURE) on network failure, 1 (DELETE_ITEM_MISSING) 
        #if the server tried to delete the item but didn't have it, and 2 (DELETE_SUCCESSFUL) if the 
        #item was actually deleted. This can be used as a boolean value, where a network failure is the only bad condition.

        Returns True if successful, False otherwise.
        """
        return delete(key, seconds=seconds, namespace=namespace)

    def delete_multi(self, keys, seconds=0, key_prefix='', namespace=None):
        """ Delete multiple keys at once. 

        The return value is True if all operations completed successfully. False if one or more failed to complete.
        """
        return delete_multi(keys, seconds=seconds, key_prefix=key_prefix, namespace=namespace)

    def add(self, key, value, time=0, namespace=None):
        """ Sets a key's value, if and only if the item is not already in dscache. 

        The return value is True if added, False on error.
        """
        return add(key, value, time=time, namespace=namespace)

    def add_multi(self, mapping, time=0, key_prefix='', namespace=None):
        """ Adds multiple values at once, with no effect for keys already in dscache. 

        The return value is a list of keys whose values were not set because they were already set in dscache, or an empty list.
        """
        return add_multi(mapping, time=time, key_prefix=key_prefix, namespace=namespace)

    def replace(self, key, value, time=0, namespace=None):
        """ Replaces a key's value, failing if item isn't already in dscache. 

        The return value is True if replaced. False on error or cache miss.
        """
        return replace(key, value, time=time, namespace=namespace)

    def replace_multi(self, mapping, time=0, key_prefix='', namespace=None):
        """ Replaces multiple values at once, with no effect for keys not in dscache. 

        The return value is a list of keys whose values were not set because they were not set in dscache, or an empty list.
        """
        return replace_multi(mapping, time=time, key_prefix=key_prefix, namespace=namespace)

    def incr(self, key, delta=1, namespace=None, initial_value=None):
        """ Atomically increments a key's value. Internally, the value is a unsigned 64-bit integer. 
        dscache doesn't check 64-bit overflows. The value, if too large, will wrap around.
        If the key does not yet exist in the cache and you specify an initial_value, 
        the key's value will be set to this initial value and then incremented. 
        If the key does not exist and no initial_value is specified, the key's value will not be set. 

        The return value is a new long integer value, or None if key was not in the cache or 
        could not be incremented for any other reason.
        """
        return incr(key, delta=delta, namespace=namespace, initial_value=initial_value)

    def decr(self, key, delta=1, namespace=None, initial_value=None):
        """ Atomically decrements a key's value. Internally, the value is a unsigned 64-bit integer. 
        dscache doesn't check 64-bit overflows. The value, if too large, will wrap around.
        If the key does not yet exist in the cache and you specify an initial_value, 
        the key's value will be set to this initial value and then decremented. 
        If the key does not exist and no initial_value is specified, the key's value will not be set. 

        The return value is a new long integer value, or None if key was not in the cache or could not be 
        decremented for any other reason.
        """
        return decr(key, delta=delta, namespace=namespace, initial_value=initial_value)

    def offset_multi(self, mapping, key_prefix='', namespace=None, initial_value=None):
        """ Increments or decrements multiple keys with integer values in a single service call. 
        Each key can have a separate offset. The offset can be positive or negative.
        Applying an offset to a single key is atomic. Applying an offset to multiple keys may 
        succeed for some keys and fail for others. 

        The return value is a mapping of the provided keys to their new values. 
        If there was an error applying an offset to a key, if a key doesn't exist in the cache and 
        no initial_value is provided, or if a key is set with a non-integer value, its return value is None.
        """
        return offset_multi(mapping, key_prefix=key_prefix, namespace=namespace, initial_value=initial_value)

    def flush_all(self):
        """ Deletes everything in dscache.

        The return value is True on success, False on RPC or server error."""
        return flush_all()

    def get_stats(self):
        """ Gets dscache statistics for this application. All of these statistics may 
        reset due to various transient conditions. They provide the best information 
        available at the time of being called.

        The return value is a dictionary mapping statistic names to associated values. """
        return get_stats()
