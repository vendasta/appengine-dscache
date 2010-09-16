""" Tests for vendasta.appengine.api.dscache """

import os
import time
import unittest
import datetime
import google.appengine.api.apiproxy_stub_map as apiproxy_stub_map
import google.appengine.api.datastore_file_stub as datastore_file_stub
from dscache import dscache
from dscache.models import _DSCache
from dscache.vacuum import Vacuum, BATCH_DELETE_SIZE

class DatastoreTests(unittest.TestCase):

    def setUp(self):
        "Sets up the unit test environment."
        APP_ID = 'dev-test'
        os.environ['APPLICATION_ID'] = APP_ID
        apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
        self.__datastore = datastore_file_stub.DatastoreFileStub(APP_ID, '/dev/null', '/dev/null')
        apiproxy_stub_map.apiproxy.RegisterStub('datastore_v3', self.__datastore)

class Obj(object):
    def __init__(self, a=None, b=None):
        self.a = a
        self.b = b
        
class VacuumTests(DatastoreTests):
    
    def setUp(self):
        super(VacuumTests, self).setUp()
        self.tomorrow = datetime.datetime.utcnow() + datetime.timedelta(days=1)
        self.yesterday = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        self.vacuum = Vacuum()
    
    def test_nothing_to_delete(self):
        _DSCache(timeout=self.tomorrow).put()
        
        self.vacuum.get()
        
        keys = _DSCache.all(keys_only=True).fetch(2)
        self.assertEquals(1, len(keys))
        
    def test_only_old_values_deleted(self):
        _DSCache(timeout=self.tomorrow).put()
        _DSCache(timeout=self.yesterday).put()
        
        self.vacuum.get()
        
        keys = _DSCache.all(keys_only=True).fetch(2)
        self.assertEquals(1, len(keys))
        
    def test_lots_to_delete(self):
        for i in range(0, 2*BATCH_DELETE_SIZE+1):
            _DSCache(timeout=self.yesterday).put()
            
        self.vacuum.get()

        keys = _DSCache.all(keys_only=True).fetch(1000)
        self.assertEquals(0, len(keys))

class SetTests(DatastoreTests):
    
    def test_int_set(self):
        value = 1
        dscache.set('key', value)
        self.assertEquals(value, dscache.get('key'))
        
    def test_float_set(self):
        value = 1.23
        dscache.set('key', value)
        self.assertEquals(value, dscache.get('key'))
        
    def test_date_set(self):
        value = datetime.date(2010, 2, 12)
        dscache.set('key', value)
        self.assertEquals(value, dscache.get('key'))
        
    def test_time_set(self):
        value = datetime.time(14, 22, 23)
        dscache.set('key', value)
        self.assertEquals(value, dscache.get('key'))
        
    def test_datetime_set(self):
        value = datetime.datetime(2010, 2, 12, 14, 22, 23)
        dscache.set('key', value)
        self.assertEquals(value, dscache.get('key'))
        
    def test_bool_set(self):
        value = True
        dscache.set('key', value)
        self.assertEquals(value, dscache.get('key'))
        
    def test_str_set(self):
        value = 'abc'
        dscache.set('key', value)
        self.assertEquals(value, dscache.get('key'))
        
    def test_unicode_set(self):
        value = u'abc'
        dscache.set('key', value)
        self.assertEquals(value, dscache.get('key'))
        
    def test_empty_string_set(self):
        value = ''
        dscache.set('key', value)
        self.assertEquals(value, dscache.get('key'))
        
    def test_long_string_set(self):
        value = '*'*1024
        dscache.set('key', value)
        self.assertEquals(value, dscache.get('key'))
        
    def test_json_set(self):
        value = { 'a': 1, 'b': [ False, 1.23 ]}
        dscache.set('key', value)
        self.assertEquals(value, dscache.get('key'))
        
    def test_object_set(self):
        value = Obj(a=1, b='b')
        dscache.set('key', value)
        lookup = dscache.get('key')
        self.assertEquals(1, lookup.a)
        self.assertEquals('b', lookup.b)
        
    def test_set_namespace_partitioning(self):
        dscache.set('key', 'value1', namespace='1')
        dscache.set('key', 'value2', namespace='2')
        value1 = dscache.get('key', namespace='1')
        value2 = dscache.get('key', namespace='2')
        self.assertNotEquals(value1, value2)
        value3 = dscache.get('key')
        self.assertEquals(None, value3)
        
    def test_set_timeout(self):
        dscache.set('key', 'value', time=-1)
        #time.sleep(2)  # wait for timeout
        value = dscache.get('key')
        self.assertEquals(None, value)
        
    def test_long_key_set(self):
        key = 'abc'*1000
        dscache.set(key, 'value')
        value = dscache.get(key)
        self.assertEquals('value', value)
        
    def test_int_set_Client(self):
        value = 1
        client = dscache.Client()
        client.set('key', value)
        self.assertEquals(value, client.get('key'))
        
    def test_numeric_key_sets(self):
        dscache.set('1', True)
        self.assertNotEquals(None, dscache.get('1'))

class GetTests(DatastoreTests):
    
    def test_unknown_key_returns_none(self):
        value = dscache.get('unknown')
        self.assertEquals(None, value)

    def test_expired_item_returns_none(self):
        dscache.set('key', 'value', time=-1)
        #time.sleep(2)  # wait for timeout
        value = dscache.get('key')
        self.assertEquals(None, value)

    def test_unknown_key_returns_none_Client(self):
        client = dscache.Client()
        value = client.get('unknown')
        self.assertEquals(None, value)

class SetMultiTests(DatastoreTests):
    
    def test_empty_mapping_returns_empty_list(self):
        result = dscache.set_multi({})
        self.assertEquals([], result)
        
    def test_single_value_set(self):
        result = dscache.set_multi({'a': 1})
        self.assertEquals([], result)
        self.assertEquals(1, dscache.get('a'))
        
    def test_multiple_values_set(self):
        result = dscache.set_multi({'a': 1, 'b': '2'})
        self.assertEquals([], result)
        self.assertEquals(1, dscache.get('a'))
        self.assertEquals('2', dscache.get('b'))
        
    def test_key_prefix_injected(self):
        dscache.set_multi({'a': 1}, key_prefix='xxx')
        self.assertEquals(None, dscache.get('a'))
        
    def test_timeout_applied_to_sets(self):
        dscache.set_multi({'a': 1, 'b': 2}, time=-1)
        #time.sleep(2)
        self.assertEquals(None, dscache.get('a'))
        self.assertEquals(None, dscache.get('b'))
        
    def test_namespace_partitions_set_multi(self):
        dscache.set_multi({'a': 1, 'b': 2}, namespace='1')
        dscache.set_multi({'a': 3, 'b': 4}, namespace='2')
        self.assertEquals(1, dscache.get('a', namespace='1'))
        self.assertEquals(2, dscache.get('b', namespace='1'))
        self.assertEquals(3, dscache.get('a', namespace='2'))
        self.assertEquals(4, dscache.get('b', namespace='2'))
        
    def test_multiple_values_set_Client(self):
        client = dscache.Client()
        result = client.set_multi({'a': 1, 'b': '2'})
        self.assertEquals([], result)
        self.assertEquals(1, client.get('a'))
        self.assertEquals('2', client.get('b'))
        
    def _create_entity_dict(self, size):
        """ Create a dictionary to set of size size. """
        d = {}
        for i in range(0, size):
            d[str(i)] = [str(i)]
        return d
        
    def test_more_than_500_entities_set_correctly(self):
        """ Ensure that more than 500 entities are set correctly. 500 is an App Engine limit for put(). """
        entities = self._create_entity_dict(501)
        result = dscache.set_multi(entities)
        self.assertEquals(0, len(result))
        value = dscache.get('500')
        self.assertEquals(['500'], value)
        
    def test_more_than_1000_entities_set_correctly(self):
        """ Ensure that more than 1000 entities are set correctly. """
        entities = self._create_entity_dict(1001)
        result = dscache.set_multi(entities)
        self.assertEquals(0, len(result))
        value = dscache.get('1000')
        self.assertEquals(['1000'], value)

class GetMultiTests(DatastoreTests):
    
    def test_single_result_returned(self):
        dscache.set('a', 1)
        result = dscache.get_multi(['a'])
        self.assertEquals({'a': 1}, result)
        
    def test_multiple_results_returned(self):
        dscache.set('a', 1)
        dscache.set('b', 2)
        result = dscache.get_multi(['a', 'b'])
        self.assertEquals({'a': 1, 'b': 2}, result)
        
    def test_partial_results_returned(self):
        dscache.set('a', 1)
        result = dscache.get_multi(['a', 'x'])
        self.assertEquals({'a': 1}, result)
        
    def test_full_miss_returned(self):
        result = dscache.get_multi(['x', 'y'])
        self.assertEquals({}, result)
        
    def test_key_prefix_get(self):
        dscache.set_multi({'a': 1, 'b': 2}, key_prefix='xxx')
        result = dscache.get_multi(['a', 'b'], key_prefix='xxx')
        self.assertEquals({'a': 1, 'b': 2}, result)
        not_in_cache = dscache.get('a') # no key_prefix
        self.assertEquals(None, not_in_cache)
        
    def test_namespace_partitioned(self):
        dscache.set('a', 1, namespace='1')
        dscache.set('b', 2, namespace='2')
        result = dscache.get_multi(['a', 'b'], namespace='1')
        self.assertEquals({'a': 1}, result)
        
    def test_expired_entry_not_returned(self):
        dscache.set('a', 1, time=-1)
        result = dscache.get_multi(['a'])
        self.assertEquals({}, result)
    
    def test_multiple_results_returned_Client(self):
        client = dscache.Client()
        client.set('a', 1)
        client.set('b', 2)
        result = client.get_multi(['a', 'b'])
        self.assertEquals({'a': 1, 'b': 2}, result)

class DeleteTests(DatastoreTests):
    
    def test_item_deleted(self):
        dscache.set('a', 1)
        ret_val = dscache.delete('a')
        self.assertEquals(True, ret_val)
        self.assertEquals(None, dscache.get('a'))
        
    def test_delete_unknown_item_returns_true(self):
        ret_val = dscache.delete('a')
        self.assertEquals(True, ret_val)
        
    def test_expired_item_returns_true(self):
        dscache.set('a', 1, time=-1)
        ret_val = dscache.delete('a')
        self.assertEquals(True, ret_val)
        
    def test_item_deleted_Client(self):
        client = dscache.Client()
        client.set('a', 1)
        ret_val = client.delete('a')
        self.assertEquals(True, ret_val)
        self.assertEquals(None, client.get('a'))

class DeleteMultiTests(DatastoreTests):
    
    def test_single_item_deleted(self):
        dscache.set('a', 1)
        ret_val = dscache.delete_multi(['a'])
        self.assertEquals(True, ret_val)
        self.assertEquals(None, dscache.get('a'))
        
    def test_multiple_items_deleted(self):
        dscache.set('a', 1)
        dscache.set('b', 1)
        ret_val = dscache.delete_multi(['a', 'b'])
        self.assertEquals(True, ret_val)
        self.assertEquals(None, dscache.get('a'))
        self.assertEquals(None, dscache.get('b'))
        
    def test_partial_items_deleted(self):
        dscache.set('a', 1)
        ret_val = dscache.delete_multi(['a', 'b'])
        self.assertEquals(True, ret_val)
        self.assertEquals(None, dscache.get('a'))
        
    def test_unknown_items_deleted_returns_true(self):
        ret_val = dscache.delete_multi(['x'])
        self.assertEquals(True, ret_val)
        
    def test_key_prefix_delete(self):
        dscache.set_multi({'a': 1}, key_prefix='x')
        ret_val = dscache.delete_multi(['a'], key_prefix='x')
        self.assertEquals(True, ret_val)
        self.assertEquals({}, dscache.get_multi(['a'], key_prefix='x'))
        self.assertEquals(None, dscache.get('a'))
        
    def test_namespace_partitions(self):
        dscache.set_multi({'a': 1, 'b': 2}, namespace='1')
        dscache.set_multi({'c': 3, 'd': 4}, namespace='2')
        ret_val = dscache.delete_multi(['c', 'd'], namespace='1') # yes, this is '1'
        self.assertEquals(True, ret_val)
        self.assertEquals({'c': 3, 'd': 4}, dscache.get_multi(['c', 'd'], namespace='2'))
    
    def test_multiple_items_deleted_Client(self):
        client = dscache.Client()
        client.set('a', 1)
        client.set('b', 1)
        ret_val = client.delete_multi(['a', 'b'])
        self.assertEquals(True, ret_val)
        self.assertEquals(None, client.get('a'))
        self.assertEquals(None, client.get('b'))

class AddTests(DatastoreTests):
    
    def test_new_item_added(self):
        ret_val = dscache.add('a', 1)
        self.assertEquals(True, ret_val)
        self.assertEquals(1, dscache.get('a'))
        
    def test_existing_item_not_overwritten(self):
        dscache.set('a', 1)
        ret_val = dscache.add('a', 2)
        self.assertEquals(False, ret_val)
        self.assertEquals(1, dscache.get('a')) # yes, this is 1
        
    def test_add_item_expires(self):
        ret_val = dscache.add('a', 1, time=-1)
        self.assertEquals(True, ret_val)
        self.assertEquals(None, dscache.get('a'))
        
    def test_namespace_partitions(self):
        dscache.add('a', 1, namespace='1')
        ret_val = dscache.add('a', 2, namespace='2')
        self.assertEquals(True, ret_val) # different namespace, item should be added
        self.assertEquals(1, dscache.get('a', namespace='1'))
        self.assertEquals(2, dscache.get('a', namespace='2'))
        self.assertEquals(None, dscache.get('a'))
    
    def test_new_item_added_Client(self):
        client = dscache.Client()
        ret_val = client.add('a', 1)
        self.assertEquals(True, ret_val)
        self.assertEquals(1, client.get('a'))

    def test_existing_item_not_overwritten_Client(self):
        client = dscache.Client()
        client.set('a', 1)
        ret_val = client.add('a', 2)
        self.assertEquals(False, ret_val)
        self.assertEquals(1, client.get('a')) # yes, this is 1

class AddMultiTests(DatastoreTests):
    pass
    
class ReplaceTests(DatastoreTests):
    pass
    
class ReplaceMultiTests(DatastoreTests):
    pass
    
class IncrTests(DatastoreTests):
    pass
    
class DecrTests(DatastoreTests):
    pass
    
class OffsetMultiTests(DatastoreTests):
    pass
    
class FlushAllTests(DatastoreTests):
    pass
    
class StatsTests(DatastoreTests):
    pass
