"""
netvendtk - A Python API for netvend written by @BardiHarborow and Syriven.

This module centers around the Agent class, which is split into three parts:
* AgentCore (signing and sending commands)
* AgentBasic (formatting commands and parsing the server responses)
* AgentExtended (additional convenience methods).

AgentCore should be the most stable. Each extension adds usability, but may be
less stable.

Agent is an alias for the current class we consider stable (AgentExtended, at
the moment).

Created by:
@Syriven (1MphZghyHmmrzJUk316iHvZ55UthfHXR34) and;
@BardiHarborow (1Bardi4eoUvJomBEtVoxPcP8VK26E3Ayxn)

Special Thanks to /u/minisat_maker on reddit for the orginal concept for netvend.
"""

import sys
import thread
import math
import time
import pickle
import json
import pybitcointools

if sys.hexversion < 0x02000000 or sys.hexversion >= 0x03000000:
    raise RuntimeError("netvend requires Python 2.x.")

try:
    import urllib
    import urllib2
    urlopen = urllib2.urlopen
    urlencode = urllib.urlencode
except ImportError:
    import urllib.request
    urlopen = urllib.request.urlopen
    import urllib.parse
    urlencode = urllib.parse.urlencode

NETVEND_URL = "http://ec2-54-68-165-84.us-west-2.compute.amazonaws.com/command.php"
NETVEND_VERSION = "1_0"

PRIVTYPE_HEX = 0
PRIVTYPE_B58CHECK = 1
PRIVTYPE_SEED = 2

BATCHTYPE_POST = 0
BATCHTYPE_PULSE = 1
BATCHTYPE_QUERY = 2
BATCHTYPE_WITHDRAW = 3

DEFAULT_QUERY_MAX_TIME_COST = 1000
DEFAULT_QUERY_MAX_SIZE_COST = 100000

LASTREAD_PREFIX = "l:"
RETURN_PREFIX = "r:"
CALL_PREFIX = "c:"

UNIT_POWERS = {"usat": 0, "msat": 3, "sat": 6,
               "ubtc": 8, "mbtc": 11, "btc": 14,
               "ubit": 8, "mbit": 11, "bit": 14,
               "base": 0}


def unit_pow(unit):
    """Return the unit in usats as a power of 10.

    :param unit: commonly used bitcoin unit abbreviation (see UNIT_POWERS)
    :return: unit in usats as a power of 10
    """
    unit = unit.lower()
    if unit in UNIT_POWERS.keys():
        return UNIT_POWERS[unit]
    else:
        raise ValueError("cannot recognize unit {}".format(unit))


def convert_value(amount, from_unit, to_unit):
    """Converts amount between units.

    :param amount: amount of from_unit
    :param from_unit: unit converted from
    :param to_unit: unit converted to
    :return: converted amount as float or int if is_integer()
    """
    from_pow = unit_pow(from_unit)
    to_pow = unit_pow(to_unit)
    delta_pow = from_pow - to_pow
    result = amount * math.pow(10, delta_pow)
    if result.is_integer():
        return int(result)
    return result


def format_value(usats, round_decimals=3, return_list=False):
    """Formats an amount of usats to a readable format.

    :param usats: amount in usats, use convert_value to get from other units
    :param round_decimals: amount of decimal places to round to, False to not round
    :param return_list: if True a tuple of amount and unit will be returned instead
    :return: by default formatted str like "amount unit", see return_list
    """
    if usats > math.pow(10, 13):
        unit = 'BTC'
    elif usats > math.pow(10, 10):
        unit = 'mBTC'
    elif usats > math.pow(10, 7):
        unit = 'uBTC'
    elif usats > math.pow(10, 5):
        unit = 'sat'
    elif usats > math.pow(10, 2):
        unit = 'msat'
    else:
        unit = 'usat'

    amount = convert_value(usats, 'usat', unit)

    if round_decimals is not False:
        amount = round(amount, round_decimals)
    if return_list:
        return amount, unit
    else:
        return "{} {}".format(amount, unit)


def convert_json_unicode_to_str(input):
    """Converts a python object(returned from json.loads) and all of its children to UTF-8 encoded strings.

    :param input: Python object returned from json.loads
    :return: Same as input with every unicode replaced with UTF-8 encoded str
    """
    if isinstance(input, dict):
        return dict([(convert_json_unicode_to_str(key), convert_json_unicode_to_str(value)) for key, value in input.iteritems()])
    elif isinstance(input, list):
        return [convert_json_unicode_to_str(element) for element in input]
    elif isinstance(input, unicode):
        return input.encode('utf-8')
    else:
        return input
        
        
class NetvendResponseError(BaseException):
    def __init__(self, batch, error_info):  # message, batch, pos_in_batch, already_charged):
        self.batch = batch
        self.message = error_info[1]
        self.pos_in_batch = error_info[2]
        self.already_charged = error_info[3]

    def __str__(self):
        to_return = "Error in batch " + str(self.batch)
        if self.pos_in_batch is not None:
            to_return += ", command " + str(self.pos_in_batch)
        if self.already_charged is not None:
            to_return += " (already charged " + str(self.already_charged) + " for batch)"
        to_return += ": " + self.message
        return to_return


class BatchResult(object):
    def __init__(self, history_id, charged, size):
        self.history_id = history_id
        self.charged = charged
        self.size = size


class PostBatchResult(BatchResult):
    def __init__(self, response, size):
        self.first_post_id = response[0]
        
        BatchResult.__init__(self, response[1], response[2], size)
    
    def __getitem__(self, index):
        if index > self.size:
            raise IndexError("post index out of batch range")
        return self.first_post_id + index

        
class PulseBatchResult(BatchResult):
    def __init__(self, response, size):
        self.first_pulse_id = response[0]
        
        BatchResult.__init__(self, response[1], response[2], size)
    
    def __getitem__(self, index):
        if index > self.size:
            raise IndexError("pulse index out of batch range")
        return self.first_pulse_id + index
        

class QueryResult(object):
    def __init__(self, result, raise_on_truncate):
        self.rows = result[0]
        self.time_cost = result[1]
        self.size_cost = result[2]
        self.truncated = bool(result[3])
        if raise_on_truncate and self.truncated:
            raise RuntimeError("query result has been truncated; rows are missing.")


class QueryBatchResult(BatchResult):
    def __init__(self, response, size, raise_on_truncate):
        results = response[0]
        self.results = []
        for result in results:
            self.results.append(QueryResult(result, raise_on_truncate))
            
        BatchResult.__init__(self, response[1], response[2], size)
    
    def __getitem__(self, index):
        if index > self.size:
            raise IndexError("query index out of batch range")
        return self.results[index]

        
class WithdrawBatchResult(BatchResult):
    def __init__(self, response, size):
        BatchResult.__init__(self, response[1], response[2], size)
        
        
class BatchResultList(object):
    def __init__(self, responses, batch_types, batch_sizes, raise_on_truncate):
        # pprint.pprint(responses)
        self.results = []
        
        for i in range(len(responses)):
            if batch_types[i] is BATCHTYPE_POST:
                self.results.append(PostBatchResult(responses[i][1], batch_sizes[i]))
            elif batch_types[i] is BATCHTYPE_PULSE:
                self.results.append(PulseBatchResult(responses[i][1], batch_sizes[i]))
            elif batch_types[i] is BATCHTYPE_QUERY:
                self.results.append(QueryBatchResult(responses[i][1], batch_sizes[i], raise_on_truncate))
            elif batch_types[i] is BATCHTYPE_WITHDRAW:
                self.results.append(WithdrawBatchResult(responses[i][1], batch_sizes[i]))
            else:
                raise RuntimeError("batch_types contains an invalid value: " + str(batch_types[i]))
    
    def __getitem__(self, index):
        return self.results[index]


class AgentCore(object):
    """Base class providing a skeleton framework. This should be stable.

    :param private: private key
    :param url: url of the netvend server
    :param privtype: private key format, see PRIVTYPE_*
    """
    def __init__(self, private, url, privtype):
        if privtype is PRIVTYPE_SEED:
            self.private = pybitcointools.sha256(private)
        elif privtype is PRIVTYPE_B58CHECK:
            try:
                self.private = pybitcointools.b58check_to_hex(private)
            except AssertionError:
                raise ValueError("Invalid private key")
        elif privtype is PRIVTYPE_HEX:
            if len(private) == 64:
                self.private = private
            else:
                raise ValueError("Invalid private key")
        else:
            # Raise a ValueError, otherwise self.private would not be defined
            raise ValueError("Invalid privtype")

        self.address = pybitcointools.pubkey_to_address(pybitcointools.privtopub(self.private))
        self.url = url

    def get_address(self):
        return self.address

    def sign_data(self, data):
        return pybitcointools.ecdsa_sign(data, self.private)

    def send_to_netvend(self, arg_dict):
        new_arg_dict = dict({'version': NETVEND_VERSION}, **arg_dict)
        return urlopen(self.url, urlencode(new_arg_dict)).read()


class AgentBasic(AgentCore):
    """Class providing increased functionality to AgentCore.

    Adds functions for all command types and a function to make server output nicer.
    This should be stable.
    """
    def __init__(self, private, url=NETVEND_URL, privtype=PRIVTYPE_SEED):
        AgentCore.__init__(self, private, url, privtype)
        self.batches = []
        self.batch_types = []
        self.log_path = None
        self.raise_on_query_truncate = True

    def post_process(self, data, batch_types, batch_sizes):
        try:
            responses = json.loads(data)
        except ValueError:
            raise ValueError("Can't parse server response. Server responded with:\n" + data)
                   
        if not responses[-1][0]:
            if self.log_path is not None:
                with open(self.log_path + self.get_address() + "_" + str(time.time()), "a") as f:
                    pickle.dump(responses, f)
            raise NetvendResponseError(len(responses)-1, responses[-1])
        
        return BatchResultList(responses, batch_types, batch_sizes, raise_on_truncate=self.raise_on_query_truncate)
    
    def set_log_path(self, log_path):
        self.log_path = log_path
    
    def clear_batches(self):
        self.batches = []
        self.batch_types = []
    
    def add_batch(self, batch):
        encoded_batch = json.dumps(batch)
        
        sig = self.sign_data(encoded_batch)
        signed_batch = [encoded_batch, sig]
        
        self.batches.append(signed_batch)
        self.batch_types.append(batch[0])
        
        return len(self.batches) - 1
    
    def add_post_batch(self, posts):
        if type(posts) is not list:
            raise TypeError("argument must be list")
        for post in posts:
            if type(post) is not str:
                raise TypeError("expected list of strings for posts")
        
        return self.add_batch([BATCHTYPE_POST, posts])
    
    def add_pulse_batch(self, pulses):
        if type(pulses) is not list:
            raise TypeError("argument must be list")
        for i in range(len(pulses)):
            if len(pulses[i]) < 2:
                raise TypeError("pulse must specify recipient and amount")
            
            if type(pulses[i][0]) is not str:
                raise TypeError("pulses must have a string as a first argument")
            if type(pulses[i][1]) is not int or pulses[i][1] < 0:
                raise TypeError("pulses must have int >= 0 as a second argument")
            
            if len(pulses[i]) > 2:
                if pulses[i][2] is None:
                    pulses[i][2] = 0
                elif type(pulses[i][2]) is not int or pulses[i][2] < 0:
                    raise TypeError("pulses must have int > 0 or None as a third argument")
            
            if len(pulses[i]) > 3 and (type(pulses[i][3]) is not int or pulses[i][3] < 0):
                raise TypeError("fourth argument of pulse must be int > 0")
        
        return self.add_batch([BATCHTYPE_PULSE, pulses])
    
    def add_query_batch(self, queries):
        if type(queries) is not list:
            raise TypeError("argument must be list")
        for i in range(len(queries)):
            if type(queries[i]) is str:
                queries[i] = [queries[i], DEFAULT_QUERY_MAX_TIME_COST, DEFAULT_QUERY_MAX_SIZE_COST]
            elif type(queries[i]) is not list or type(queries[i][0]) is not str or type(queries[i][1]) is not int or type(queries[i][2]) is not int:
                raise TypeError("query must be either [string, int, int], or string.")
        
        return self.add_batch([BATCHTYPE_QUERY, queries])
    
    def add_withdraw_batch(self, withdraws):
        if type(withdraws) is not list:
            raise TypeError("argument must be list")
        for withdraw in withdraws:
            if type(withdraw) is not list or len(withdraw) < 2 or type(withdraw[0]) is not int or type(withdraw[1]) is not str:
                raise TypeError("withdraw must be list of [int, string]")
        
        return self.add_batch([BATCHTYPE_WITHDRAW, withdraws])
    
    def transmit_batches_blocking(self):
        batches = self.batches
        batch_types = self.batch_types
        batch_sizes = [len(x) for x in batches]
        self.batches = []
        self.batch_types = []
        
        return self.post_process(self.send_to_netvend({"batches": json.dumps(batches)}), batch_types, batch_sizes)
    
    def transmit_batches_callback(self, callback):
        if not callable(callback):
            raise TypeError("can't use type " + type(callback) + " as a callback")
        
        result_list = self.transmit_batches_blocking()
        callback(result_list)
    
    def transmit_batches(self, callback=None):
        if callback is None:
            return self.transmit_batches_blocking()
        else:
            return thread.start_new_thread(self.transmit_batches_callback, (callback,))
    
    def transmit_single_batch_blocking(self, batch_type, signed_batch, batch_size):
        result_list = self.post_process(self.send_to_netvend({"batches": json.dumps([signed_batch])}), [batch_type], [batch_size])
        batch_result = result_list[0]
        return batch_result
    
    def transmit_single_batch_callback(self, batch_type, signed_batch, batch_size, callback):
        if not callable(callback):
            raise TypeError("can't use type " + type(callback) + " as a callback")
        
        batch_result = self.transmit_single_batch_blocking(batch_type, signed_batch, batch_size)
        callback(batch_result)
    
    def transmit_single_batch(self, batch_type, signed_batch, batch_size, callback=None):
        if callback is None:
            return self.transmit_single_batch_blocking(batch_type, signed_batch, batch_size)
        else:
            return thread.start_new_thread(self.transmit_single_batch_callback, (batch_type, signed_batch, batch_size, callback))
    
    def sign_and_transmit_single_command_blocking(self, type, command):
        batch = [type, [command]]
        encoded_batch = json.dumps(batch)
        
        sig = self.sign_data(encoded_batch)
        signed_batch = [encoded_batch, sig]
        
        batch_result = self.transmit_single_batch_blocking(type, signed_batch, 1)
        
        return batch_result[0]
    
    def sign_and_transmit_single_command_callback(self, type, command, callback):
        if not callable(callback):
            raise TypeError("can't use type " + type(callback) + " as a callback")
        
        result = self.sign_and_transmit_single_command_blocking(type, command)
        callback(result)
    
    def sign_and_transmit_single_command(self, type, command, callback=None):
        if callback is None:
            return self.sign_and_transmit_single_command_blocking(type, command)
        else:
            return thread.start_new_thread(self.sign_and_transmit_single_command_callback(type, command, callback))
    
    def post(self, post, callback=None):
        return self.sign_and_transmit_single_command(BATCHTYPE_POST, post, callback)
    
    def pulse(self, address, amount, post_id=None, post_id_from_batch=None, callback=None):
        if post_id is None:
            pulse = [address, amount]
        elif post_id_from_batch is None:
            pulse = [address, amount, post_id]
        else:
            pulse = [address, amount, post_id, post_id_from_batch]
        
        return self.sign_and_transmit_single_command(BATCHTYPE_PULSE, pulse, callback)
    
    def query(self, query, max_time_cost=None, max_size_cost=None, callback=None):
        if max_time_cost is None:
            max_time_cost = DEFAULT_QUERY_MAX_TIME_COST
        if max_size_cost is None:
            max_size_cost = DEFAULT_QUERY_MAX_SIZE_COST
        
        return self.sign_and_transmit_single_command(BATCHTYPE_QUERY, [query, max_time_cost, max_size_cost], callback)
    
    def withdraw(self, amount, address=None, callback=None):
        if address is None:
            withdraw = [amount]
        else:
            withdraw = [amount, address]
        
        return self.sign_and_transmit_single_command(BATCHTYPE_WITHDRAW, withdraw, callback)


class AgentExtended(AgentBasic):
    """NetVendCore - Less stable functionality. Experimental, may change at any time."""
    def fetch_balance(self):
        query = "SELECT balance FROM accounts WHERE address = '" + self.get_address() + "'"
        response = self.query(query)
        balance = int(response.rows[0][0])
        
        balance -= response.time_cost + response.size_cost
        return balance
    
    # def fetch_pulsenet(self, pulse_id_list):
    #     query = "SELECT * FROM pulses LEFT JOIN posts ON pulses.post_id = posts.post_id WHERE pulses.pulse_id IN (" + str(pulse_id_list)[1:-1] + ") ORDER BY tips.value DESC"
    #     response = self.query(query)
    #     return Pulsenet(response.rows)
    

Agent = AgentExtended


class Service(object):
    """Service class used and usually created by ServiceAgent

    :param func: function to be called
    :param fee: fee of service
    :param advanced: if False: func(*args), else: func(request_info_dict, args), see call
    """
    def __init__(self, func, fee, advanced=False):
        self.func = func
        self.fee = fee
        self.is_advanced = advanced

    def call(self, args, request_info_dict):
        if self.is_advanced:
            return self.func(request_info_dict, args)
        else:
            return self.func(*args)


class ServiceAgent(Agent):
    """Agent used to call and serve services."""
    def __init__(self, private, url=NETVEND_URL, privtype=PRIVTYPE_SEED):
        super(ServiceAgent, self).__init__(self, private, url, privtype)
        self.services = {}
        self.lowest_fee = None
        self.refund_fee = 0
        self.raise_error_local = False
    
    def set_refund_fee(self, refund_fee):
        self.refund_fee = refund_fee

    def register_service(self, name, func, fee, is_advanced=False):
        self.services[name] = Service(func, fee, is_advanced)
        if self.lowest_fee is None or fee < self.lowest_fee:
            self.lowest_fee = fee

    def work(self, max_time_cost=None, max_size_cost=None):
        if not self.services:  # len(self.services) == 0
            raise RuntimeError("Need to register services before ServiceAgent can work")
        
        # Clear any existing batches
        self.clear_batches()
    
        # We need an inner query that fetches the tip_id our agent has served last (we will update this in a post later)
        # The SQL SUBSTRING method considers the first character position 1 (not 0), so we have to have len(lastread_prefix)+1
        inner_query = "SELECT SUBSTRING(data, " + str(len(LASTREAD_PREFIX)+1) + ", LENGTH(data)) " \
                      "FROM posts WHERE address = '" + self.get_address() + "' " \
                      "AND data LIKE '" + LASTREAD_PREFIX + "%' " \
                      "ORDER BY post_id DESC LIMIT 1"

        # The outer query will fetch all info about any calls, checking all posts more recent than the data_id the inner query fetches
        query = "SELECT " \
                "pulses.pulse_id, " \
                "pulses.from_address, " \
                "pulses.value, " \
                "pulses.post_id, " \
                "posts.data " \
                "FROM pulses LEFT JOIN posts " \
                "ON pulses.post_id = posts.post_id " \
                "WHERE " \
                "pulses.to_address = '" + self.get_address() + "' " \
                "AND pulses.pulse_id > IFNULL((" + inner_query + "), 0) " \
                "AND pulses.value >= " + str(self.lowest_fee) + " " \
                "AND posts.data LIKE '" + CALL_PREFIX + "%'" \
                "ORDER BY pulses.pulse_id ASC"
                

        result = self.query(query)
        if result.truncated:
            # Not meant to be a permanent solution
            # TODO: Implement proper solution
            raise RuntimeError("query truncated; max_size_cost too low.")

        rows = result.rows
        service_results = []
        refund_pulses = []
        for row in rows:
            [pulse_id, pulse_from_address, pulse_value, post_id, data] = row
            pulse_id = int(pulse_id)
            pulse_from_address = str(pulse_from_address)
            pulse_value = int(pulse_value)
            post_id = int(post_id)
            data = str(data)
            try:
                # Get the name and args of the function, as packed by the call method
                [name, args] = convert_json_unicode_to_str(json.loads(data[len(CALL_PREFIX):]))

                # Call the service's function
                if name in self.services:
                    if self.services[name].is_advanced:
                        request_info_dict = {'pulse_id': pulse_id,
                                             'pulse_from_address': pulse_from_address,
                                             'pulse_value': pulse_value,
                                             'post_id': post_id}
                        returned = self.services[name].call(args, request_info_dict)
                    else:
                        returned = self.services[name].call(args)
                else:
                    continue  # Name not registered as a service, skip

                # We only want to post if the function actually returns a value
                if returned is not None:
                    return_str = RETURN_PREFIX + str(post_id) + ":" + json.dumps(returned)
                    service_results.append(return_str)
            
            except Exception as e:
                if self.raise_error_local:
                    raise
                    
                # If there's an error, respond with an error response and send a refund
                return_str = RETURN_PREFIX + str(post_id) + ":e:" + str(e)
                service_results.append(return_str)

                refund = int(pulse_value) - self.refund_fee
                # If the refund fee (which should cover netvend fees and processing costs) is too much, don't refund
                if refund > 0:
                    # Tip will refer to the nth post_id in what will be batch 0, our post batch, where n is the position of the error post in the post batch.
                    refund_pulses.append([pulse_from_address, refund, len(service_results)-1, 0])
        
        if len(rows) > 0:
            # Get the pulse id of the last row checked
            last_pulse_id = rows[-1][0]
            # Post all of our responses, and post our lastread placeholder
            post_batch_iter = self.add_post_batch(service_results + [LASTREAD_PREFIX + str(last_pulse_id)])
            # If we have any refund pulses, add those in a batch as well
            if len(refund_pulses) > 0:
                pulse_batch_iter = self.add_pulse_batch(refund_pulses)
            else:
                pulse_batch_iter = None

            # Transmit batches, return info
            responses = self.transmit_batches()
            post_batch_response = responses[post_batch_iter]
            
            if pulse_batch_iter is None:
                pulse_batch_response = None
            else:
                pulse_batch_response = responses[pulse_batch_iter]
            
            return [post_batch_response, pulse_batch_response]
        
        else:
            return [None, None]
    
    def call(self, service_address, service_name, args, value, timeout=None, wait_for_response=True, convert_unicode_to_str=True):
        if type(args) is not list and type(args) is not dict:
            raise TypeError("args must be a list")
        # Clear any existing batches
        self.clear_batches()
        
        # First, make a post to call the service
        call_str = CALL_PREFIX + json.dumps([service_name, args])
        post_batch_iter = self.add_post_batch([call_str])

        # Then use a pulse to alert service_address of our call post
        pulse_batch_iter = self.add_pulse_batch([[service_address, value, 0, post_batch_iter]])

        # Send the query, post, and tip batches
        response_list = self.transmit_batches()
        
        if not wait_for_response:
            #return information about pulse and post results instead
            return response_list
        
        # Get the post_id of our request, so later we can query netvend for responses--posts that reference this post_id
        post_id = response_list[post_batch_iter][0]
        # Also use the post_id as our initial value for last_post_checked_id, which is needed to check each time for *new* posts
        last_checked_post_id = post_id

        start_time = time.time()
        
        # We have to get two values from netvend:
        # New responses to our request from the service address,
        # and the new max post_id, to know where to start looking for "new" posts next iteration
        
        # Last_post_checked_query won't change, so we can define it now:
        last_post_id_query = "SELECT MAX(post_id) FROM posts"
        
        while True:
            # The query that requests new posts changes where it searches from (last_post_checked_id),
            # so we'll define it each loop
            
            response_check_query = "SELECT data FROM posts WHERE post_id > " + str(last_checked_post_id) + " AND address = '" + service_address + "' AND data LIKE '" + RETURN_PREFIX + str(post_id) + ":%' LIMIT 1"
        
            # Add a query batch with both of our queries
            self.add_query_batch([response_check_query, last_post_id_query])
            
            # Send all batches (which is just our one query batch)
            responses = self.transmit_batches()
            query_batch_response = responses[0]

            last_checked_post_id = query_batch_response[1].rows[0][0]
            response_rows = query_batch_response[0].rows

            if len(response_rows) > 0:
                data = response_rows[0][0]
                if data.split(':')[2] == "e":
                    error = data.split(':')[3]
                    raise RuntimeError("Error in serving script: " + error)
                    
                decoded = json.loads(data[len(RETURN_PREFIX)+len(str(post_id)+":"):])
                if convert_unicode_to_str:
                    decoded = convert_json_unicode_to_str(decoded)
                return decoded

            elapsed_time = time.time() - start_time
            if timeout is not None and elapsed_time > timeout:
                raise RuntimeError("timeout elapsed")

            time.sleep(elapsed_time/20)
            
    def post_var_json(self, name, obj):
        try:
            encoded = json.dumps(obj)
        except TypeError:
            raise ValueError('object is not json-serializable')
        
        prefix = "v:json:"+name+":"
        data = prefix + encoded
        
        return self.post(data)
    
    def fetch_var_json(self, address, name, max_size_cost=DEFAULT_QUERY_MAX_SIZE_COST, convert_unicode_to_str=True):
        prefix = "v:json:"+name+":"
        
        query_result = self.query("SELECT SUBSTRING(data, " + str(len(prefix)+1) + ", LENGTH(data)) FROM posts WHERE address = '" + address + "' AND data LIKE '"+prefix+"%' ORDER BY post_id DESC LIMIT 1", max_size_cost=max_size_cost)
        if len(query_result.rows) == 0:
            return None
        encoded = query_result.rows[0][0]
        
        try:
            decoded = json.loads(encoded)
        except ValueError:
            raise RuntimeError("error in decoding fetched object")
        
        if convert_unicode_to_str:
            decoded = convert_json_unicode_to_str(decoded)
            
        return decoded