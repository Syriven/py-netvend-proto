"""
NetVendAPI - An API for NetVend written by @BardiHarborow and Syriven.

This module is split into three parts:
    * NetVendCore (signing and sending commands)
    * NetVendBasic (formatting commands and parsing the server responses)
    * NetVendExtended (additional convenience methods).

NetVendCore should be the most stable. Each extension adds usability, but may be
less stable.

NetVend is an alias for the current class we consider stable (NetVendBasic, at 
the moment) and should be used in most programs. We'd rather you get an error
than lose a chunk of BTC.

The code should be mostly self-explanatary. Note that raw_signed_command in
NetVendBasic is not a bug; read the code a couple of times *before* you file a
bug report.

If you\'re cashed up and would like to spread some love, send a few bitcents our
way =)

Created by:
@Syriven (1MphZghyHmmrzJUk316iHvZ55UthfHXR34) and;
@BardiHarborow (1Bardi4eoUvJomBEtVoxPcP8VK26E3Ayxn)

Special Thanks to /u/minisat_maker on reddit for the orginal concept for NetVend.

Copyright (c) Bardi Harborow and @Syriven 2013.
Licensed under the Creative Commons Attribution 3.0 Unported License.
Grab a copy at http://creativecommons.org/licenses/by/3.0/

This project follows The Semantic Versioning 2.0.0 Specification as defined at
http://semver.org/.
"""

import sys, thread

if sys.hexversion < 0x02000000 or  sys.hexversion >= 0x03000000:
    raise RuntimeError("netvend requires Python 2.x.")

import json, pybitcointools
try:
    import urllib, urllib2
    urlopen = urllib2.urlopen
    urlencode = urllib.urlencode
except ImportError:
    import urllib.request
    urlopen = urllib.request.urlopen
    import urllib.parse
    urlencode = urllib.parse.urlencode

NETVEND_URL = "http://ec2-54-213-176-154.us-west-2.compute.amazonaws.com/command.php"

class NetvendResponseError(BaseException):
    def __init__(self, response):
        self.response = response

    def __str__(self):
        return self.response['error_code']+": "+self.response['error_info']

class AgentCore(object):
    '''Base class providing a skeleton framework. This should be stable.'''
    def __init__(self, private, url=NETVEND_URL, seed=False):
        if seed:
            self._private = pybitcointools.sha256(private)
        else:
            try:
                self._private = pybitcointools.b58check_to_hex(private)
            except AssertionError:
                raise RuntimeError("Invalid private key. Did you mean to set seed=True?")

        self.address = pybitcointools.pubkey_to_address(pybitcointools.privtopub(self._private))
        self.url = url
    
    def get_address(self):
        return self.address
    
    def sign_command(self, command):
        return pybitcointools.ecdsa_sign(command, self._private)

    def send_command(self, command, sig):
        return urlopen(self.url, urlencode({'address': self.get_address(), 'command' : command, 'signed' : sig})).read()
        
    def sign_and_send_command(self, command):
        sig = self.sign_command(command)
        return self.send_command(command, sig)
    

class AgentBasic(AgentCore):
    '''Class providing increased functionality (functions for all command types and afunction to make server output nicer). This should be stable.'''
    def __init__(self, private, url=NETVEND_URL, seed=False):
        AgentCore.__init__(self, private, url, seed)
        self.max_query_fee = 3000

    def set_max_query_fee(self, fee):
        self.max_query_fee = fee

    def post_process(self, data):
        try:
            data = json.loads(data)
        except ValueError:
            raise ValueError("Can't parse server response. Server responded with:\n" + data)
        return_dict = {}
        return_dict['success'] = data[0]
        if not return_dict['success']:
            return_dict['error_code'] = data[1]
            return_dict['error_info'] = data[2]
        else:
            return_dict['history_id'] = data[1]
            return_dict['charged'] = data[2]
            raw_command_result = data[3]
            if isinstance(raw_command_result, int): # For Tips and Data
                command_result = raw_command_result
            else: # For Query
                command_result = {}
                command_result['success'] = raw_command_result[0]
                if command_result['success']:
                    command_result['num_rows'] = raw_command_result[1]
                    command_result['rows'] = raw_command_result[2]
                    command_result['field_types'] = raw_command_result[3]
                else:
                    fees = {'base': raw_command_result[1][0],
                            'time': raw_command_result[1][1],
                            'size': raw_command_result[1][2],
                            'total': raw_command_result[1][3]
                            }
                    command_result['fees'] = fees
            return_dict['command_result'] = command_result
        return return_dict
    
    #def send_signed_command(self, command, signed):
    #    return self.post_process(AgentCore.send_signed_command(self, command, signed))

    def handle_command_asynch(self, command, callback):
        server_response = self.sign_and_send_command(command)
        callback(self.post_process(server_response))

    def handle_command(self, command, callback):
        if callback is None:
            return self.post_process(self.sign_and_send_command(command))
        else:
            thread.start_new_thread(self.handle_command_asynch, (command, callback))
        
    def post(self, data, callback=None):
        return self.handle_command(json.dumps(['p', data], separators=(',',':')), callback)

    def tip(self, address, amount, data_id, callback=None):
        if data_id == None:
            data_id = 0
        return self.handle_command(json.dumps(['t', address, amount, data_id], separators=(',',':')), callback)
    
    def query(self, sql, callback=None):
        return self.handle_command(json.dumps(['q', sql, self.max_query_fee], separators=(',',':')), callback)
    
    def withdraw(self, amount, callback=None):
        return self.handle_command(json.dumps(['w', amount], separators=(',',':')), callback)

class AgentExtended(AgentBasic):
    '''NetVendCore - Less stable functionality. Experimental, may change at any time.'''
    
    def fetchBalance(self):
        query = "SELECT balance FROM accounts WHERE address = '" + self.get_address() + "'"
        return int(self.query(query)['command_result']['rows'][0][0])

Agent = AgentExtended
