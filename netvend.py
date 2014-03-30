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

NETVEND_URL = "http://ec2-54-213-176-154.us-west-2.compute.amazonaws.com/command.php"

import json
import hashlib
try:
    import urllib, urllib2
    urlopen = urllib2.urlopen
    urlencode = urllib.urlencode
except ImportError:
    import urllib.request
    urlopen = urllib.request.urlopen
    import urllib.parse
    urlencode = urllib.parse.urlencode
    
try:
    import pybitcointools
except ImportError:
    raise ImportError('Download http://git.io/BTNMqw and rename it pybitcointools.py')

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
            self._private = pybitcointools.b58check_to_hex(private)
        self.address = pybitcointools.pubkey_to_address(pybitcointools.privtopub(self._private))
        self.url = url
    
    def get_address(self):
        return self.address
    
    def sign_command(self, command):
        return pybitcointools.ecdsa_sign(command, self._private)
    
    def raw_signed_command(self, command, signed):
        return urlopen(self.url, urlencode({'address': self.get_address(), 'command' : command, 'signed' : signed})).read()
        
    def raw_command(self, command):
        return self.raw_signed_command(command, self.sign_command(command))
    

class AgentBasic(AgentCore):
    '''Class providing increased functionality (functions for all command types and afunction to make server output nicer). This should be stable.'''
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
                    fees = {'base' : raw_command_result[1][0],
                            'time' : raw_command_result[1][1],
                            'size' : raw_command_result[1][2],
                            'total' : raw_command_result[1][3]
                            }
                    command_result['fees'] = fees
            return_dict['command_result'] = command_result
        return return_dict
    
    def raw_signed_command(self, command, signed):
        return self.post_process(AgentCore.raw_signed_command(self, command, signed))
        
    def post(self, data):
        return self.raw_command(json.dumps(['p', data], separators=(',',':')))
    
    def tip(self, address, amount, data_id):
        #if type(address) == type(NetVendCore)
        return self.raw_command(json.dumps(['t', address, amount, data_id], separators=(',',':')))
    
    def query(self, sql, max_fee):
        return self.raw_command(json.dumps(['q', sql, max_fee], separators=(',',':')))
    
    def withdraw(self, amount):
        return self.raw_command(json.dumps(['w', amount], separators=(',',':')))

class AgentExtended(AgentBasic):
    '''NetVendCore - Less stable functionality. Experimental, may change at any time.'''
    
    def fetchBalance(self):
        query = "SELECT balance FROM accounts WHERE address = '" + self.get_address() + "'"
        return int(self.query(query, 3000)['command_result']['rows'][0][0])

Agent = AgentExtended
