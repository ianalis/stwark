#!/usr/bin/env python
import argparse
import configparser
import os
import sys
import json
from datetime import datetime, timedelta
from twython import TwythonStreamer
from bz2 import BZ2File
from requests.exceptions import ChunkedEncodingError, ConnectionError
from OpenSSL.SSL import ZeroReturnError

class OutputStream(object):
    def __init__(self, archive_dir, prefix='data'):
        self.archive_dir = archive_dir
        if not os.path.exists(archive_dir):
            os.makedirs(archive_dir)
        self.prefix = prefix
        self.curhour = None
        self._outfile = None
        
    def restart(self, base_datetime):
        """
        Start a new hourly output tarball.
        
        Parameters
        ----------
        base_datetime : datetime
            Hour of output will be based on this
        move_current : bool
            Move current archive file from working dir to archive dir
        """
        if self._outfile:
            self._outfile.close()

        if move_current:
            if not os.path.exists(self.archive_dir):
                os.makedirs(self.archive_dir)
            os.replace(f.name,
                       os.path.join(self.archive_dir, 
                                    os.path.basename(f.name)))

        self.curhour = base_datetime.replace(minute=0, second=0, microsecond=0)
        self._outfile = BZ2File(os.path
                                  .join(self.working_dir,
                                        self.curhour
                                            .strftime(self.prefix+'-%y%m%d%H.json.bz2')),
                                'a')
        
    def write(self, data):
        self._outfile.write((json.dumps(data) + '\r\n').encode('utf8'))        
        
class SampleStreamer(TwythonStreamer):
    def __init__(self, app_key, app_secret, oauth_token, oauth_token_secret,
                 outstream):
        super(SampleStreamer, self).__init__(app_key, app_secret, oauth_token, 
                                             oauth_token_secret)
        
        self.outstream = outstream

    def on_success(self, data):
        if 'created_at' in data:
            created_at = datetime.strptime(data['created_at'], 
                                           '%a %b %d %H:%M:%S +0000 %Y') 
            if created_at > self.outstream.curhour + timedelta(hours=1):
                self.outstream.restart(created_at)
        self.outstream.write(data)

    def on_error(self, status_code, data):
        print(status_code)

def read_settings(args_source=sys.argv[1:]):
    """Read settings from command line and config file"""
    # read from command line
    parser = argparse.ArgumentParser()
    parser.add_argument('oauth_token',
                        nargs='?',
                        help='User OAuth token')
    parser.add_argument('oauth_secret', 
                        nargs='?',
                        help='User OAuth secret')
    parser.add_argument('-p', '--prefix',
                        help='Name to start filenames with (default: data)')
    parser.add_argument('--config',
                        default='stwark.cfg',
                        help='Read settings from supplied config file '
                             '(default: stwark.cfg)')
    
    args = parser.parse_args(args_source)
    
    # read from config file
    settings = {
        'app_key': 'RWmvpkGK4m9tavh4bCfdzsYjH',
        'app_secret': 'uCShewTskeuBvt9haLi8LFARSJXkxJsCPNZ3dGwpYz4vuc5Mo9',
        'prefix': 'data',
    }    
    config = configparser.SafeConfigParser(settings)
    if os.path.exists(args.config):
        config.read(args.config)
    if config.has_section('stwark'):
        settings.update(dict(config.items('stwark')))
     
    # overwrite settings from config line with command line arguments
    for key, value in vars(args).items():
        if value is not None:
            settings[key] = value
            
    if (not 'oauth_token' in settings) or (not 'oauth_secret' in settings):
        print("Both OAuth token and secret must be defined in either command "
              "line or config file")
        sys.exit(-1)
    
    return settings
    
if __name__ == "__main__":
    settings = read_settings()
    
    outstream = OutputStream('data/sample', settings['prefix'])
    outstream.restart(datetime.utcnow())
    while True:
        try:
            streamer = SampleStreamer(settings['app_key'], 
                                      settings['app_secret'], 
                                      settings['oauth_token'], 
                                      settings['oauth_secret'],
                                      outstream)
            streamer.statuses.sample()
        except (ChunkedEncodingError, ConnectionError, ZeroReturnError):
            pass
