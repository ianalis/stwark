from __future__ import absolute_import

import tempfile
import os
import pytest
from ..stwark import read_settings

def test_oauth_command_line():
    """Read both OAuth token and secret from command line"""
    assert read_settings('abc 123'.split()) == \
           {'oauth_token': 'abc',
            'oauth_secret': '123',
            'app_key': 'RWmvpkGK4m9tavh4bCfdzsYjH',
            'app_secret': 'uCShewTskeuBvt9haLi8LFARSJXkxJsCPNZ3dGwpYz4vuc5Mo9',
            'config': 'stwark.cfg',
            'prefix': 'data'}

def test_all_command_line():
    """Read all settings from command line"""
    assert read_settings('abc 123 -p testpre'.split()) == \
           {'oauth_token': 'abc',
            'oauth_secret': '123',
            'app_key': 'RWmvpkGK4m9tavh4bCfdzsYjH',
            'app_secret': 'uCShewTskeuBvt9haLi8LFARSJXkxJsCPNZ3dGwpYz4vuc5Mo9',
            'config': 'stwark.cfg',
            'prefix': 'testpre'}
       
def test_oauth_config():
    """Read both OAuth token and secret from config"""
    f = tempfile.NamedTemporaryFile(delete=False)
    fname = f.name
    f.write(b"""
[stwark]
oauth_token = abc
oauth_secret = 123
            """)
    f.close()
    
    try:
        assert read_settings('--config {}'.format(fname).split()) ==\
           {'oauth_token': 'abc',
            'oauth_secret': '123',
            'app_key': 'RWmvpkGK4m9tavh4bCfdzsYjH',
            'app_secret': 'uCShewTskeuBvt9haLi8LFARSJXkxJsCPNZ3dGwpYz4vuc5Mo9',
            'config': fname,
            'prefix': 'data'}
    finally:
        os.unlink(fname)
        
def test_all_config():
    """Read all settings from config"""
    f = tempfile.NamedTemporaryFile(delete=False)
    fname = f.name
    f.write(("""
[stwark]
oauth_token = abc
oauth_secret = 123
prefix = testpre
config = {}
            """.format(fname)).encode('utf8'))
    f.close()
    
    try:
        assert read_settings('--config {}'.format(fname).split()) ==\
           {'oauth_token': 'abc',
            'oauth_secret': '123',
            'app_key': 'RWmvpkGK4m9tavh4bCfdzsYjH',
            'app_secret': 'uCShewTskeuBvt9haLi8LFARSJXkxJsCPNZ3dGwpYz4vuc5Mo9',
            'config': fname,
            'prefix': 'testpre'}
    finally:
        os.unlink(fname)
        
def test_mixed_config():
    """Read settings from both command line and config"""
    f = tempfile.NamedTemporaryFile(delete=False)
    fname = f.name
    f.write(("""
[stwark]
oauth_token = abc
oauth_secret = 123
app_key = xyz
app_secret = 789
config = {}
            """.format(fname)).encode('utf8'))
    f.close()
    
    try:
        assert read_settings('--config {} -p testpre'.format(fname).split()) ==\
           {'oauth_token': 'abc',
            'oauth_secret': '123',
            'app_key': 'xyz',
            'app_secret': '789',
            'config': fname,
            'prefix': 'testpre'}
    finally:
        os.unlink(fname)
        
def test_missing_oauth():
    """Exit if OAuth token and secret are missing"""
    with pytest.raises(SystemExit):
        read_settings('-p testpre'.split())
        
    with pytest.raises(SystemExit):
        f = tempfile.NamedTemporaryFile(delete=False)
        fname = f.name
        f.write(("""
[stwark]
prefix = testpre
config = {}
                """.format(fname)).encode('utf8'))
        f.close()
        
        try:
            read_settings('--config {}'.format(fname).split())
        finally:
            os.unlink(fname)
