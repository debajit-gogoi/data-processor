# Copyright (c) 2022 debajit Inc. All rights reserved.
#
# Author: gdowding@debajit.com
#
"""
Common definitions and functions for ETL operations
"""

import argparse
import configparser
import logging


def setup_main(log_level=logging.INFO):
    """Set up for programs that perform ETL.
    Should only be called from main programs.
    1. Set up logging configuration.
    Returns: None
    """
    logging.basicConfig(format='%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s',
                        level=log_level)


def get_arg_parser():
    """Create argument parser with flags that are common to all scripts.
    Returns: ArgumentParser object configured for this program.
    """
    argp = argparse.ArgumentParser()
    argp.add_argument("--config", help='required: configuration file')
    argp.add_argument("--debug", help='run in debug mode', action='store_true')
    return argp


def check_config(args):
    '''Common configuration checks.'''
    error = False
    if args.config is None:
        logging.error('missing required argument "config"')
        error = True
    return error


def get_config(path):
    """Read 'path' and return a parsed config object'
    Returns: Config object.
    """
    cp = configparser.ConfigParser()
    cp.read(path)
    return cp
