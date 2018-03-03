#
#    File:   jemdefs.py
#
#    Description:
#       Library of common definitions used by the "jem(abs|del|eff)" tools.
#
#    Usage:
#       To use in an externally-callable utility Python script, include the line:
#
#       from jemdefs import *
#

import sys

sys.path.insert(0, '.')

one_kay = 1024
one_meg = one_kay * one_kay
one_gig = one_kay * one_meg

def readable(sz):
    asz = abs(sz)
    if (asz >= one_gig):
        scale = ' GB'
        quant = sz / one_gig
    elif (asz >= one_meg):
        scale = ' MB'
        quant = sz / one_meg
    elif (asz >= one_kay):
        scale = ' KB'
        quant = sz / one_kay
    else:
        scale = ' B'
        quant = sz
    return str(quant) + scale

def pct(n, d):
   return str(int(10000 * (n / d)) / 100.0) + '%'

table = {}

i = -1
j = None

def arenas(name):
    global i, j
    if (name == 'sum'):
        i += 1
        table[i] = {}
    else:
        name = int(name)
    j = name
    table[i][j] = {}

def total(val):
    global i, j
    table[i][j]['total'] = int(val)

def active(val):
    global i, j
    table[i][j]['active'] = int(val)

def mapped(val):
    global i, j
    table[i][j]['mapped'] = int(val)
