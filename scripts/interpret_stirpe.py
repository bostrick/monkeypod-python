#!/usr/bin/env python
#
#  Copyright (c) 2021 Red Hat, Inc.  <bowe@redhat.com>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
#
"""
"""
__author__ = 'RHT Platform <gls-platform@redhat.com>'
__docformat__ = 'restructuredtext'


import sys
import csv
from pathlib import Path
import pydash as py_

import yaml

p = Path(sys.argv[1])
reader = csv.DictReader(p.open())
data_list = list(reader)

idx = 0
for data in data_list:
    data['row'] = idx
    idx += 1

data_map = py_.key_by(data_list, 'row')
for k in data_map.keys():
    data_map[k] = {k: v for k, v in data_map[k].items() if v}

payout_map = py_.group_by(data_map.values(), 'Transfer')

payout_tx_map = {}
for key, transfers in payout_map.items():
    payout_tx_map[key] = py_.group_by(transfers, 'Type')

payouts = list(payout_tx_map.values())
payouts = sorted(payouts, key=lambda x: x['payout'][0]['Transfer Date (UTC)'])

for p in reversed(payouts):

    if len(p['payout']) != 1:
        raise ValueError('bad payout')

    payout = p['payout'][0]
    amt = payout['Amount']
    when = payout['Transfer Date (UTC)']
    transfer = payout['Transfer']

    print(f"{when} {transfer} {amt}")

    for c in p.get('charge', []):
        when = c['Created (UTC)']
        amt = c['Amount']
        desc = c['Description']
        fee = c['Fee']
        net = c['Net']

        print(f"\t{when} {amt:10} {fee:10} {net:10} {desc}")
