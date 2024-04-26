#!/usr/bin/env python
#
#  Copyright (c) 2023 Bowe Strickland <bowe@yak.net>
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
__author__ = 'Bowe Strickland <bowe@ryak.net>'
__docformat__ = 'restructuredtext'

import logging
import os
import functools

import attr
import stripe
import arrow
import datemath
from pydash import py_

LOG = logging.getLogger(__name__)
stripe.api_key = os.environ.get("STRIPE_API_KEY")


@attr.s
class StripeClient:

    time_attrs = """
        created available_on
    """.split()

    api_key = attr.ib(default=os.environ.get("STRIPE_API_KEY"))
    dflt_when = "now-1M/M:now-1M/M"  # last month

    def _to_timestamp(self, time_str):
        return int(arrow.get(time_str).float_timestamp)

    def _from_timestamp(self, timestamp):
        return str(arrow.get(timestamp))

    def _from_stripe_item(self, obj):
        data = stripe.util.convert_to_dict(obj)
        for k in self.time_attrs:
            value = py_.get(data, k)
            if value is not None:
                py_.set(data, k, self._from_timestamp(value))

        # dodgy check for email
        desc = data.get("description")
        if desc:
            last_token = desc.split()[-1]
            if "@" in last_token:
                data.setdefault('email', last_token)

        return data

    def _from_iter(self, itr):
        for obj in itr:
            yield self._from_stripe_item(obj)

    def _convert_when(self, when):
        result = {}
        start, _, end = when.partition(":")
        if start:
            result["gte"] = self._to_timestamp(str(datemath.dm(start)))
        if end:
            result["lte"] = self._to_timestamp(
                str(datemath.dm(end, roundDown=False))
            )
        return result

    @functools.cached_property
    def client(self):
        return stripe.StripeClient(self.api_key)

    def customer_iter(self, when=dflt_when):
        result = stripe.Customer.list(
            created=self._convert_when(when)
        )
        for obj in self._from_iter(result.auto_paging_iter()):
            yield obj

    def balance_transaction_iter(self, when=dflt_when, add_address=True):
        result = stripe.BalanceTransaction.list(
            created=self._convert_when(when)
        )
        for obj in self._from_iter(result.auto_paging_iter()):
            if add_address:
                if obj["source"].startswith("ch_"):
                    charge = self.get_charge(obj["source"])
                    obj["billing_details"] = charge["billing_details"]
            yield obj

    def get_charge(self, charge_id):
        result = stripe.Charge.retrieve(charge_id)
        return self._from_stripe_item(result)

# vi: ts=4 expandtab
