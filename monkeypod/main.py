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

import logging

import functions_framework
import arrow
from pydash import py_


from stripe_client import StripeClient
from client import MonkeyPodClient
from manager import MonkeyPodManager
from google_workspace import get_root_folder

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def run():

    now = str(arrow.utcnow())[:19]
    tag = f"stripe_import_{now}"

    # get iterator to stripe transactions
    sc = StripeClient()
    when = "now-1M/M:now-1M/M"
    itr = sc.balance_transaction_iter(when)

    # extract monkeypod import data
    client = MonkeyPodClient()
    mgr = MonkeyPodManager(client)
    data = mgr.gen_stripe_imports_from_recs(
        itr, confirm_entities=True, tag=tag,
    )

    # create the stripe import google sheet
    root_folder = get_root_folder()
    si_spreadsheet = root_folder.create(tag, "spreadsheet")
    info = si_spreadsheet.get_info()
    si_name = py_.get(info, "properties.title")
    si_url = py_.get(info, "spreadsheetUrl")
    LOG.info(f"created spreadsheet {si_name} {si_url}")

    # insert the sheet data
    si_spreadsheet.add_sheets(sorted(data), purge=True)
    for k, info in data.items():
        def _extract_data(flds, row):
            return [str(row.get(k, "")) for k in flds]
        flds = info['fields']
        rr = [_extract_data(flds, row) for row in info['rows']]
        result = si_spreadsheet.write(
            f"{k}!A1", [flds] + rr,
        )
        result.pop('spreadsheetId')
        LOG.info(result)

    return f"{si_name} {si_url}"


@functions_framework.http
def my_http_function(request):
    return run()


@functions_framework.cloud_event
def handle_event(cloud_event):
    result = run()
    LOG.info("cloudevent details")
    LOG.info(cloud_event)
    LOG.info(cloud_event.__dict__)
    return result


if __name__ == '__main__':
    print(run())
