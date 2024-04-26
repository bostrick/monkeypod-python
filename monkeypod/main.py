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

from pathlib import Path
import logging
import os
import sys

import functions_framework
import arrow
import yaml

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from stripe_client import StripeClient
from client import MonkeyPodClient
from manager import MonkeyPodManager

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger()

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]
SAMPLE_SPREADSHEET_ID = "18H5ByEuxGdsxzHyTpgeMqLim1z2Hl0bDvdClst5-9kU"
sa_cred_file = "/etc/secrets/service-account.json"


def get_sheets_service():
    creds = Credentials.from_service_account_file(
        sa_cred_file,
        scopes=SCOPES,
    )
    return build("sheets", "v4", credentials=creds)


def get_sheet_data(service):

    cells = "Class Data!A2:E"

    sheet = service.spreadsheets()
    result = (
        sheet.values()
        .get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=cells)
        .execute()
    )
    values = result.get("values", [])

    if not values:
        return "No data found."
    text = os.linesep.join([f"{row[0]}, {row[4]}" for row in values])
    return text


@functions_framework.http
def my_http_function(request):

    local_path = Path()
    result = {'local_path': os.getcwd()}

    service = get_sheets_service()
    result['cells'] = get_sheet_data(service)

    sc = StripeClient()
    when = "now-1M/M:now-1M/M"
    itr = sc.balance_transaction_iter(when)
    client = MonkeyPodClient()
    mgr = MonkeyPodManager(client)
    mgr.gen_stripe_imports_from_recs(
        itr,
        confirm_entities=False,
        tag=str(arrow.utcnow())[:19]
    )

    result['files'] = [str(x) for x in local_path.glob("**")]
    text = yaml.safe_dump(result)
    LOG.info(text)
    return text
