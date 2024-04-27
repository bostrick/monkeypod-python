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
import functools

import functions_framework
import arrow
import yaml
import attr
from pydash import py_

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from stripe_client import StripeClient
from client import MonkeyPodClient
from manager import MonkeyPodManager

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger()

SCOPES = [
    "https://www.googleapis.com/auth/drive.appdata",
    "https://www.googleapis.com/auth/drive.appfolder",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]
SAMPLE_SPREADSHEET_ID = "18H5ByEuxGdsxzHyTpgeMqLim1z2Hl0bDvdClst5-9kU"
GCLOUD_FOLDER = "15RYPb4wld4imDMyyJF-lTmZiKm7jUCaC"
sa_cred_file = "/etc/secrets/service-account.json"

doc_types = "folder spreadsheet".split()
mime_types = {d: f"application/vnd.google-apps.{d}" for d in doc_types}
rev_mime_types = {v: k for k, v in mime_types.items()}


@attr.s
class GoogleManager:

    creds = attr.ib()
    scopes = attr.ib(factory=SCOPES.copy)

    @functools.cached_property
    def drive_service(self):
        return build("drive", "v3", credentials=self.creds)

    def get_user_info(self):
        return self.drive_service.about().get(
            fields="kind,user"
        ).execute()


@attr.s
class DriveObject:

    doc_type = None         # subclass responsibiillity

    doc_id = attr.ib()
    manager = attr.ib(repr=False)
    info = attr.ib(factory=dict)

    _CLASS_REGISTRY = {}

    @classmethod
    def register(cls, subclass):
        cls._CLASS_REGISTRY[subclass.doc_type] = subclass
        return subclass

    @classmethod
    def promote(cls, info, manager):
        kind = rev_mime_types.get(info['mimeType'])
        kls = cls._CLASS_REGISTRY.get(kind, cls)
        return kls(info['id'], manager, info)


@attr.s
@DriveObject.register
class DriveFolder(DriveObject):

    doc_type = "folder"

    def create(self, name, doc_type='folder'):
        metadata = {
            'name': name,
            'parents': [self.doc_id],
            'mime_type': mime_types[doc_type],
        }
        result = self.manager.drive_service.files().create(
            body=metadata,
        ).execute()
        return DriveObject.promote(result, self.manager)

    def list(self):
        results = (
            self.manager.drive_service.files()
            .list(
                pageSize=10,
                q=f"'{self.doc_id}' in parents",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
            )
            .execute()
        )
        return [
            DriveObject.promote(info, self.manager)
            for info in results.get("files", [])
        ]


@attr.s
@DriveObject.register
class DriveSpreadsheet(DriveObject):

    doc_type = "spreadsheet"

    @functools.cached_property
    def service(self):
        return build("sheets", "v4", credentials=self.manager.creds)

    @functools.cached_property
    def api(self):
        return self.service.spreadsheets()

    def _add_sheet_req(self, name):
        return {'addSheet': {'properties': {'title': name}}}

    def _del_sheet_req(self, idx):
        return {'deleteSheet': {'sheetId': idx}}

    def _batch_update(self, requests):
        results = self.api.batchUpdate(
            spreadsheetId=self.doc_id,
            body={"requests": requests},
        ).execute()
        return results

    def get_info(self):
        self.info = self.api.get(spreadsheetId=self.doc_id).execute()
        return self.info

    def add_sheets(self, names, purge=False):
        rr = [self._add_sheet_req(n) for n in names]
        if purge:
            rr.append(self._del_sheet_req(0))
        return self._batch_update(rr)

    def write(self, range_name, rows, value_input_option="USER_ENTERED"):
        results = self.api.values().update(
            spreadsheetId=self.doc_id,
            range=range_name,
            valueInputOption=value_input_option,
            body={"values": rows},
        ).execute()
        return results


def get_root_folder():
    creds = Credentials.from_service_account_file(
        sa_cred_file,
        scopes=SCOPES,
    )
    mgr = GoogleManager(creds)
    return DriveFolder(GCLOUD_FOLDER, mgr)


def run():

    now = str(arrow.utcnow())[:19]
    tag = f"stripe_import_{now}"

    # get iterator to stripe transactions
    sc = StripeClient()
    # when = "now-1M/M:now-1M/M"
    when = "now-1w/w:now-1w/w"
    itr = sc.balance_transaction_iter(when)

    # extract monkeypod import data
    client = MonkeyPodClient()
    mgr = MonkeyPodManager(client)
    data = mgr.gen_stripe_imports_from_recs(itr, tag)

    # create the stripe import google sheet
    root_folder = get_root_folder()
    si_spreadsheet = root_folder.create(tag, "spreadsheet")
    info = si_spreadsheet.get_info()
    _name = py_.get(info, "properties.title")
    _url = py_.get(info, "spreadsheetUrl")
    LOG.info(f"created spreadsheet {_name} {_url}")

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


@functions_framework.http
def my_http_function(request):
    return run()


if __name__ == '__main__':
    run()
