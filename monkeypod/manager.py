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
import collections
import csv
from pathlib import Path
from functools import cached_property

import attr
import arrow
import yaml

LOG = logging.getLogger(__name__)

HERE = Path(__file__)


@attr.s
class MonkeyPodManager:

    client = attr.ib()

    import_path = HERE / "data/imports/monkey_pod_columns.yaml"

    @cached_property
    def import_column_map(self):
        return yaml.safe_load(self.import_path.read_text())

    def _extract_data(self, item, attr_map):
        # forces all entries to strings
        ndata = {
            v: str(item.get(k, "")).strip()
            for k, v in attr_map.items()
        }
        ndata = {k: v for k, v in ndata.items() if v}
        return ndata

    def import_entities(
        self,
        entity_list,
        attr_map=None,
        source_attr=None,
        import_attr=None,
    ):

        nadded = 0
        for entity in entity_list:

            if attr_map:
                entity = self._extract_data(entity, attr_map)

            email = entity.get('email')
            if not email:
                LOG.warning(f"skipping {entity}: no email")
                continue

            match = self.client.entity_match(email=email)
            if match:
                # TODO... merge additional attributes into entity
                LOG.info(f"skipping {entity}: exists")
                continue

            entity.setdefault("type", "Individual")

            extra = entity.setdefault("extra_attributes", {})
            if source_attr:
                extra.setdefault("source", source_attr)
            if import_attr:
                extra.setdefault("import", import_attr)

            roles = set(entity.setdefault("roles", []))
            roles.add("Donor")
            roles.add("Customer")
            entity['roles'] = list(roles)

            LOG.info(f"creating entity {email}")
            _ = self.client.entity_create(entity)
            nadded += 1

        return nadded

    #####################################################################
    # stripe imports
    #####################################################################

    def gen_stripe_imports(self, rows, tag=None):

        tag = tag or arrow.utcnow().format("YYYYMMDD")

        # filter out empty rows
        try:
            rows = [r for r in rows if "Description" in r]
        except Exception:
            breakpoint()

        # assign row numbers and normalize
        for idx, r in enumerate(rows):
            r["_row_number"] = idx
            self._normalize_row(r)

        # qualify the transactions
        parts = self._partition_rows(rows)

        # process transactions
        self._report_unknown(parts["unknown"])
        self._gen_stripe_donations_csv(parts["donations"], tag)
        self._gen_stripe_transfers_csv(parts["transfers"], tag)
        self._gen_stripe_sales_csv(parts["charges"], tag)
        self._gen_stripe_fees_csv(rows, tag)

    def _normalize_row(self, row):
        row["Amount"] = row["Amount"].replace(",", "")
        row["Fee"] = row["Fee"].replace(",", "")
        if not row.get("email", "").strip():
            email = row["Description"].split()[-1]
            if "@" in email:
                row["email"] = email

    def _partition_rows(self, rows):

        results = collections.defaultdict(list)
        for row in rows:
            desc = row["Description"]
            if "PAYOUT" in desc:
                results["transfers"].append(row)
            elif desc.startswith("Donation by"):
                results["donations"].append(row)
            elif desc.startswith("Charge for"):
                results["charges"].append(row)
            elif desc.startswith("Invoice"):
                results["invoices"].append(row)
            else:
                results["unknown"].append(row)

        counts = {k: len(v) for k, v in results.items()}
        LOG.info(f"read transactions: {counts}")
        return results

    def _report_unknown(self, rows):
        for r in rows:
            LOG.warning(f"unknown transaction: {r}")

    def _gen_stripe_donations_csv(self, rows, tag):

        def _gen_row(data):

            return {
                'Date': arrow.get(data["Created (UTC)"]).format("YYYY-MM-DD"),
                'Donor': data["email"],
                'Amount': data["Amount"],
                'Income Account': "Contributions",
                'Asset Account': "Stripe Account",
                'External ID': data["id"],
                'Ref Number': data["id"],
                'Payment Method': "Other",
                'Memo': data.get("memo", ""),
                'Class': data.get("class", ""),
                'Tags': data.get("tags", ""),
            }

        fields = [
            'Date',
            'Donor',
            'Amount',
            'Income Account',
            'Asset Account',
            'External ID',
            'Ref Number',
            'Payment Method',
            'Memo',
            'Class',
            'Tags',
        ]

        fname = f"stripe_donations_{tag}.csv"
        LOG.info(f"writing {len(rows)} donations to {fname}")
        with open(fname, "w") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            [writer.writerow(_gen_row(r)) for r in rows]

        return fname

    def _gen_stripe_transfers_csv(self, rows, tag):

        def _gen_row(data):
            return {
                'Date': arrow.get(data["Created (UTC)"]).format("YYYY-MM-DD"),
                'Amount': -float(data["Amount"]),
                'To Account': "Stripe Account",
                'From Account': "Wells Fargo - Main 1582",
                'External ID': data["id"],
                'Ref Number': data["id"],
                'Memo': data["Description"],
            }

        fields = [
            "Date",
            "Amount",
            "From Account",
            "To Account",
            "External ID",
            "Ref Number",
            "Memo",
        ]

        fname = f"stripe_transfers_{tag}.csv"
        LOG.info(f"writing {len(rows)} transfers to {fname}")
        with open(fname, "w") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            [writer.writerow(_gen_row(r)) for r in rows]

        return fname

    def _gen_stripe_sales_csv(self, rows, tag):

        def _gen_row(data):

            return {
                'Date': arrow.get(data["Created (UTC)"]).format("YYYY-MM-DD"),
                'Customer': data["email"],
                'Total': data["Amount"],
                'Item': data["item"],
                'Quantity': data["quantity"],
                'Asset Account': "Stripe Account",
                'External ID': data["id"],
                'Payment Method': "Other",
                'Memo': data.get("memo", ""),
                'Class': data.get("class", ""),
                'Tags': data.get("tags", ""),
            }

        fields = [
            'Date',
            'Customer',
            'Total',
            'Item',
            'Quantity',
            'Asset Account',
            'External ID',
            'Payment Method',
            'Memo',
            'Class',
            'Tags',
        ]

        fname = f"stripe_sales_{tag}.csv"
        LOG.info(f"writing {len(rows)} sales to {fname}")
        with open(fname, "w") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            [writer.writerow(_gen_row(r)) for r in rows]

        return fname

    def _gen_stripe_fees_csv(self, rows, tag):

        transactions = collections.defaultdict(list)
        for r in rows:
            when = arrow.get(r["Created (UTC)"])
            bucket = when.format("YYYY-MM")
            transactions[bucket].append(float(r["Fee"]))

        data = []
        for k in sorted(transactions):
            data.append({
                'Payee': "stripe",
                'Date': f"{k}-28",
                'Amount': sum(transactions[k]),
                'Expense Account': "Bank Fees",
                'Checking Account': "Stripe Account",
                'Ref Number': f"calculated stripe_fees_{k}",
                'External ID': f"calculated stripe_fees_{k}",
            })

        fields = [
            'Payee',
            'Date',
            'Amount',
            'Expense Account',
            'Checking Account',
            'Ref Number',
            'External ID',
        ]

        fname = f"stripe_fees_{tag}.csv"
        LOG.info(f"writing {len(data)} fees to {fname}")
        with open(fname, "w") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            [writer.writerow(d) for d in data]

        return fname

