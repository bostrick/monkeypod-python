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
import re
import os
from pathlib import Path
from functools import cached_property

import attr
import arrow
import yaml
from pydash import py_

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
        """Import a list of entities.

        Skip the entitiy if the email already exists.

        'source_attr' and 'import_attr' get added as 'source' and 'import'
        extra attributes.

        attr_map can be used to reshape attrs from another input shape.
        """

        nadded = 0
        for entity in entity_list:

            if attr_map:
                entity = self._extract_data(entity, attr_map)

            email = entity.get('email')
            name = entity.get('name')
            # TODO... merge additional attributes into entity
            if email:
                match = self.client.entity_match(email=email)
                if match:
                    LOG.info(f"skipping {email}: exists")
                    continue
            elif name:
                if name:
                    match = self.client.entity_match(name=name)
                    if match:
                        LOG.info(f"skipping {name}: exists")
                        continue
            else:
                LOG.debug("skipping no name or email")
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

            LOG.info(f"creating entity {email} {name}")
            LOG.info(os.linesep+yaml.safe_dump(entity))
            yesno = input("import entity? (y/N)").strip().lower()
            if yesno and yesno[0] == 'y':
                _ = self.client.entity_create(entity)
                nadded += 1

        return nadded

    stripe_mp_entity_map = yaml.safe_load("""
        email: billing_details.email
        name: billing_details.name
        city: billing_details.address.city
        country: billing_details.address.country
        state: billing_details.address.state
        postal_code: billing_details.address.postal_code
        address: billing_details.address.line1
        # address2: billing_details.address.line2
    """)

    def _confirm_stripe_entity(
        self, item, source_attr=None, import_attr=None
    ):
        entity = self._extract_path_map(
            item, self.stripe_mp_entity_map,
        )
        entity = {k: v.strip() for k, v in entity.items() if v}
        entity = {k: v for k, v in entity.items() if v}
        self.import_entities(
            [entity],
            source_attr=source_attr,
            import_attr=import_attr,
        )
        return entity

    def _extract_path_map(self, src, path_map):
        dst = {}
        for k, v in path_map.items():
            if py_.has(src, v):
                py_.set(dst, k, py_.get(src, v))
        return dst

    #####################################################################
    # stripe imports
    #####################################################################

    row_record_paths = yaml.safe_load("""
        expense account
        class
        tags
        memo
        Created (UTC)
        Description
        Amount
        Fee
        id
    """)

    stripe_import_map = yaml.safe_load("""
      transfer:
        paths:
           Date: created
           Amount: amount
           External ID: id
           Ref Number: id
           Memo: description
        constants:
           From Account: Stripe Account
           To Account: Wells Fargo - Main 1582
        fields:
        - Date
        - Amount
        - From Account
        - To Account
        - External ID
        - Ref Number
        - Memo
      donation:
        paths:
          Date: created
          Amount: amount
          External ID: id
          Ref Number: id
          Memo: memo
          Class: class
          Tags: tags
        constants:
          Income Account: Contributions
          Asset Account: Stripe Account
          Payment Method: Other
        fields:
        - Date
        - Donor
        - Amount
        - Income Account
        - Asset Account
        - External ID
        - Ref Number
        - Payment Method
        - Memo
        - Class
        - Tags
      sale:
        paths:
          Date: created
          Customer: description
          Total: amount
          Item: item
          External ID: id
          Memo: memo
          Class: class
          Tags: tags
        constants:
          Asset Account: Stripe Account
          Payment Method: Other
        fields:
        - Date
        - Customer
        - Total
        - Item
        - Asset Account
        - External ID
        - Payment Method
        - Memo
        - Class
        - Tags
      fee:
        paths:
          Amount: Amount
          Date: Date
        constants:
          Payee: Stripe Transfer
          Expense Account: Bank Fees
          Checking Account: Stripe Account
        fields:
        - Payee
        - Date
        - Amount
        - Expense Account
        - Checking Account
        - Ref Number
        - External ID
    """)

    def gen_stripe_imports_from_recs(
        self, records, tag=None, confirm_entities=False,
    ):

        tag = tag or "stripe_" + arrow.utcnow().format("YYYYMMDD")
        collector = collections.defaultdict(list)
        fee_collector = collections.defaultdict(lambda: 0.0)

        for rec in records:
            if confirm_entities:
                self._confirm_stripe_entity(
                    rec,
                    source_attr="stripe",
                    import_attr=tag,
                )
            what, row = self._stripe_generate_import_row(rec)
            collector[what].append(row)
            self._collect_fee(fee_collector, rec)

        data = dict(collector)
        data["fee"] = self._reduce_fees(fee_collector)
        self.write_csvs(data, tag)
        return data

    def write_csvs(self, data, tag):

        def _write_csv(what, tag, fields, records):

            fname = f"stripe_{what}_{tag}.csv"
            LOG.info(f"writing {len(records)} {what} to {fname}")
            with open(fname, "w") as f:
                writer = csv.DictWriter(f, fieldnames=fields)
                writer.writeheader()
                [writer.writerow(r) for r in records]

        for k in data:
            if k in self.stripe_import_map:
                si_map = self.stripe_import_map[k]
                _write_csv(k, tag, si_map['fields'], data[k])

    def _stripe_generate_import_row(self, record):

        desc = record["description"]

        if "PAYOUT" in desc:
            return self._generate_transfer(record)
        elif desc.startswith("Donation by"):
            return self._generate_donation(record)
        elif desc.startswith("Invoice"):
            return self._generate_sale(record)
        elif desc.startswith("Charge for"):
            return self._generate_sale(record)
        else:
            return "unknown", record

    def _get_entity_identifier(self, record):
        email = py_.get(record, "billing_details.email")
        if email:
            return email
        name = py_.get(record, "billing_details.name")
        if name:
            return name
        raise ValueError("no identifier")

    def _normalize_record(self, record):
        if 'Date' in record:
            record['Date'] = record['Date'][:10]
        if 'Amount' in record:
            record['Amount'] = record['Amount']/100.0
        if 'Total' in record:
            record['Total'] = record['Total']/100.0

    def _generate_base(self, what, record):
        si_map = self.stripe_import_map[what]
        dst = self._extract_path_map(record, si_map["paths"])
        dst.update(si_map["constants"])
        self._normalize_record(dst)
        return dst

    def _generate_transfer(self, record):
        dst = self._generate_base("transfer", record)
        dst["Amount"] = -dst["Amount"]
        return "transfer", dst

    def _generate_donation(self, record):
        dst = self._generate_base("donation", record)
        dst["Donor"] = self._get_entity_identifier(record)
        return "donation", dst

    def _generate_sale(self, record):
        dst = self._generate_base("sale", record)
        dst["Customer"] = self._get_entity_identifier(record)
        if 'Charge for' in record['description']:
            #dst["Item"] = "Community Class"
            #dst["Class"] = "Community Classes"
            dst["Item"] = "Home School"
            dst["Class"] = "Home School"
        elif 'Invoice' in record['description']:
            dst["Memo"] = record["description"]
            dst["Item"] = "Home School"
            dst["Class"] = "Home School"
        return "sale", dst

    def _collect_fee(self, collector, record):
        when = arrow.get(record["created"]).format("YYYY-MM") + "-28"
        collector[when] += record["fee"]

    def _reduce_fees(self, collector):
        rows = [{'Date': k, 'Amount': v} for k, v in collector.items()]
        rows = [self._generate_base("fee", r) for r in rows]
        return rows

    def _write_csv(self, what, tag, fields, records):

        fname = f"stripe_{what}_{tag}.csv"
        LOG.info(f"writing {len(records)} donations to {fname}")
        with open(fname, "w") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            [writer.writerow(r) for r in records]

        return fname

    #####################################################3
    # legacy stripe import from csv
    #####################################################3

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

    #############################################
    # Quickbooks Importing
    #############################################

    qb_import_map = yaml.safe_load("""
      sale:
        paths:
          Date: Date
          Total: Amount
          External ID: Trans ID
          Memo: Batch ID
          Customer: Cardholder Name
        constants:
          Asset Account: Wells Fargo - Main 1582
          Payment Method: Other
          Item: Community Class
          Class: Community Classes
        fields:
        - Date
        - Customer
        - Total
        - Item
        - Asset Account
        - External ID
        - Payment Method
        - Memo
        - Class
        - Tags
      fee:
        paths:
          Amount: Fee
          Date: Date
          Ref Number: Batch Id
          External Id: Trans Id
        constants:
          Payee: Intuit Transactions
          Expense Account: Bank Fees
          Checking Account: Wells Fargo - Main 1582
        fields:
        - Payee
        - Date
        - Amount
        - Expense Account
        - Checking Account
        - Ref Number
        - External ID
        - Class
        - Tags
    """)

    digit_re = re.compile(r"[\d.]+")

    def gen_qbmarketplace_imports(self, rows, tag=None):

        collector = {'sale': [], 'fee': []}

        if not tag:
            tag = "qb_marketplace_" + arrow.utcnow().format("YYYYMMDD")

        # filter out empty rows
        for row in rows:

            if 'Amount' not in row:
                continue

            for k in ['Amount', 'Fee']:
                m = self.digit_re.search(row[k])
                row[k] = float(m.group())

            row['Date'] = row['Date'].split()[0]
            row.setdefault("Cardholder Name", "unknown")

            si_map = self.qb_import_map['sale']
            item = self._extract_path_map(row, si_map["paths"])
            item.update(si_map["constants"])
            collector['sale'].append(item)

            si_map = self.qb_import_map['fee']
            item = self._extract_path_map(row, si_map["paths"])
            item.update(si_map["constants"])
            collector['fee'].append(item)

        for k in collector:
            fname = f"qbmarketplace_{k}_{tag}.csv"
            LOG.info(f"writing {len(collector[k])} {k} to {fname}")
            fields = self.qb_import_map[k]['fields']
            with open(fname, "w") as f:
                writer = csv.DictWriter(f, fieldnames=fields)
                writer.writeheader()
                [writer.writerow(r) for r in collector[k]]

        # return collector

