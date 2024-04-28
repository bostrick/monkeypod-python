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
from pathlib import Path

import attr
import arrow
import yaml
from pydash import py_

LOG = logging.getLogger(__name__)

HERE = Path(__file__)


@attr.s
class MonkeyPodManager:

    client = attr.ib()

#    import_path = HERE / "data/imports/monkey_pod_columns.yaml"
#
#    @cached_property
#    def import_column_map(self):
#        return yaml.safe_load(self.import_path.read_text())

    def _extract_data(self, item, attr_map):
        # forces all entries to strings
        ndata = {
            v: str(item.get(k, "")).strip()
            for k, v in attr_map.items()
        }
        ndata = {k: v for k, v in ndata.items() if v}
        return ndata

#    def import_entities(
#        self,
#        entity_list,
#        attr_map=None,
#        source_attr=None,
#        import_attr=None,
#    ):
#        """Import a list of entities.
#
#        Skip the entitiy if the email already exists.
#
#        'source_attr' and 'import_attr' get added as 'source' and 'import'
#        extra attributes.
#
#        attr_map can be used to reshape attrs from another input shape.
#        """
#
#        nadded = 0
#        for entity in entity_list:
#
#            if attr_map:
#                entity = self._extract_data(entity, attr_map)
#
#            email = entity.get('email')
#            name = entity.get('name')
#            # TODO... merge additional attributes into entity
#            if email:
#                match = self.client.entity_match(email=email)
#                if match:
#                    LOG.info(f"skipping {email}: exists")
#                    continue
#            elif name:
#                if name:
#                    match = self.client.entity_match(name=name)
#                    if match:
#                        LOG.info(f"skipping {name}: exists")
#                        continue
#            else:
#                LOG.debug("skipping no name or email")
#                continue
#
#            entity.setdefault("type", "Individual")
#
#            extra = entity.setdefault("extra_attributes", {})
#            if source_attr:
#                extra.setdefault("source", source_attr)
#            if import_attr:
#                extra.setdefault("import", import_attr)
#
#            roles = set(entity.setdefault("roles", []))
#            roles.add("Donor")
#            roles.add("Customer")
#            entity['roles'] = list(roles)
#
#            LOG.info(f"creating entity {email} {name}")
#            LOG.info(os.linesep+yaml.safe_dump(entity))
#            yesno = input("import entity? (y/N)").strip().lower()
#            if yesno and yesno[0] == 'y':
#                _ = self.client.entity_create(entity)
#                nadded += 1
#
#        return nadded

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

#    def _unknown_entities_itr(self, stripe_items):
#        for item in stripe_items:
#            mp_entity = self._extract_stripe_entity(item)
#            if not self._mp_entity_exists(mp_entity):
#                yield self._normalize_mp_entity(mp_entity)

    def _extract_mp_entity_from_stripe_tx(self, stripe_tx):
        entity = self._extract_path_map(
            stripe_tx, self.stripe_mp_entity_map,
        )
        entity = {k: v.strip() for k, v in entity.items() if v}
        entity = {k: v for k, v in entity.items() if v}
        return entity

    def _mp_entity_exists(self, entity):

        email = entity.get('email')
        if email:
            match = self.client.entity_match(email=email)
            if match:
                LOG.info(f"mp {email} exists")
                return True

        name = entity.get('name')
        if name:
            match = self.client.entity_match(name=name)
            if match:
                LOG.info(f"mp {name} exists")
                return True

        return False

    def _normalize_mp_entity(self, entity):

        entity.setdefault("type", "Individual")

        # extra = entity.setdefault("extra_attributes", {})

        roles = set(entity.setdefault("roles", []))
        roles.add("Donor")
        roles.add("Customer")
        entity['roles'] = list(roles)

        return entity

#    def _confirm_stripe_entity(
#        self, item, source_attr=None, import_attr=None
#    ):
#        entity = self._extract_stipe_entity(item)
#        self.import_entities(
#            [entity],
#            source_attr=source_attr,
#            import_attr=import_attr,
#        )
#        return entity

    def _extract_path_map(self, src, path_map):
        dst = {}
        for k, v in path_map.items():
            if py_.has(src, v):
                py_.set(dst, k, py_.get(src, v))
        return dst

    #####################################################################
    # stripe imports
    #####################################################################

    stripe_import_map = yaml.safe_load("""
      relationship:
        paths:
          First Name: name      # needs postprocessing
          Last Name: name       # needs postprocessing
          Address1: address
          City: city
          State: state
          Zip: postal_code
          Country: country
          Email: email
        constants:
          Type: Individual
          Source: stripe
          Roles: Donor,Customer
        fields:
        - Type
        - Email
        - First Name
        - Last Name
        - Address1
        - City
        - State
        - Zip
        - Country
        - Phone
        - Roles
        - Source
        - Import
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
        self, stripe_transactions, tag=None, confirm_entities=False,
    ):

        tag = tag or "stripe_" + arrow.utcnow().format("YYYYMMDD")
        collector = collections.defaultdict(list)
        fee_collector = collections.defaultdict(lambda: 0.0)
        n_entities = n_new_entities = 0

        for stripe_tx in stripe_transactions:

            mp_entity = self._extract_mp_entity_from_stripe_tx(stripe_tx)
            if mp_entity:
                n_entities += 1
                if not (
                    confirm_entities and self._mp_entity_exists(mp_entity)
                ):
                    _, r_row = self._generate_relationship(mp_entity)
                    r_row["Import"] = tag
                    collector['relationship'].append(r_row)
                    n_new_entities += 1

            what, row = self._stripe_generate_import_row(stripe_tx)
            collector[what].append(row)
            self._collect_fee(fee_collector, stripe_tx)

        data = dict(collector)
        data["fee"] = self._reduce_fees(fee_collector)

        LOG.info(f"import {n_new_entities} out of {n_entities} entities")

        # reshape data map to include data and fields
        _sim = self.stripe_import_map
        return {
            k: {'rows': rows, 'fields': _sim[k]['fields']}
            for k, rows in data.items()
        }

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
                _write_csv(k, tag, data[k]['fields'], data[k]['rows'])

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

    def _generate_relationship(self, record):
        name_tokens = record.get("name", "").split()
        dst = self._generate_base("relationship", record)
        dst["First Name"] = " ".join(name_tokens[:-1])
        dst["Last Name"] = name_tokens[-1]
        return "relationship", dst

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

