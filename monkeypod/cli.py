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
"""
"""
__author__ = 'Bowe Strickland <bowe@yak.net>'
__docformat__ = 'restructuredtext'

import logging
import csv

import click
import yaml

from .client import MonkeyPodClient
from .manager import MonkeyPodManager
from .stripe_client import StripeClient

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@click.group()
@click.option(
    "-a", "--api",
    help="URL in form https://[institution].monkeypod.io/api/v2"
)
@click.option(
    "-t", "--token",
    help="Authorization token"
)
@click.pass_context
def monkeypod(ctx, **kw):
    """MonkeyPod command line client

    Recognized environment variables:
        MONKEYPOD_API="https://[institution].monkeypod.io/api/v2/"
        MONKEYPOD_TOKEN="..."
    """
    kw = {k: v for k, v in kw.items() if v}
    ctx.client = MonkeyPodClient(**kw)
    ctx.manager = MonkeyPodManager(ctx.client)
    LOG.info("using %s" % ctx.client)


@monkeypod.group(name="entity")
@click.pass_context
def entity(ctx):
    """Manage Entities"""
    pass


@entity.command(name="create")
@click.option("-f", "--yaml-filename", type=click.File(), required=True)
@click.pass_context
def entity_create(ctx, yaml_filename):
    """Create entity by specifing .yaml formattted file"""
    data = yaml.safe_load(yaml_filename.read())
    c = ctx.parent.parent.client
    click.echo(yaml.safe_dump(c.entity_create(data)))


@entity.command(name="match")
@click.option("-i", "--id")
@click.option("-n", "--name")
@click.option("-e", "--email")
@click.option("-m", "--metadata")
@click.pass_context
def entity_match(ctx, **kw):
    """Search for matching entities"""
    kw = {k: v for k, v in kw.items() if v}
    c = ctx.parent.parent.client
    click.echo(yaml.safe_dump(c.entity_match(**kw)))


@entity.command(name="delete")
@click.option("-i", "--id")
@click.option("-e", "--email")
@click.pass_context
def entitydelete(ctx, id, email):
    """Delete an entry by id or matching email"""
    assert id or email, "either id or email is required"
    assert not (id and email), "only one of id or email is accepted"
    c = ctx.parent.parent.client
    click.echo(yaml.safe_dump(c.entity_delete(id=id, email=email)))

#######################################################################
# imports
#######################################################################


def _ingest_csv_file(fd):

    def _filter_row(data):
        return {k: v for k, v in data.items() if v}

    reader = csv.DictReader(fd)
    return [_filter_row(r) for r in reader]


@entity.command(name="import-csv")
@click.option("-f", "--csv-filename", type=click.File())
@click.option("-h", "--headers-yaml", type=click.File())
@click.option("-s", "--source-attr")
@click.option("-i", "--import-attr")
@click.pass_context
def import_csv(ctx, csv_filename, headers_yaml, source_attr, import_attr):

    entities = _ingest_csv_file(csv_filename)

    attr_map = None
    if headers_yaml:
        attr_map = yaml.safe_load(headers_yaml.read())

    mgr = ctx.parent.parent.manager
    result = mgr.import_entities(
        entities,
        attr_map,
        source_attr,
        import_attr,
    )
    click.echo(yaml.safe_dump(result))


@monkeypod.group(name="transaction")
@click.pass_context
def transaction(ctx):
    """Manage Transactions"""
    pass


@transaction.command(name="import-stripe-transactions")
@click.option("-f", "--csv-filename", type=click.File())
@click.option("-y", "--yaml-filename", type=click.File())
@click.option("-c", "--confirm-entities", is_flag=True)
@click.pass_context
def import_stripe_transactions(
    ctx, csv_filename, yaml_filename, confirm_entities
):

    if csv_filename:
        rows = _ingest_csv_file(csv_filename)
        mgr = ctx.parent.parent.manager
        result = mgr.gen_stripe_imports(rows)
        click.echo(yaml.safe_dump(result))

    elif yaml_filename:
        recs = yaml.safe_load_all(yaml_filename.read())
        mgr = ctx.parent.parent.manager
        result = mgr.gen_stripe_imports_from_recs(
            recs, confirm_entities=confirm_entities,
        )


@monkeypod.group(name="stripe")
@click.pass_context
def stripe(ctx):
    ctx.stripe = StripeClient()


@stripe.command(name="customers")
@click.option("-w", "--when", default="now-1M/M:now-1M/M")
@click.pass_context
def stripecustomers(ctx, when):
    c = ctx.parent.stripe
    for obj in c.customer_iter(when):
        print("---\n" + yaml.safe_dump(obj))


@stripe.command(name="transactions")
@click.option("-w", "--when", default="now-1M/M:now-1M/M")
@click.pass_context
def stripe_transactions(ctx, when):
    c = ctx.parent.stripe
    for obj in c.balance_transaction_iter(when):
        print("---\n" + yaml.safe_dump(obj))


@stripe.group(name="charge")
@click.pass_context
def stripe_charge(ctx):
    pass


@stripe_charge.command(name="get")
@click.argument("charge_id")
@click.pass_context
def stripe_charge(ctx, charge_id):
    c = ctx.parent.parent.stripe
    print(yaml.safe_dump(c.get_charge(charge_id)))
