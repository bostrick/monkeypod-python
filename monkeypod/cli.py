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

import click
import yaml

from .client import MonkeyPodClient

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
    kw = {k:v for k,v in kw.items() if v}
    ctx.client = MonkeyPodClient(**kw)
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
    kw = {k:v for k,v in kw.items() if v}
    c = ctx.parent.parent.client
    click.echo(yaml.safe_dump(c.entity_match(**kw)))

@entity.command(name="delete")
@click.option("-i", "--id")
@click.option("-e", "--email")
@click.pass_context
def entity_match(ctx, id, email):
    """Delete an entry by id or matching email"""
    assert id or email, "either id or email is required"
    assert not (id and email), "only one of id or email is accepted"
    c = ctx.parent.parent.client
    click.echo(yaml.safe_dump(c.entity_delete(id=id, email=email)))

