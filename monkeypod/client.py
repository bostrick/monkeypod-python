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
import base64
import functools
import urllib.parse

import attr
import json
import yaml
import requests
#import arrow
#from pydash import py_

LOG = logging.getLogger(__name__)

@attr.s
class MonkeyPodClient:

    api = attr.ib(default=os.environ.get("MONKEYPOD_API"))
    token = attr.ib(default=os.environ.get("MONKEYPOD_TOKEN"), repr=False)

    def vet_response(self, resp):
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:  # pragma: nocover
            LOG.error("response error: %s" % e)
            LOG.error(resp.content)
            raise

    @property
    def std_headers(self):
        return {
            'Content-type': "application/json",
            'Accept': "application/json",
        }

    @functools.cached_property
    def session(self):
        sess = requests.session()
        sess.headers["Authorization"] = "Bearer %s" % self.token
        sess.headers.update(self.std_headers)
        return sess

    def _dump_response(self, resp):
        print("request")
        print("%s: %s" % (resp.request.method, resp.request.url))
        print(yaml.safe_dump(dict(resp.request.headers)))

        print("repsonse")
        print(resp.status_code)
        print(yaml.safe_dump(dict(resp.headers)))

    def _u(self, path):
        return self.api + path

#    def _object_iter(self, url, type=None):
#        starting_after = ""
#        params = {'limit': 100}
#        while True:
#            resp = self.session.get(url, params=params)
#            resp.raise_for_status()
#            payload = resp.json()
#            for d in payload['data']:
#                d['doc_id'] = d['id']
#                dtype = d.get('object')
#                if 'created' in d:
#                    d['creation_time'] = str(arrow.get(d['created']))
#                if dtype:
#                    d['doc_type'] = "stripe_%s" % dtype
#                    mgr = get_zdoc_manager(d['doc_type'])
#                    if mgr:
#                        mgr.create(**d)
#                yield d
#            if not payload.get("has_more"):
#                return 
#            last_id = py_.get(payload, 'data.-1.id')
#            params['starting_after'] = last_id

#    def entities(self):
#        #resp =  self.session.get(self.u('entities')
#        #with open("/tmp/foo.html", "wb") as f:
#        #    f.write(resp.content)
#        resp.raise_for_status()
#        return resp.json()

    def entity_match(self, id=None, email=None, name=None, metadata=None):
        q = {}
        if id:
            q['id'] = id
        if email:
            q['email'] = email
        if name:
            q['name'] = name
        if metadata:
            q['metadata'] = metadata
        qstr = urllib.parse.urlencode(q)
        response =  self.session.get(self._u(f"entities/match?{qstr}"))
        response.raise_for_status()
        return response.json()

    def _get_unique_item(self, items):

        if not items:
            raise KeyError("item not found")

        if len(items) > 1:
            LOG.warning("multiple items found")
            LOG.warning(yaml.safe_dump(items))
            raise ValueError("multiple items found")

        return items[0]

    def _resolve_unique_match(self, email=None):
        data = self.entity_match(email=email)
        return self._get_unique_item(data.get("data", []))

    def entity_create(self, data):
        response =  self.session.post(self._u("entities"), json=data)
        response.raise_for_status()
        return response.json()

    def entity_delete(self, id=None, email=None):

        if email and not id:
            entity = self._resolve_unique_match(email=email)
            id = entity['id']

        response =  self.session.delete(self._u(f"entities/{id}"))
        response.raise_for_status()
        return {}



# vi: ts=4 expandtab
