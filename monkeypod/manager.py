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

import attr

LOG = logging.getLogger(__name__)


@attr.s
class MonkeyPodManager:

    client = attr.ib()

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
