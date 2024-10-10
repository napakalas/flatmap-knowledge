#===============================================================================
#
#  Flatmap viewer and annotation tools
#
#  Copyright (c) 2019-24  David Brooks
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#===============================================================================

from pathlib import Path
import sqlite3

#===============================================================================

import mapknowledge
from mapknowledge.scicrunch import SCICRUNCH_PRODUCTION, SCICRUNCH_STAGING

#===============================================================================

__version__ = "1.9.4"

#===============================================================================

KNOWLEDGE_BASE = 'knowledgebase.db'

#===============================================================================

FLATMAP_SCHEMA = """
    -- will auto convert datetime.datetime objects
    create table flatmaps(id text primary key, models text, created datetime, knowledge_source text);
    create unique index flatmaps_index on flatmaps(id);
    create index flatmaps_models_index on flatmaps(models);

    create table flatmap_entities (flatmap text, entity text);
    create index flatmap_entities_flatmap_index on flatmap_entities(flatmap);
    create index flatmap_entities_entity_index on flatmap_entities(entity);
"""

## Need to manually run when upgrading:
"""
alter table flatmaps add knowledge_source text;
alter table flatmaps add column dt datetime;
update flatmaps set dt = created;
alter table flatmaps drop column created;
alter table flatmaps rename column dt to created;
"""

#===============================================================================

class KnowledgeStore(mapknowledge.KnowledgeStore):
    def __init__(self, store_directory, knowledge_base=KNOWLEDGE_BASE, create=True, read_only=False,
        clean_connectivity=False, sckan_version=None, **kwds):
        new_db = not Path(store_directory, knowledge_base).resolve().exists()
        scicrunch_version = kwds.pop('scicrunch_version', 'production')
        scicrunch_version = SCICRUNCH_PRODUCTION if scicrunch_version=='production' else SCICRUNCH_STAGING
        if create and new_db:
            super().__init__(store_directory,
                             knowledge_base=knowledge_base,
                             create=create,
                             read_only=False,
                             clean_connectivity=clean_connectivity,
                             scicrunch_version=scicrunch_version,
                             sckan_version=sckan_version,
                             **kwds)
            if self.db is not None:
                self.db.executescript(FLATMAP_SCHEMA)
                self.db.commit()
                if read_only:
                    super().open(read_only=True)
        else:
            super().__init__(store_directory,
                             knowledge_base=knowledge_base,
                             create=create,
                             read_only=read_only,
                             clean_connectivity=clean_connectivity,
                             scicrunch_version=scicrunch_version,
                             sckan_version=sckan_version,
                             **kwds)

    def add_flatmap(self, flatmap, knowledge_source=None):
    #=====================================================
        if self.db is not None:
            self.db.execute('replace into flatmaps(id, models, created, knowledge_source) values (?, ?, ?, ?)',
                (flatmap.uuid, flatmap.models, flatmap.created, knowledge_source))
            self.db.execute('delete from flatmap_entities where flatmap=?', (flatmap.uuid, ))
            self.db.executemany('insert into flatmap_entities(flatmap, entity) values (?, ?)',
                ((flatmap.uuid, entity) for entity in flatmap.entities))
            self.db.commit()

    def flatmap_entities(self, flatmap):
    #===================================
        if self.db is not None:
            select = ['select distinct entity from flatmap_entities']
            if flatmap is not None:
                select.append('where flatmap=?')
            select.append('order by entity')
            if flatmap is not None:
                return [row[0] for row in self.db.execute(' '.join(select), (flatmap.uuid,))]
            else:
                return [row[0] for row in self.db.execute(' '.join(select))]

#===============================================================================
