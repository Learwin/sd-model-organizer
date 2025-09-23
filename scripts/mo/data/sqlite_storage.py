import os
import shutil
import sqlite3
import threading
from typing import List

from modules import shared

from scripts.mo.data.storage import Storage
from scripts.mo.environment import env, logger
from scripts.mo.models import Record, ModelType

_DB_FILE = 'database.sqlite'
_DB_VERSION = 8
_DB_TIMEOUT = 30


def map_row_to_record(row) -> Record:
    return Record(
        id_=row[0],
        name=row[1],
        model_type=ModelType.by_value(row[2]),
        download_url=row[3],
        url=row[4],
        download_path=row[5],
        download_filename=row[6],
        preview_url=row[7],
        description=row[8],
        positive_prompts=row[9],
        negative_prompts=row[10],
        sha256_hash=row[11],
        md5_hash=row[12],
        created_at=row[13],
        groups=row[14].split(',') if row[14] else [],
        subdir=row[15],
        location=row[16],
        weight=row[17],
        backup_url=row[18]
    )


class SQLiteStorage(Storage):

    def __init__(self):
        self.local = threading.local()
        self._initialize()

    def _database_path(self):
        mo_database_dir = getattr(shared.cmd_opts, "mo_database_dir")
        database_dir = mo_database_dir if mo_database_dir is not None else env.script_dir
        db_file_path = os.path.join(database_dir, _DB_FILE)
        return db_file_path

    def _connection(self):
        if not hasattr(self.local, "connection"):
            self.local.connection = sqlite3.connect(self._database_path(), _DB_TIMEOUT)
        return self.local.connection

    def _initialize(self):
        cursor = self._connection().cursor()

        cursor.execute('''CREATE TABLE IF NOT EXISTS Record
                                    (id INTEGER PRIMARY KEY,
                                    name TEXT,
                                    model_type TEXT,
                                    download_url TEXT,
                                    url TEXT DEFAULT '',
                                    download_path TEXT DEFAULT '',
                                    download_filename TEXT DEFAULT '',
                                    preview_url TEXT DEFAULT '',
                                    description TEXT DEFAULT '',
                                    positive_prompts TEXT DEFAULT '',
                                    negative_prompts TEXT DEFAULT '',
                                    sha256_hash TEXT DEFAULT '',
                                    md5_hash TEXT DEFAULT '',
                                    created_at INTEGER DEFAULT 0,
                                    groups TEXT DEFAULT '',
                                    subdir TEXT DEFAULT '',
                                    location TEXT DEFAULT '',
                                    weight REAL DEFAULT 1,
                                    backup_url TEXT)
                                 ''')
        
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Tag(
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE
            );
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS TagSet(
                record_id INTEGER REFERENCES Record(id) ON DELETE CASCADE,
                tag_id INTEGER REFERENCES Tag(id) ON DELETE CASCADE
            );
            """
        )        

        cursor.execute(f'''CREATE TABLE IF NOT EXISTS Version
                                (version INTEGER DEFAULT {_DB_VERSION})''')
        self._connection().commit()
        self._check_database_version()

    def _check_database_version(self):
        cursor = self._connection().cursor()
        cursor.execute('SELECT * FROM Version ', )
        row = cursor.fetchone()

        if row is None:
            cursor.execute(f'INSERT INTO Version VALUES ({_DB_VERSION})')
            self._connection().commit()

        version = _DB_VERSION if row is None else row[0]
        if version != _DB_VERSION:
            self._run_migration(version)

    def _run_migration(self, current_version):
        migration_map = {
            1: self._migrate_1_to_2,
            2: self._migrate_2_to_3,
            3: self._migrate_3_to_4,
            4: self._migrate_4_to_5,
            5: self._migrate_5_to_6,
            6: self._migrate_6_to_7,
            7: self._migrate_7_to_8,
        }
        for ver in range(current_version, _DB_VERSION):
            self._backup_database(ver)
            migration = migration_map.get(ver)
            if migration is None:
                raise Exception(f'Missing SQLite migration from {ver} to {_DB_VERSION}')
            migration()

    def _backup_database(self, migrate_from):
        db_file_path = self._database_path()
        backup_db_file_path = f'{db_file_path}.v{migrate_from}.bak'
        last_backup_db_file_path = f'{db_file_path}.v{migrate_from-1}.bak' if migrate_from > 1 else None
        if last_backup_db_file_path and os.path.isfile(last_backup_db_file_path):
            os.remove(last_backup_db_file_path)
            logger.info('Backup database v%s removed', migrate_from - 1)
        shutil.copy(db_file_path, backup_db_file_path)
        logger.info('Database v%s backup created', migrate_from)

    def _migrate_1_to_2(self):
        cursor = self._connection().cursor()
        cursor.execute('ALTER TABLE Record ADD COLUMN created_at INTEGER DEFAULT 0;')
        cursor.execute("DELETE FROM Version")
        cursor.execute('INSERT INTO Version VALUES (2)')
        self._connection().commit()

    def _migrate_2_to_3(self):
        cursor = self._connection().cursor()
        cursor.execute("ALTER TABLE Record ADD COLUMN groups TEXT DEFAULT '';")
        cursor.execute("DELETE FROM Version")
        cursor.execute('INSERT INTO Version VALUES (3)')
        self._connection().commit()

    def _migrate_3_to_4(self):
        cursor = self._connection().cursor()
        cursor.execute("ALTER TABLE Record RENAME COLUMN model_hash TO sha256_hash;")
        cursor.execute("ALTER TABLE Record ADD COLUMN subdir TEXT DEFAULT '';")
        cursor.execute("DELETE FROM Version")
        cursor.execute('INSERT INTO Version VALUES (4)')
        self._connection().commit()

    def _migrate_4_to_5(self):
        cursor = self._connection().cursor()
        cursor.execute("ALTER TABLE Record ADD COLUMN location TEXT DEFAULT '';")
        cursor.execute("DELETE FROM Version")
        cursor.execute('INSERT INTO Version VALUES (5)')
        self._connection().commit()

    def _migrate_5_to_6(self):
        cursor = self._connection().cursor()
        cursor.execute("ALTER TABLE Record ADD COLUMN weight REAL DEFAULT 1;")
        cursor.execute("DELETE FROM Version")
        cursor.execute('INSERT INTO Version VALUES (6)')
        self._connection().commit()

    def _migrate_6_to_7(self):
        cursor = self._connection().cursor()
        cursor.execute("ALTER TABLE Record ADD COLUMN backup_url TEXT DEFAULT '';")
        cursor.execute("DELETE FROM Version")
        cursor.execute('INSERT INTO Version VALUES (7)')
        self._connection().commit()

    def _migrate_7_to_8(self):
        cursor = self._connection().cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Tag(
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE
            );
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS TagSet(
                record_id INTEGER REFERENCES Record(id) ON DELETE CASCADE,
                tag_id INTEGER REFERENCES Tag(id) ON DELETE CASCADE
            );
            """
        )       

        cursor.execute("ALTER TABLE Record RENAME COLUMN _name TO name;") 
        cursor.execute("DELETE FROM Version")
        cursor.execute('INSERT INTO Version VALUES (8)')
        self._connection().commit()

    def get_all_records(self) -> List:
        cursor = self._connection().cursor()
        cursor.execute(
            """
            SELECT r.*, GROUP_CONCAT(t.name, ',') AS tags
            FROM Record r
            LEFT JOIN TagSet ts ON r.id = ts.record_id
            LEFT JOIN Tag t ON t.id = ts.tag_id
            GROUP BY r.id
            """)
        rows = cursor.fetchall()
        result = []
        for row in rows:
            result.append(map_row_to_record(row))
        return result

    def query_records(self, name_query: str = None, groups=None, model_types=None, show_downloaded=True,
                      show_not_downloaded=True) -> List:

        query = """
            SELECT r.*, GROUP_CONCAT(t.name, ',') AS tags
            FROM Record r
            LEFT JOIN TagSet ts ON r.id = ts.record_id
            LEFT JOIN Tag t ON t.id = ts.tag_id
            """

        is_where_appended = False
        append_and = False    

        if name_query is not None and name_query:
            if not is_where_appended:
                query += ' WHERE'
                is_where_appended = True

            query += f" LOWER(r.name) LIKE '%?%'"
            append_and = True

        if model_types is not None and len(model_types) > 0:
            if not is_where_appended:
                query += ' WHERE'
                is_where_appended = True

            if append_and:
                query += ' AND'

            query += ' ('
            query += ' OR '.join(["r.model_type=?" for _ in model_types])
            query += ')'

            append_and = True

        if groups is not None and len(groups) > 0:
            if not is_where_appended:
                query += ' WHERE'

            for idx, group in enumerate(groups):
                if append_and or idx > 0:
                    query += ' AND'
                query += " t.name LIKE %?%"
                append_and = True

        query += " GROUP BY r.id"
        logger.debug('query: %s',query)
        cursor = self._connection().cursor()
        params = []
        if name_query:
            params.append(f"%{name_query.lower()}%")
        if model_types:
            params.extend(model_types)
        if groups:
            params.extend([f"%{g.lower()}%" for g in groups])   

        cursor.execute(query, params)
        rows = cursor.fetchall()
        result = []
        for row in rows:
            record = map_row_to_record(row)
            is_downloaded = bool(record.location) and os.path.exists(record.location)

            if show_downloaded and is_downloaded:
                result.append(record)
            elif show_not_downloaded and not is_downloaded:
                result.append(record)

        return result

    def get_record_by_id(self, id_) -> Record:
        cursor = self._connection().cursor()
        cursor.execute(
            """
            SELECT r.*, GROUP_CONCAT(t.name, ',') AS tags 
            FROM Record r
            LEFT JOIN TagSet ts ON r.id = ts.record_id
            LEFT JOIN Tag t ON t.id = ts.tag_id
            WHERE r.id=?
            """, (id_,))
        row = cursor.fetchone()
        return None if row is None else map_row_to_record(row)

    def get_records_by_group(self, group: str) -> List:
        cursor = self._connection().cursor()
        cursor.execute(
            """
            SELECT r.*, GROUP_CONCAT(t.name, ',') AS tags 
            FROM Record r
            JOIN TagSet ts ON r.id = ts.record_id
            JOIN Tag t ON t.id = ts.tag_id
            WHERE t.name LIKE '%' || LOWER(?) || '%'
            group by r.id
            """, [group])
        rows = cursor.fetchall()
        result = []
        for row in rows:
            result.append(map_row_to_record(row))
        return result

    def get_records_by_query(self, query: str) -> List:
        cursor = self._connection().cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        result = []
        for row in rows:
            result.append(map_row_to_record(row))
        return result

    def add_record(self, record: Record) -> int:
        cursor = self._connection().cursor()
        data = (
            record.name,
            record.model_type.value,
            record.download_url,
            record.url,
            record.download_path,
            record.download_filename,
            record.preview_url,
            record.description,
            record.positive_prompts,
            record.negative_prompts,
            record.sha256_hash,
            record.md5_hash,
            record.created_at,
            ",".join(record.groups),
            record.subdir,
            record.location,
            record.weight,
            record.backup_url
        )
        cursor.execute(
            """INSERT INTO Record(
                    name,
                    model_type,
                    download_url,
                    url,
                    download_path,
                    download_filename,
                    preview_url,
                    description,
                    positive_prompts,
                    negative_prompts,
                    sha256_hash,
                    md5_hash,
                    created_at,
                    groups,
                    subdir,
                    location,
                    weight,
                    backup_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            data)
        self._connection().commit()
        return cursor.lastrowid

    def update_record(self, record: Record):
        cursor = self._connection().cursor()
        data = (
            record.name,
            record.model_type.value,
            record.download_url,
            record.url,
            record.download_path,
            record.download_filename,
            record.preview_url,
            record.description,
            record.positive_prompts,
            record.negative_prompts,
            record.sha256_hash,
            record.md5_hash,
            ",".join(record.groups),
            record.subdir,
            record.location,
            record.weight,
            record.backup_url,
            record.id_
        )
        cursor.execute(
            """UPDATE Record SET 
                    name=?,
                    model_type=?,
                    download_url=?,
                    url=?,
                    download_path=?,
                    download_filename=?,
                    preview_url=?,
                    description=?,
                    positive_prompts=?,
                    negative_prompts=?,
                    sha256_hash=?,
                    md5_hash=?,
                    groups=?,
                    subdir=?,
                    location=?,
                    weight=?,
                    backup_url=?
                WHERE id=?
            """, data
        )

        self._connection().commit()

    def remove_record(self, _id):
        cursor = self._connection().cursor()
        cursor.execute("DELETE FROM Record WHERE id=?", (_id,))
        self._connection().commit()

    def get_all_records_locations(self) -> List:
        cursor = self._connection().cursor()
        cursor.execute('SELECT location FROM Record')
        rows = cursor.fetchall()
        result = []
        for row in rows:
            if row[0]:
                result.append(row[0])

        return result

    def get_records_by_name(self, record_name) -> List:
        cursor = self._connection().cursor()
        cursor.execute(
            """
            SELECT r.*, GROUP_CONCAT(t.name, ',') AS tags 
            FROM Record r
            JOIN TagSet ts ON r.id = ts.record_id
            JOIN Tag t ON t.id = ts.tag_id
            WHERE r.name=?
            group by r.id
            """, (record_name,))
        rows = cursor.fetchall()
        result = []
        for row in rows:
            result.append(map_row_to_record(row))
        return result

    def get_records_by_url(self, url) -> List:
        cursor = self._connection().cursor()
        cursor.execute(
            """
            SELECT r.*, GROUP_CONCAT(t.name, ',') AS tags 
            FROM Record r
            JOIN TagSet ts ON r.id = ts.record_id
            JOIN Tag t ON t.id = ts.tag_id 
            WHERE r.url=?
            group by r.id
            """, (url,))
        rows = cursor.fetchall()
        result = []
        for row in rows:
            result.append(map_row_to_record(row))
        return result

    def get_records_by_download_destination(self, download_path, download_filename) -> List:
        cursor = self._connection().cursor()
        cursor.execute(
            """
            SELECT r.*, GROUP_CONCAT(t.name, ',') AS tags 
            FROM Record r
            JOIN TagSet ts ON r.id = ts.record_id
            JOIN Tag t ON t.id = ts.tag_id 
            WHERE r.download_path=? AND r.download_filename=?
            group by r.id
            """,
                       (download_path, download_filename,))
        rows = cursor.fetchall()
        result = []
        for row in rows:
            result.append(map_row_to_record(row))
        return result

    def get_all_tags(self) -> List:
        cursor = self._connection().cursor()
        cursor.execute("SELECT name from Tag")
        rows = cursor.fetchall()
        result = [row[0].lower() for row in rows if row[0]]
        return list(set(result))

    def add_tag(self, tag) -> int:
        tag = tag.lower()
        cursor = self._connection().cursor()
        cursor.execute(
            """
            INSERT INTO Tag(name) VALUES(?)
            """,
            [tag],
        )
        self._connection().commit()
        return cursor.lastrowid

    def remove_tag(self, tag_name: str, tag_id: int = None):
        tag_name = tag_name.lower() if tag_name else None
        cursor = self._connection().cursor()
        if tag_id is None:
            cursor.execute("""DELETE FROM Tag WHERE name=?""", [tag_name])
        else:
            cursor.execute("""DELETE FROM Tag WHERE id=?""", [tag_id])
        self._connection().commit()


    def get_tags_for_record(self, record_id) -> List:
        cursor = self._connection().cursor()
        cursor.execute(
            """
            SELECT t.name
            FROM Tag t
            JOIN TagSet ts ON t.id = ts.tag_id
            WHERE ts.record_id = ?
            """,
            [record_id],
        )
        rows = cursor.fetchall()
        return [row[0].lower() for row in rows if row[0]]

    def set_tags_for_record(self, record_id, tag_list):
        tag_list = [t.lower() for t in tag_list if t]
        
        cursor = self._connection().cursor()
        current_tags_names = set(self.get_tags_for_record(record_id))
        new_tags = set(tag_list) - current_tags_names
        existing_tags = set(tag_list) & current_tags_names

        tag_name_to_id = {}
        for tag in new_tags:
            tag_id = self.add_tag(tag)
            tag_name_to_id[tag] = tag_id

        if existing_tags:
            cursor.execute(
                "SELECT id, name FROM Tag WHERE name IN ({seq})".format(
                    seq=",".join("?" * len(existing_tags))
                ),
                list(existing_tags),
            )
            for row in cursor.fetchall():
                tag_name_to_id[row[1]] = row[0]

        cursor.execute("DELETE FROM TagSet WHERE record_id = ?", [record_id])

        for tag_name, tag_id in tag_name_to_id.items():
            cursor.execute(
                "INSERT INTO TagSet(record_id, tag_id) VALUES(?, ?)",
                [record_id, tag_id],
            )
        self._connection().commit()
        cursor.execute(
            "DELETE FROM Tag WHERE id NOT IN (SELECT DISTINCT tag_id FROM TagSet)"
        )
        self._connection().commit()