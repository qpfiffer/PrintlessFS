from fuse import FUSE, FuseOSError, Operations
from time import time
from stat import S_IFDIR, S_IFLNK, S_IFREG
import rethinkdb as r
import sys

META_DB_NAME = "printlessfs"
META_TABLE_PREFIX = "pfs_fs"

class PrintlessFS(Operations):

    def __init__(self, mountpoint):
        self.root = mountpoint
        self.connection = r.connect("localhost", 28015)
        if META_DB_NAME not in r.db_list().run(self.connection):
            r.db_create(META_DB_NAME).run(self.connection)

        self._create_tables()

    def _create_tables(self):
        for table_name in [self._file_md_tname(), self._file_ctent_tname()]:
            if table_name not in r.db(META_DB_NAME).table_list().run(self.connection):
                r.db(META_DB_NAME).table_create(table_name).run(self.connection)

    # Table for file metadata
    def _file_md_tname(self):
        return "{}_MD_{}".format(META_TABLE_PREFIX, self.root.replace("/", "_"))

    # Table for file content (binary blobs)
    def _file_ctent_tname(self):
        return "{}_CTNT_{}".format(META_TABLE_PREFIX, self.root.replace("/", "_"))

    def _global_fd_name(self):
        return "{}_GFD_COUNT".format(META_TABLE_PREFIX)

    """
    Helpers for querying.
    """
    @property
    def metadata_table(self):
        return r.db(META_DB_NAME).table(self._file_md_tname())

    @property
    def content_table(self):
        return r.db(META_DB_NAME).table(self._file_ctent_tname())

    def increment_fd(self):
        return self.metadata_table.get(self._global_fd_name())\
                   .update(r.row[self._global_fd_name()] + 1, return_changes=True)\
                   .run(self.connection)

    def decrement_fd(self):
        return self.metadata_table.get(self._global_fd_name())\
                   .update(r.row[self._global_fd_name()] - 1, return_changes=True)\
                   .run(self.connection)

    def fname(self, path):
        return path.replace("/", "_")

    """
    FUSE stuff.
    """
    def open(self, path, flags):
        existing = self.metadata_table.get(path).run(self.connection)
        if not existing:
            return -1
        return self.increment_fd()

    def create(self, path, mode, fi=None):
        existing = self.metadata_table.get(path).run(self.connection)
        if not existing:
            data = {
                    "path": self.fname(path),
                    "posix": {
                        "st_mode":  (S_IFREG | mode),
                        "st_nlink": 1,
                        "st_size":  0,
                        "st_ctime": time(),
                        "st_mtime": time(),
                        "st_atime": time()
                        }
                    }
            self.metadata_table.insert(data).run(self.connection)
        return self.increment_fd()

    def release(self, path, fh):
        return self.decrement_fd()

    ##def flush(self, path, fh):
    ##    return os.fsync(fh)

    ##def read(self, path, length, offset, fh):
    ##    os.lseek(fh, offset, os.SEEK_SET)
    ##    return os.read(fh, length)

    ##def write(self, path, buf, offset, fh):
    ##    os.lseek(fh, offset, os.SEEK_SET)
    ##    return os.write(fh, buf)

    ##def truncate(self, path, length, fh=None):
    ##    full_path = self._full_path(path)
    ##    with open(full_path, 'r+') as f:
    ##        f.truncate(length)

    ##def fsync(self, path, fdatasync, fh):
    ##    return self.flush(path, fh)

def main():
    mountpoint = sys.argv[1]
    FUSE(PrintlessFS(mountpoint), mountpoint, foreground=True)

if __name__ == '__main__':
    main()
