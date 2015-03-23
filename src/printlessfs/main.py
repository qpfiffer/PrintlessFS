from fuse import FUSE, FuseOSError, Operations
import rethinkdb as r
import sys

META_DB_NAME = "printlessfs"
META_TABLE_PREFIX = "pfs_fs"

class PrintlessFS(Operations):

    def __init__(self, root):
        self.root = root
        self.connection = r.connect("localhost", 28015)
        if META_DB_NAME not in r.db_list().run(self.connection):
            r.db_create(META_DB_NAME).run(self.connection)

        self._create_tables()

    def _create_tables(self):
        for table_name in [self._file_md_table_name(), self._file_ctent_table_name()]:
            if table_name not in r.db(META_DB_NAME).table_list().run(self.connection):
                r.db(META_DB_NAME).table_create(table_name).run(self.connection)

    # Table for file metadata
    def _file_md_table_name(self):
        return "{}_MD_{}".format(META_TABLE_PREFIX, self.root.replace("/", "_"))

    # Table for file content (binary blobs)
    def _file_ctent_table_name(self):
        return "{}_CTNT_{}".format(META_TABLE_PREFIX, self.root.replace("/", "_"))

    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path

    def open(self, path, flags):
        full_path = self._full_path(path)
        return os.open(full_path, flags)

    def create(self, path, mode, fi=None):
        full_path = self._full_path(path)
        return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

    def write(self, path, buf, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)

    def truncate(self, path, length, fh=None):
        full_path = self._full_path(path)
        with open(full_path, 'r+') as f:
            f.truncate(length)

    def flush(self, path, fh):
        return os.fsync(fh)

    def release(self, path, fh):
        return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        return self.flush(path, fh)

def main():
    mountpoint, root = sys.argv[2], sys.argv[1]
    FUSE(PrintlessFS(root), mountpoint, foreground=True)

if __name__ == '__main__':
    main()
