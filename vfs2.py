#!/usr/bin/env python3

from dataclasses import dataclass
from pathlib import Path
from sys import argv, exit
from zlib import compress as zcompress, decompress as zdecompress


VFS_COMPRESSION_LEVEL = 9 # 0-9


@dataclass
class VfsDir:
    hash: int
    id: int
    parent: int
    from_dir: int
    from_file: int

@dataclass
class VfsFile:
    hash: int
    id: int
    type: int
    dir: int
    offset: int
    size: int


def read_magic(vfs):
    return vfs.read(4) == b'VFS2'

def write_magic(vfs):
    vfs.write(b'VFS2')

def read_int(vfs, signed=False):
    return int.from_bytes(vfs.read(4), 'little', signed=signed)

def write_int(vfs, value, signed=False):
    return vfs.write(value.to_bytes(4, 'little', signed=signed))

def read_str(vfs):
    size = read_int(vfs)
    return vfs.read(size).decode('ascii')

def write_str(vfs, s):
    write_int(vfs, len(s))
    vfs.write(s.encode('ascii'))

def read_dir(vfs):
    hash = read_int(vfs)
    id = read_int(vfs)
    parent = read_int(vfs, signed=True)
    from_dir = read_int(vfs, signed=True)
    from_file = read_int(vfs)
    return VfsDir(hash, id, parent, from_dir, from_file)

def write_dir(vfs, dir):
    write_int(vfs, dir.hash)
    write_int(vfs, dir.id)
    write_int(vfs, dir.parent, signed=True)
    write_int(vfs, dir.from_dir, signed=True)
    write_int(vfs, dir.from_file)

def read_file(vfs):
    hash = read_int(vfs)
    id = read_int(vfs)
    type = read_int(vfs)
    dir = read_int(vfs)
    offset = read_int(vfs)
    size = read_int(vfs)
    return VfsFile(hash, id, type, dir, offset, size)

def write_file(vfs, file):
    write_int(vfs, file.hash)
    write_int(vfs, file.id)
    write_int(vfs, file.type)
    write_int(vfs, file.dir)
    write_int(vfs, file.offset)
    write_int(vfs, file.size)

def read_vfs(vfs):
    if not read_magic(vfs):
        raise ValueError('Bad magic number for vfs header')
    
    dirs = []
    files = []
    fnames = []
    dnames = []

    for _ in range(read_int(vfs)):
        dirs.append(read_dir(vfs))
    for _ in range(read_int(vfs)):
        files.append(read_file(vfs))
    blob = vfs.read(read_int(vfs) - vfs.tell())
    for i in range(read_int(vfs)):
        fnames.append(read_str(vfs))
    for i in range(read_int(vfs)):
        dnames.append(read_str(vfs))

    return dirs, files, blob, fnames, dnames

def write_vfs(vfs, dirs, files, blob, fnames, dnames):
    write_magic(vfs)
    write_int(vfs, len(dirs))
    for d in dirs:
        write_dir(vfs, d)
    write_int(vfs, len(files))
    for f in files:
        write_file(vfs, f)
    write_int(vfs, len(blob) + vfs.tell() + 4)
    vfs.write(blob)
    write_int(vfs, len(files))
    for n in fnames:
        write_str(vfs, n)
    write_int(vfs, len(dirs))
    for n in dnames:
        write_str(vfs, n)

def build_path(dirs, dnames, dir):
    if dir.parent == -1:
        return Path(dnames[dir.id])
    return build_path(dirs, dnames, dirs[dir.parent]) / dnames[dir.id]

def decompress(vfs, prefix):
    with open(vfs, 'rb') as vfs:
        dirs, files, blob, fnames, dnames = read_vfs(vfs)

    for file in files:
        path = build_path(dirs, dnames, dirs[file.dir])
        data = blob[file.offset:file.offset + file.size]
        if file.type:
            bufsize = int.from_bytes(data[0:4], byteorder='little')
            data = zdecompress(data[4:], bufsize=bufsize)
        prefix.joinpath(path).mkdir(parents=True, exist_ok=True)
        prefix.joinpath(path / fnames[file.id]).write_bytes(data)

def compress(ivfs, prefix, ovfs):
    with open(ivfs, 'rb') as ivfs:
        idirs, ifiles, iblob, ifnames, idnames = read_vfs(ivfs)

    odirs = idirs.copy()
    ofiles = ifiles.copy()
    oblob = b''
    ofnames = ifnames.copy()
    odnames = idnames.copy()

    offset = 0
    for file in sorted(ofiles, key=lambda f: f.id):
        path = build_path(odirs, odnames, odirs[file.dir]) / ofnames[file.id]
        data = prefix.joinpath(path).read_bytes()
        if file.type == 2:
            bufsize = len(data).to_bytes(4, 'little')
            data = bufsize + zcompress(data, VFS_COMPRESSION_LEVEL)
        size = len(data)
        file.size = size
        file.offset = offset
        offset += size
        oblob += data

    with open(ovfs, 'wb') as ovfs:
        write_vfs(ovfs, odirs, ofiles, oblob, ofnames, odnames)


if __name__ == '__main__':
    if len(argv) < 2:
        print('usage:')
        print('python3 vfs2.py d data.vfs outdir')
        print('python3 vfs2.py c data.vfs outdir repacked.vfs')
        exit(1)

    if argv[1] == 'c':
        ivfs = argv[2]
        prefix = Path(argv[3])
        ovfs = argv[4]
        compress(ivfs, prefix, ovfs)
    elif argv[1] == 'd':
        vfs = argv[2]
        prefix = Path(argv[3])
        decompress(vfs, prefix)