"""
Convert a Distant Horizons database (DataFormatVersion 2) into DataFormatVersion 1,
so Voxy's `/voxy import distant_horizons` can read it.

Usage:
    python dh_v2_to_v1.py <input DistantHorizons.sqlite> [output.sqlite]

The input file is never modified. By default the output is written next to it as
DistantHorizons_v1.sqlite. Requires: pip install zstandard

Format details mirror distant-horizons-core FullDataSourceV2DTO:
  v1 Data blob: for each of 64*64 columns (index = x*64 + z): int16 count, then count int64 datapoints.
  v2 Data blob: interior columns (x,z in 1..62) as 5 streams (counts, id+flags, heights,
                bottomY corrections, packed light); the edge columns live in the
                North/South/East/West AdjData blobs using the same encoding.
  datapoint long: id bits 0-31, height 32-43, bottomY 44-55, sky light 56-59, block light 60-63.
"""
import os
import sqlite3
import struct
import sys
import time
from array import array
from multiprocessing import Pool

import zstandard

WIDTH = 64
UNCOMPRESSED, LZ4, ZSTD_STREAM, LZMA2, ZSTD_BLOCK = 0, 1, 2, 3, 4

# (minX, maxX, minZ, maxZ), end-exclusive, from FullDataMinMaxPosUtil
INTERIOR = (1, WIDTH - 1, 1, WIDTH - 1)
ADJ_BOUNDS = {
    "NorthAdjData": (0, WIDTH, 0, 1),
    "SouthAdjData": (0, WIDTH, WIDTH - 1, WIDTH),
    "EastAdjData": (WIDTH - 1, WIDTH, 0, WIDTH),
    "WestAdjData": (0, 1, 0, WIDTH),
}

_zd = zstandard.ZstdDecompressor()
_zc = zstandard.ZstdCompressor(level=3, write_content_size=True)


def decompress(blob, mode):
    if blob is None:
        return b""
    if mode == ZSTD_BLOCK:
        return _zd.decompress(blob) if blob else b""
    if mode == UNCOMPRESSED:
        return bytes(blob)
    if mode == LZMA2:
        import lzma
        return lzma.decompress(blob)
    raise ValueError(f"unsupported compression mode {mode}")


def compress(raw, mode):
    if mode == ZSTD_BLOCK:
        return _zc.compress(raw)
    if mode == LZMA2:
        import lzma
        return lzma.compress(raw, format=lzma.FORMAT_XZ, check=lzma.CHECK_CRC64,
                             filters=[{"id": lzma.FILTER_LZMA2, "preset": 3}])
    if mode == UNCOMPRESSED:
        return raw
    raise ValueError(f"unsupported compression mode {mode}")


class Reader:
    __slots__ = ("b", "p")

    def __init__(self, b):
        self.b = b
        self.p = 0

    def byte(self):
        v = self.b[self.p]
        self.p += 1
        return v

    def varint(self):
        value = shift = 0
        while True:
            if shift >= 32:
                raise ValueError("invalid varint")
            b = self.b[self.p]
            self.p += 1
            value |= (b & 127) << shift
            shift += 7
            if not b & 128:
                return value


def zigzag_decode(n):
    return (n >> 1) ^ -(n & 1)


def read_v2_region(raw, bounds, columns):
    """Decode one v2 data stream into columns[x*64+z] = list of (id, height, bottomY, block, sky)."""
    minX, maxX, minZ, maxZ = bounds
    idxs = [x * WIDTH + z for x in range(minX, maxX) for z in range(minZ, maxZ)]
    r = Reader(raw)

    counts = [r.varint() for _ in idxs]

    ids, disc, lit = {}, {}, {}
    for i, n in zip(idxs, counts):
        col_id, col_d, col_l = [], [], []
        for _ in range(n):
            e = r.varint()
            col_id.append(e >> 2)
            col_l.append(bool(e & 2))
            col_d.append(bool(e & 1))
        ids[i], disc[i], lit[i] = col_id, col_d, col_l

    heights = {i: [r.varint() for _ in range(n)] for i, n in zip(idxs, counts)}

    bottoms = {}
    prev = 0
    for i, n in zip(idxs, counts):
        col = []
        for k in range(n):
            err = zigzag_decode(r.varint()) if disc[i][k] else 0
            b = prev - heights[i][k] + err
            col.append(b)
            prev = b
        bottoms[i] = col

    for i, n in zip(idxs, counts):
        col = []
        for k in range(n):
            block = sky = 0
            if lit[i][k]:
                packed = r.byte()
                sky, block = packed & 0xF, packed >> 4
            col.append((ids[i][k], heights[i][k], bottoms[i][k], block, sky))
        columns[i] = col

    if r.p != len(raw):
        raise ValueError(f"{len(raw) - r.p} trailing bytes after decoding region")


def encode_datapoint(id_, height, bottom, block, sky):
    if not (0 <= id_ <= 0x7FFFFFFF and 0 < height < 4096 and 0 <= bottom < 4096
            and bottom + height <= 4096 and 0 <= block <= 15 and 0 <= sky <= 15):
        raise ValueError(f"datapoint out of range: id={id_} h={height} y={bottom} bl={block} sl={sky}")
    return id_ | (height << 32) | (bottom << 44) | (sky << 56) | (block << 60)


def mapping_count(raw):
    return struct.unpack(">i", raw[:4])[0] if len(raw) >= 4 else 0


def convert_row(row):
    mode = row["CompressionMode"]
    columns = [[] for _ in range(WIDTH * WIDTH)]
    read_v2_region(decompress(row["Data"], mode), INTERIOR, columns)
    for name, bounds in ADJ_BOUNDS.items():
        if row[name]:
            read_v2_region(decompress(row[name], mode), bounds, columns)

    mapping_raw = decompress(row["Mapping"], mode)
    n_ids = mapping_count(mapping_raw)
    out = bytearray()
    points = 0
    pack_count = struct.Struct(">h").pack
    for col in columns:
        out += pack_count(len(col))
        if not col:
            continue
        vals = array("Q")
        for dp in col:
            if dp[0] >= n_ids:
                raise ValueError(f"datapoint id {dp[0]} >= mapping size {n_ids}")
            vals.append(encode_datapoint(*dp))
        # Java writeLong = big-endian; same bytes whether read signed or unsigned
        if sys.byteorder == "little":
            vals.byteswap()
        out += vals.tobytes()
        points += len(col)
    out_mode = row.get("out_mode", mode)
    new_mapping = row["Mapping"] if out_mode == mode else compress(mapping_raw, out_mode)
    return compress(bytes(out), out_mode), new_mapping, out_mode, points


ROW_COLS = ("DetailLevel", "PosX", "PosZ", "CompressionMode", "Data", "Mapping") + tuple(ADJ_BOUNDS)


def _work(row):
    """Runs in a worker process. Returns (rowid, new_data | None, points, error)."""
    try:
        return (row["rowid"], *convert_row(row), None)
    except Exception as e:  # a bad row is left as v2 (Voxy just skips it)
        return row["rowid"], None, None, None, 0, f"DetailLevel={row['DetailLevel']} PosX={row['PosX']} PosZ={row['PosZ']}: {e}"


def _iter_rows(path, where, out_mode):
    c = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    for r in c.execute(f"SELECT rowid, {', '.join(ROW_COLS)} FROM FullData WHERE {where}"):
        r = dict(r)
        r["out_mode"] = r["CompressionMode"] if out_mode is None else out_mode
        yield r
    c.close()


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    all_levels = "--all-levels" in sys.argv
    # Voxy's Zstd import path is broken (swapped buffers), so write XZ unless asked otherwise
    out_mode = None if "--keep-compression" in sys.argv else LZMA2
    if not args:
        print(__doc__)
        print("Options: --all-levels         also convert DetailLevel > 0 (Voxy only imports level 0)")
        print("         --keep-compression   keep the original compression instead of writing XZ")
        sys.exit(1)
    src = os.path.abspath(args[0])
    dst = os.path.abspath(args[1]) if len(args) > 1 else os.path.join(os.path.dirname(src), "DistantHorizons_v1.sqlite")
    if src == dst:
        sys.exit("output must differ from input")
    if os.path.exists(dst):
        sys.exit(f"output already exists, delete it first: {dst}")

    # Copy via SQLite's backup API (includes anything still in the -wal file); source opened read-only.
    s = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
    d = sqlite3.connect(dst)
    s.backup(d)
    s.close()

    where = "DataFormatVersion = 2" + ("" if all_levels else " AND DetailLevel = 0")
    total = d.execute(f"SELECT count(*) FROM FullData WHERE {where}").fetchone()[0]
    workers = max(1, (os.cpu_count() or 2) - 1)
    print(f"Converting {total} rows using {workers} processes ...", flush=True)

    done = failed = points = 0
    last_pct = -1
    start = time.time()
    with Pool(workers) as pool:
        for rowid, data, mapping, mode, n, err in pool.imap_unordered(_work, _iter_rows(src, where, out_mode), chunksize=4):
            if err:
                failed += 1
                print(f"  failed: {err}", flush=True)
            else:
                d.execute("UPDATE FullData SET Data = ?, Mapping = ?, CompressionMode = ?, DataFormatVersion = 1 WHERE rowid = ?",
                          (data, mapping, mode, rowid))
                points += n
            done += 1
            pct = done * 100 // total
            if pct != last_pct:
                last_pct = pct
                el = time.time() - start
                eta = el / done * (total - done)
                bar = "#" * (pct // 4) + "-" * (25 - pct // 4)
                print(f"  [{bar}] {pct:3d}%  {done}/{total}  elapsed {el:5.0f}s  eta {eta:5.0f}s", flush=True)
                d.commit()
    d.commit()
    print("Compacting ...", flush=True)
    d.execute("VACUUM")
    d.close()
    print(f"Done in {time.time() - start:.0f}s: {done - failed} converted, {failed} failed, {points} datapoints.\nOutput: {dst}")


if __name__ == "__main__":
    main()
