"""
Recompress a converted (DataFormatVersion 1) Distant Horizons database from Zstd to XZ.

Why: Voxy's DHImporter passes the Zstd buffers to ZSTD_decompressDCtx in the wrong
order (src/dst swapped), so every Zstd row fails with "Unknown frame descriptor".
Its XZ (CompressionMode 3) path works.

Usage: python dh_zstd_to_xz.py <input.sqlite> [output.sqlite]
Default output: <input name>_xz.sqlite next to the input. Input is opened read-only.
Only DetailLevel 0 rows (the ones Voxy imports) are recompressed.
"""
import lzma
import os
import sqlite3
import sys
import time
from multiprocessing import Pool

import zstandard

ZSTD, XZ = 4, 3
XZ_FILTERS = [{"id": lzma.FILTER_LZMA2, "preset": 3}]
_zd = None


def xz(raw):
    return lzma.compress(raw, format=lzma.FORMAT_XZ, check=lzma.CHECK_CRC64, filters=XZ_FILTERS)


def work(row):
    global _zd
    if _zd is None:
        _zd = zstandard.ZstdDecompressor()
    rowid, data, mapping = row
    try:
        new_data = xz(_zd.decompress(data)) if data else data
        new_map = xz(_zd.decompress(mapping)) if mapping else mapping
        # round-trip check so a bad blob is never written
        if data and lzma.decompress(new_data) != _zd.decompress(data):
            raise ValueError("round-trip mismatch")
        return rowid, new_data, new_map, None
    except Exception as e:
        return rowid, None, None, str(e)


def iter_rows(path):
    c = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    yield from c.execute(
        "SELECT rowid, Data, Mapping FROM FullData WHERE DetailLevel = 0 AND CompressionMode = ? AND DataFormatVersion = 1",
        (ZSTD,))
    c.close()


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    src = os.path.abspath(sys.argv[1])
    base, ext = os.path.splitext(src)
    dst = os.path.abspath(sys.argv[2]) if len(sys.argv) > 2 else base + "_xz" + (ext or ".sqlite")
    if src == dst or os.path.exists(dst):
        sys.exit(f"output must be a new file: {dst}")

    s = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
    d = sqlite3.connect(dst)
    s.backup(d)
    s.close()

    total = d.execute("SELECT count(*) FROM FullData WHERE DetailLevel = 0 AND CompressionMode = ? AND DataFormatVersion = 1",
                      (ZSTD,)).fetchone()[0]
    workers = max(1, (os.cpu_count() or 2) // 2)
    print(f"Recompressing {total} rows to XZ using {workers} processes ...", flush=True)

    done = failed = 0
    last = -1
    start = time.time()
    with Pool(workers) as pool:
        for rowid, nd, nm, err in pool.imap_unordered(work, iter_rows(src), chunksize=4):
            done += 1
            if err:
                failed += 1
                print(f"  rowid {rowid} failed: {err}", flush=True)
            else:
                d.execute("UPDATE FullData SET Data = ?, Mapping = ?, CompressionMode = ? WHERE rowid = ?",
                          (nd, nm, XZ, rowid))
            pct = done * 100 // max(total, 1)
            if pct != last:
                last = pct
                el = time.time() - start
                bar = "#" * (pct // 4) + "-" * (25 - pct // 4)
                print(f"  [{bar}] {pct:3d}%  {done}/{total}  elapsed {el:5.0f}s  eta {el / done * (total - done):5.0f}s", flush=True)
                d.commit()
    d.commit()
    print("Compacting ...", flush=True)
    d.execute("VACUUM")
    d.close()
    print(f"Done in {time.time() - start:.0f}s: {done - failed} recompressed, {failed} failed.\nOutput: {dst}")


if __name__ == "__main__":
    main()
