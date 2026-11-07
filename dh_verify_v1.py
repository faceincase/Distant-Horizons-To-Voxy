"""
Sanity-check DataFormatVersion=1 rows the way Voxy will read them.

Usage: python dh_verify_v1.py <converted.sqlite> [max_rows]

Checks per row: blob parses exactly (64*64 columns, no trailing bytes), every id is
inside the mapping, and every column is a clean stack of runs (no overlaps, no gaps
except where DH itself marked them). Also prints the top block of a few columns so
you can eyeball that they look like real terrain.
"""
import os
import sqlite3
import struct
import sys
import time
from collections import Counter
from multiprocessing import Pool

import zstandard

zd = zstandard.ZstdDecompressor()


def dec(b, mode):
    if mode == 4:
        return zd.decompress(b)
    if mode == 3:
        import lzma
        return lzma.decompress(b)
    return bytes(b)


def read_mapping(raw):
    n = struct.unpack(">i", raw[:4])[0]
    p, names = 4, []
    for _ in range(n):
        ln = struct.unpack(">H", raw[p:p + 2])[0]
        names.append(raw[p + 2:p + 2 + ln].decode("utf-8", "replace"))
        p += 2 + ln
    return names


def check_row(row):
    """Worker: returns (error | None, columns, overlaps, gaps, Counter of top blocks)."""
    lvl, px, pz, mode, data, mapping = row
    cols = gaps = overlaps = 0
    tops = Counter()
    if True:
        try:
            raw = dec(data, mode)
            names = read_mapping(dec(mapping, mode))
            p = 0
            for _ in range(64 * 64):
                (n,) = struct.unpack_from(">h", raw, p)
                p += 2
                runs = []
                for _ in range(n):
                    (v,) = struct.unpack_from(">Q", raw, p)
                    p += 8
                    i, h, y = v & 0x7FFFFFFF, (v >> 32) & 0xFFF, (v >> 44) & 0xFFF
                    if i >= len(names):
                        raise ValueError(f"id {i} >= mapping {len(names)}")
                    runs.append((y, h, i))
                if runs:
                    cols += 1
                    runs.sort()
                    for (y0, h0, _), (y1, _, _) in zip(runs, runs[1:]):
                        if y0 + h0 > y1:
                            overlaps += 1
                        elif y0 + h0 < y1:
                            gaps += 1
                    # topmost non-air run
                    for y, h, i in reversed(runs):
                        name = names[i].split("_DH-BSW_")[-1].split("_STATE_")[0]
                        if name != "AIR" and not name.endswith(":air"):
                            tops[name] += 1
                            break
            if p != len(raw):
                raise ValueError(f"{len(raw) - p} trailing bytes")
        except Exception as e:
            return f"BAD row L{lvl} ({px},{pz}): {e}", 0, 0, 0, Counter()
    return None, cols, overlaps, gaps, tops


def iter_rows(path, limit):
    c = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    q = "SELECT DetailLevel, PosX, PosZ, CompressionMode, Data, Mapping FROM FullData WHERE DataFormatVersion = 1"
    if limit:
        q += f" LIMIT {limit}"
    yield from c.execute(q)
    c.close()


def main():
    path = sys.argv[1]
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else None
    c = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    total = c.execute("SELECT count(*) FROM FullData WHERE DataFormatVersion = 1").fetchone()[0]
    c.close()
    if limit:
        total = min(total, limit)
    workers = max(1, (os.cpu_count() or 2) // 2)
    print(f"Verifying {total} rows using {workers} processes ...", flush=True)

    rows = bad = cols = gaps = overlaps = 0
    tops = Counter()
    last_pct = -1
    start = time.time()
    with Pool(workers) as pool:
        for err, c_, o_, g_, t_ in pool.imap_unordered(check_row, iter_rows(path, limit), chunksize=4):
            rows += 1
            if err:
                bad += 1
                print(err, flush=True)
            cols += c_
            overlaps += o_
            gaps += g_
            tops.update(t_)
            pct = rows * 100 // max(total, 1)
            if pct != last_pct:
                last_pct = pct
                el = time.time() - start
                eta = el / rows * (total - rows)
                bar = "#" * (pct // 4) + "-" * (25 - pct // 4)
                print(f"  [{bar}] {pct:3d}%  {rows}/{total}  elapsed {el:5.0f}s  eta {eta:5.0f}s", flush=True)
    print(f"rows checked={rows} bad={bad} columns={cols} overlaps={overlaps} gaps={gaps}")
    print("most common top blocks:", tops.most_common(12))


if __name__ == "__main__":
    main()
