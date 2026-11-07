# Distant-Horizons-To-Voxy

Convert Distant Horizons LOD data into a format Voxy can import, fully client-side.

Voxy has a built-in importer (`/voxy import distant_horizons <path>`), but out of the box it
doesn't work with current Distant Horizons databases:

1. **The command is hidden.** Voxy only registers it when the SQLite JDBC driver is on the
   classpath, and Voxy's release jar doesn't bundle it.
   → Install the [Minecraft SQLite JDBC](https://modrinth.com/mod/minecraft-sqlite-jdbc) mod.
2. **Only DH data format version 1 is read.** Current DH writes `DataFormatVersion = 2`
   (a compact varint encoding with the edge columns in separate `*AdjData` blobs), so Voxy
   skips every row and the import "finishes" instantly with 0 chunks/s.
   → `dh_v2_to_v1.py` rewrites the data in the version 1 layout.
3. **Voxy's Zstd path is broken.** `DHImporter` passes its buffers to `ZSTD_decompressDCtx`
   with source and destination swapped, so every Zstd row fails with
   `ZSTD EXCEPTION: Unknown frame descriptor`. Its XZ path works.
   → The converter writes XZ (`CompressionMode = 3`) by default.

## Requirements

- Python 3.10+
- `pip install zstandard`

## Usage

### 1. Convert

```bash
python dh_v2_to_v1.py "<path>\DistantHorizons.sqlite"
```

This writes `DistantHorizons_v1.sqlite` next to the input. The input is opened read-only and
never modified. Rows are processed in parallel and progress is printed every 1%.

Options:

| Flag | Effect |
|---|---|
| `--all-levels` | Also convert `DetailLevel > 0` rows. Voxy only imports level 0, so this is off by default. |
| `--keep-compression` | Keep the original compression instead of writing XZ. Voxy can't read Zstd rows, so only use this for other tools. |

A second path argument sets a custom output file.

Distant Horizons stores each dimension in its own folder, for example
`.minecraft\Distant_Horizons_server_data\<server>\<id>@minecraft@@overworld\DistantHorizons.sqlite`,
with matching `@@the_nether` and `@@the_end` folders. Convert each one separately.

### 2. (Optional) Verify

```bash
python dh_verify_v1.py "<path>\DistantHorizons_v1.sqlite"
```

This checks every version 1 row the way Voxy reads it:
- each blob parses exactly, with no leftover bytes
- every block ID exists in the row's mapping
- no column has overlapping or missing runs

It also prints the most common top blocks so you can see that the terrain looks right.
It runs on half your CPU cores. An optional second argument limits the number of rows checked.

### 3. Import in game

Go to the matching dimension on the server and run:

```
/voxy import distant_horizons "C:/full/path/to/DistantHorizons_v1.sqlite"
```

- Put the path in quotes and use forward slashes. Without quotes the `:` breaks argument
  parsing, and backslashes are escape characters inside quotes.
- Point at the file itself. If you give a folder, Voxy looks for `DistantHorizons.sqlite`.

## dh_zstd_to_xz.py

This one-off tool recompresses an already-converted version 1 file from Zstd to XZ. You only
need it for files made with an older version of the converter that kept Zstd.

```bash
python dh_zstd_to_xz.py "<path>\DistantHorizons_v1.sqlite"
```

It writes `DistantHorizons_v1_xz.sqlite` next to the input.

## Warnings

- **Don't just change the version number.** Running
  `UPDATE FullData SET DataFormatVersion = 1` only relabels the rows; the data is still in the
  version 2 layout. Voxy then misreads it, and Distant Horizons can no longer read the file
  either. The converter only picks up rows still marked version 2, so it skips a relabelled
  file. Keep an untouched copy of the original.
- The converter only reads full-detail data (`DetailLevel = 0`). Areas that DH only ever
  stored at lower detail won't be imported.

## Format notes

These come from `distant-horizons-core` (`FullDataSourceV2DTO`, `FullDataPointUtil`) and
Voxy's `DHImporter`.

- **Table:** `FullData`. Each row is a 64×64 block area covering 4×4 chunks.
- **Datapoint:** one `long` per vertical run:
  - bits 0–31: mapping ID
  - bits 32–43: height
  - bits 44–55: bottom Y
  - bits 56–59: sky light
  - bits 60–63: block light
- **Version 1 `Data` blob:** for each of the 64×64 columns (index `x*64+z`), an `int16` run
  count followed by that many big-endian `int64` datapoints.
- **Version 2 `Data` blob:** only the inner columns (x and z from 1 to 62), split into five
  streams:
  1. varint run counts
  2. varint `(id<<2 | lit<<1 | discontinuous)`
  3. varint heights
  4. zigzag varint bottom-Y corrections, only where `discontinuous` is set
  5. one packed light byte, only where `lit` is set
  
  The edge columns are stored the same way in `NorthAdjData`, `SouthAdjData`, `EastAdjData`
  and `WestAdjData`.
- **`Mapping` blob:** the same in both versions. An `int32` count followed by `writeUTF`
  strings of the form `<biome>_DH-BSW_<block>[_STATE_{prop:value}...]`.
- **`CompressionMode`:** 0 = none, 1 = LZ4, 2 = Zstd stream (deprecated), 3 = XZ/LZMA2,
  4 = Zstd block.
