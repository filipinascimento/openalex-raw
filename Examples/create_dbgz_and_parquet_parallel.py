from pathlib import Path

import openalexraw as oaraw


def main():
    openalex_path = Path("/mnt/openalex/OpenAlex/openalex-snapshot/")
    out_path = Path("./Processed")
    out_path.mkdir(parents=True, exist_ok=True)

    oa = oaraw.OpenAlex(openAlexPath=openalex_path)

    # You can replace with a small subset of file paths for smoke tests.
    raw_paths = oa.getRawEntitiesPaths("works")[:2]

    selection = [
        ("best_oa_location:source:id",),
        ("best_oa_location:source:issn",),
        ("best_oa_location:source:issn_l",),
        ("best_oa_location:source:publisher",),
        ("best_oa_location:source:publisher_id",),
    ]

    dbgz_path = oaraw.archive.createDBGZ(
        oa,
        "works",
        out_path,
        selection=selection,
        rawPaths=raw_paths,
        parallel=True,
        ncpu=4,
        showProgressbar=True,
    )
    print("DBGZ:", dbgz_path)

    parquet_path = oaraw.archive.createParquet(
        oa,
        "works",
        out_path,
        selection=selection,
        rawPaths=raw_paths,
        parallel=True,
        ncpu=4,
        showProgressbar=True,
    )
    print("Parquet:", parquet_path)


if __name__ == "__main__":
    main()
