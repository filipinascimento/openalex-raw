from pathlib import Path
import gzip

import pytest

import dbgz
import openalexraw as oaraw
import openalexraw.archive as oa_archive


@pytest.fixture
def openalex_fixture(tmp_path: Path):
    base = tmp_path / "openalex-snapshot"
    works_dir = base / "data" / "works" / "updated_date=2024-01-01"
    works_dir.mkdir(parents=True, exist_ok=True)

    gz_path = works_dir / "part_000.gz"
    records = [
        {
            "id": "https://openalex.org/W1",
            "best_oa_location": {
                "source": {
                    "id": "https://openalex.org/S11",
                    "issn": "1234-5678",
                    "issn_l": "1234-5678",
                    "publisher": "Pub A",
                    "publisher_id": "https://openalex.org/P99",
                }
            },
        },
        {
            "id": "https://openalex.org/W2",
            "best_oa_location": {
                "source": {
                    "id": "https://openalex.org/S12",
                    "issn": "8765-4321",
                    "issn_l": "8765-4321",
                    "publisher": "Pub B",
                    "publisher_id": "https://openalex.org/P98",
                }
            },
        },
    ]

    with gzip.open(gz_path, "wt") as fd:
        for row in records:
            fd.write(__import__("ujson").dumps(row) + "\n")

    manifest_path = base / "data" / "works" / "manifest"
    manifest_path.write_text('{"meta":{"record_count":2},"entries":[]}')

    return {
        "base": base,
        "gz_path": gz_path,
    }


def test_openalex_parallel_loader(openalex_fixture):
    oa = oaraw.OpenAlex(openAlexPath=openalex_fixture["base"])

    seq_ids = [item["id"] for item in oa.rawEntities("works")]
    par_ids = [item["id"] for item in oa.rawEntitiesParallel(entityType="works", ncpu=2, unordered=True)]

    assert sorted(seq_ids) == sorted(par_ids)
    assert len(seq_ids) == 2


def test_create_dbgz_custom_selection_with_inference(openalex_fixture, tmp_path: Path):
    oa = oaraw.OpenAlex(openAlexPath=openalex_fixture["base"])

    selection = [
        ("best_oa_location:source:id",),
        ("best_oa_location:source:issn",),
        ("best_oa_location:source:publisher", "s"),
        ("best_oa_location:source:publisher_id", "i", oa_archive.processOpenAlexID),
    ]

    archive_path = oa_archive.createDBGZ(
        oa,
        "works",
        tmp_path,
        selection=selection,
        rawPaths=[openalex_fixture["gz_path"]],
        parallel=True,
        ncpu=2,
        showProgressbar=False,
    )

    with dbgz.DBGZReader(str(archive_path)) as reader:
        rows = list(reader.entries)

    assert len(rows) == 2
    assert rows[0]["best_oa_location:source:id"] in (11, 12)
    assert rows[0]["best_oa_location:source:publisher"] in ("Pub A", "Pub B")


def test_create_parquet_with_custom_paths(openalex_fixture, tmp_path: Path):
    try:
        import parcake  # noqa: F401
    except Exception as exc:
        pytest.skip("parcake unavailable: %s" % exc)

    oa = oaraw.OpenAlex(openAlexPath=openalex_fixture["base"])
    selection = [
        ("best_oa_location:source:id", "i", oa_archive.processOpenAlexID),
        ("best_oa_location:source:issn", "s", None),
    ]

    parquet_path = oa_archive.createParquet(
        oa,
        "works",
        tmp_path,
        selection=selection,
        rawPaths=[openalex_fixture["gz_path"]],
        parallel=False,
        showProgressbar=False,
    )

    import pandas as pd

    frame = pd.read_parquet(parquet_path)
    assert frame.shape[0] == 2
    assert set(frame.columns) == {"best_oa_location:source:id", "best_oa_location:source:issn"}
