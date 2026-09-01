import sqlite3
from pathlib import Path

import pytest

from ms_flow.core.data import (
    BytesInputSpec,
    DataBridge,
    DataContext,
    DataContractError,
    DbInputSpec,
    DbOutputSpec,
    ExecutorTransportProfile,
    FileInputSpec,
    FileOutputSpec,
    InlineInputSpec,
    from_wire_value,
    to_wire_value,
)
from ms_flow.core.data.backends import SQLiteBackend


class _FakeOutputBackend:
    target = "db"

    def __init__(self):
        self.calls = []

    def write(self, spec, data, context):
        self.calls.append((spec, data, context))
        return {"backend": "fake", "rows": 1}


def test_backend_wire_roundtrip_specs():
    payload = {
        "inline": InlineInputSpec({"value": 1}),
        "bytes": BytesInputSpec(b"abc"),
        "file": FileInputSpec("input.txt", root="project", fmt="text"),
        "out": FileOutputSpec("result.json", root="project", fmt="json"),
        "graph": DbOutputSpec(
            table="",
            mode="graph",
            meta={
                "graph": {
                    "nodes": [
                        {"name": "molecules", "table": "molecules", "columns": ("name", "kind")},
                        {"name": "ligands", "table": "ligands", "columns": ("molecule_id", "active")},
                    ],
                    "relations": [
                        {
                            "source_node": "ligands",
                            "source_ref_field": "molecule_ref",
                            "target_node": "molecules",
                            "fk_field": "molecule_id",
                        },
                    ],
                },
            },
        ),
    }
    wired = to_wire_value(payload)
    restored = from_wire_value(wired)

    assert isinstance(restored["inline"], InlineInputSpec)
    assert restored["inline"].payload == {"value": 1}
    assert isinstance(restored["bytes"], BytesInputSpec)
    assert restored["bytes"].payload == b"abc"
    assert isinstance(restored["file"], FileInputSpec)
    assert restored["file"].root == "project"
    assert isinstance(restored["out"], FileOutputSpec)
    assert isinstance(restored["graph"], DbOutputSpec)
    assert restored["graph"].mode == "graph"
    assert restored["graph"].meta["graph"]["nodes"][0]["name"] == "molecules"
    assert restored["graph"].meta["graph"]["relations"][0]["fk_field"] == "molecule_id"


def test_db_input_spec_supports_friendly_aliases_and_roundtrip():
    spec = DbInputSpec(
        table="molecules",
        fields=("id", "name"),
        filters={"score__gte": 2.5},
        order=("-score", "id"),
        limit=10,
        offset=5,
    )
    wired = to_wire_value({"spec": spec})
    restored = from_wire_value(wired)["spec"]

    assert restored.fields == ("id", "name")
    assert restored.filters == {"score__gte": 2.5}
    assert restored.order == ("-score", "id")
    assert restored.limit == 10
    assert restored.offset == 5


def test_data_bridge_materializes_file_specs(tmp_path):
    sample = tmp_path / "sample.txt"
    sample.write_text("ligand-001", encoding="utf-8")
    blob = tmp_path / "blob.bin"
    blob.write_bytes(b"\x00\x01")

    bridge = DataBridge()
    payload = {
        "name": FileInputSpec(str(sample), fmt="text"),
        "blob": FileInputSpec(str(blob), fmt="binary"),
        "inline": InlineInputSpec({"ok": True}),
    }
    wired = to_wire_value(payload)

    materialized = bridge.materialize_payload(wired, DataContext(project_dir=tmp_path))
    assert materialized["name"] == "ligand-001"
    assert materialized["blob"] == b"\x00\x01"
    assert materialized["inline"] == {"ok": True}


def test_data_bridge_reads_and_writes_sqlite(tmp_path):
    db_path = tmp_path / "sample.db"
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE molecules (id INTEGER PRIMARY KEY, name TEXT)")
        cur.execute("CREATE TABLE results (id INTEGER PRIMARY KEY, score REAL)")
        cur.execute("INSERT INTO molecules (id, name) VALUES (?, ?)", (1, "MolA"))
        cur.execute("INSERT INTO molecules (id, name) VALUES (?, ?)", (2, "MolB"))
        conn.commit()
    finally:
        conn.close()

    bridge = DataBridge()
    ctx = DataContext(project_dir=tmp_path)
    rows = bridge.resolve_input(
        DbInputSpec(table="molecules", columns=("id", "name"), where={"id": 2}, db_role="custom", db_path=str(db_path)),
        ctx,
    )
    assert rows == [{"id": 2, "name": "MolB"}]

    receipt = bridge.persist_output(
        DbOutputSpec(table="results", columns=("id", "score"), db_role="custom", db_path=str(db_path)),
        {"id": 10, "score": 7.5},
        ctx,
    )
    assert receipt["rows"] == 1

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, score FROM results WHERE id = 10")
        row = cur.fetchone()
    finally:
        conn.close()
    assert row == (10, 7.5)


def test_data_bridge_writes_graph_output_with_autoincrement_relations(tmp_path):
    db_path = tmp_path / "graph.db"
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE molecules (id INTEGER PRIMARY KEY, name TEXT, kind TEXT)")
        cur.execute("CREATE TABLE receptors (id INTEGER PRIMARY KEY, molecule_id INTEGER, source TEXT)")
        cur.execute("CREATE TABLE ligands (id INTEGER PRIMARY KEY, molecule_id INTEGER, active INTEGER)")
        cur.execute("CREATE TABLE complexes (id INTEGER PRIMARY KEY, receptor_id INTEGER, ligand_id INTEGER, label TEXT)")
        conn.commit()
    finally:
        conn.close()

    graph_spec = DbOutputSpec(
        table="",
        mode="graph",
        db_role="custom",
        db_path=str(db_path),
        meta={
            "graph": {
                "nodes": [
                    {"name": "molecules", "table": "molecules", "columns": ("name", "kind")},
                    {"name": "receptors", "table": "receptors", "columns": ("molecule_id", "source")},
                    {"name": "ligands", "table": "ligands", "columns": ("molecule_id", "active")},
                    {"name": "complexes", "table": "complexes", "columns": ("receptor_id", "ligand_id", "label")},
                ],
                "relations": [
                    {
                        "source_node": "receptors",
                        "source_ref_field": "molecule_ref",
                        "target_node": "molecules",
                        "fk_field": "molecule_id",
                    },
                    {
                        "source_node": "ligands",
                        "source_ref_field": "molecule_ref",
                        "target_node": "molecules",
                        "fk_field": "molecule_id",
                    },
                    {
                        "source_node": "complexes",
                        "source_ref_field": "receptor_ref",
                        "target_node": "receptors",
                        "fk_field": "receptor_id",
                    },
                    {
                        "source_node": "complexes",
                        "source_ref_field": "ligand_ref",
                        "target_node": "ligands",
                        "fk_field": "ligand_id",
                    },
                ],
            },
        },
    )
    payload = {
        "molecules": [
            {"$ref": "mol_rec_1", "name": "rec-a", "kind": "protein"},
            {"$ref": "mol_lig_1", "name": "lig-a", "kind": "small_molecule"},
        ],
        "receptors": [
            {"$ref": "rec_1", "molecule_ref": "mol_rec_1", "source": "cocrystal"},
        ],
        "ligands": [
            {"$ref": "lig_1", "molecule_ref": "mol_lig_1", "active": 1},
        ],
        "complexes": [
            {"$ref": "cx_1", "receptor_ref": "rec_1", "ligand_ref": "lig_1", "label": "ref-complex"},
        ],
    }

    bridge = DataBridge()
    receipt = bridge.persist_output(graph_spec, payload, DataContext(project_dir=tmp_path))
    assert receipt["rows"] == 5
    assert receipt["nodes"] == {"molecules": 2, "receptors": 1, "ligands": 1, "complexes": 1}

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, name, kind FROM molecules ORDER BY id ASC")
        molecules = cur.fetchall()
        cur.execute("SELECT id, molecule_id, source FROM receptors ORDER BY id ASC")
        receptors = cur.fetchall()
        cur.execute("SELECT id, molecule_id, active FROM ligands ORDER BY id ASC")
        ligands = cur.fetchall()
        cur.execute("SELECT id, receptor_id, ligand_id, label FROM complexes ORDER BY id ASC")
        complexes = cur.fetchall()
    finally:
        conn.close()

    assert molecules == [(1, "rec-a", "protein"), (2, "lig-a", "small_molecule")]
    assert receptors == [(1, 1, "cocrystal")]
    assert ligands == [(1, 2, 1)]
    assert complexes == [(1, 1, 1, "ref-complex")]


def test_data_bridge_reads_sqlite_with_friendly_filters_order_and_offset(tmp_path):
    db_path = tmp_path / "ordered.db"
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE molecules (id INTEGER PRIMARY KEY, name TEXT, score REAL)")
        cur.execute("INSERT INTO molecules (id, name, score) VALUES (1, 'MolA', 1.0)")
        cur.execute("INSERT INTO molecules (id, name, score) VALUES (2, 'MolB', 2.0)")
        cur.execute("INSERT INTO molecules (id, name, score) VALUES (3, 'MolC', 3.0)")
        cur.execute("INSERT INTO molecules (id, name, score) VALUES (4, 'LigD', 4.0)")
        conn.commit()
    finally:
        conn.close()

    bridge = DataBridge()
    ctx = DataContext(project_dir=tmp_path)
    rows = bridge.resolve_input(
        DbInputSpec(
            table="molecules",
            fields=("id", "name"),
            filters={"name__contains": "M", "score__gte": 2.0},
            order=("-score",),
            limit=1,
            offset=1,
            db_role="custom",
            db_path=str(db_path),
        ),
        ctx,
    )
    assert rows == [{"id": 2, "name": "MolB"}]


def test_data_bridge_transport_planner_stages_remote_without_shared_fs(tmp_path):
    sample = tmp_path / "ligands.smi"
    sample.write_text("CCO ligand_1\n", encoding="utf-8")
    bridge = DataBridge()
    payload = to_wire_value({"ligands": FileInputSpec(str(sample), fmt="text")})
    profile = ExecutorTransportProfile(backend="ray", mode="external", shared_fs=False)

    materialized = bridge.materialize_payload(payload, DataContext(project_dir=tmp_path), executor_profile=profile)
    transfer = materialized["ligands"]["__molsuite_ray_file_input__"]
    assert transfer["path"] == str(sample)
    assert transfer["delivery"] == "content"


def test_data_bridge_transport_planner_accepts_remote_with_shared_fs(tmp_path):
    sample = tmp_path / "ligands.smi"
    sample.write_text("CCO ligand_1\n", encoding="utf-8")
    bridge = DataBridge()
    payload = to_wire_value({"ligands": FileInputSpec(str(sample), fmt="text")})
    profile = ExecutorTransportProfile(backend="ray", mode="external", shared_fs=True)

    materialized = bridge.materialize_payload(payload, DataContext(project_dir=tmp_path), executor_profile=profile)
    assert materialized["ligands"] == "CCO ligand_1\n"


def test_data_bridge_hpc_staged_copy_requires_wdir(tmp_path):
    sample = tmp_path / "ligands.smi"
    sample.write_text("CCO ligand_1\n", encoding="utf-8")
    bridge = DataBridge()
    payload = to_wire_value({"ligands": FileInputSpec(str(sample), fmt="text")})
    profile = ExecutorTransportProfile(backend="hpc", mode="external", shared_fs=False)

    with pytest.raises(DataContractError):
        bridge.materialize_payload(payload, DataContext(project_dir=tmp_path), executor_profile=profile)


def test_data_bridge_hpc_staged_copy_returns_remote_path(tmp_path):
    sample = tmp_path / "ligands.smi"
    sample.write_text("CCO ligand_1\n", encoding="utf-8")
    hpc_wdir = tmp_path / "hpc_wdir"
    bridge = DataBridge()
    payload = to_wire_value({"ligands": FileInputSpec(str(sample), fmt="text")})
    profile = ExecutorTransportProfile(backend="hpc", mode="external", shared_fs=False)
    ctx = DataContext(project_dir=tmp_path, extras={"hpc_wdir": str(hpc_wdir), "job_id": "job1", "chunk_id": "c1"})

    materialized = bridge.materialize_payload(payload, ctx, executor_profile=profile)
    staged_path = Path(materialized["ligands"])
    assert staged_path.exists()
    assert staged_path.read_text(encoding="utf-8") == "CCO ligand_1\n"


def test_data_bridge_transport_planner_allows_bytes_without_shared_fs():
    bridge = DataBridge()
    payload = to_wire_value({"blob": BytesInputSpec(b"abc")})
    profile = ExecutorTransportProfile(backend="ray", mode="external", shared_fs=False)
    materialized = bridge.materialize_payload(payload, DataContext(), executor_profile=profile)
    assert materialized["blob"] == b"abc"


def test_data_bridge_materializes_db_inputs_before_remote_dispatch(tmp_path):
    db_path = tmp_path / "project.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE molecules (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO molecules (id, name) VALUES (1, 'MolA')")
        conn.commit()
    finally:
        conn.close()

    bridge = DataBridge()
    payload = to_wire_value(
        {"molecules": DbInputSpec(table="molecules", columns=("id", "name"), db_role="custom", db_path=str(db_path))}
    )
    profile = ExecutorTransportProfile(backend="ray", mode="external", shared_fs=False)

    materialized = bridge.materialize_payload(payload, DataContext(project_dir=tmp_path), executor_profile=profile)

    assert materialized == {"molecules": [{"id": 1, "name": "MolA"}]}


def test_data_bridge_uses_injected_output_backend_for_non_project_db():
    fake_backend = _FakeOutputBackend()
    bridge = DataBridge(output_backends={"db": fake_backend})
    spec = DbOutputSpec(table="results", columns=("id",), db_role="custom", db_path="/tmp/fake.db")

    receipt = bridge.persist_output(spec, {"id": 1}, DataContext())

    assert receipt == {"backend": "fake", "rows": 1}
    assert len(fake_backend.calls) == 1
    assert fake_backend.calls[0][0] is spec


def test_data_bridge_uses_injected_project_output_persister_without_sqlite_assumptions():
    calls = []

    def _fake_project_persister(spec, data, context):
        calls.append((spec, data, context))
        return {"backend": "project-fake", "rows": 2}

    bridge = DataBridge(project_output_persister=_fake_project_persister)
    spec = DbOutputSpec(table="molecules", columns=("id",), db_role="project")

    receipt = bridge.persist_output(spec, [{"id": 1}, {"id": 2}], DataContext())

    assert receipt == {"backend": "project-fake", "rows": 2}
    assert len(calls) == 1
    assert calls[0][0] is spec


def test_sqlite_backend_project_writes_delegate_to_project_store(tmp_path):
    db_path = tmp_path / "project.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE results (id INTEGER)")
        conn.commit()
    finally:
        conn.close()

    receipt = SQLiteBackend().write(
        DbOutputSpec(table="results", columns=("id",), db_role="project"),
        {"id": 1},
        DataContext(
            project_db_path=db_path,
            extras={
                "molsuite_output_sink_key": "sink",
                "molsuite_output_commit_key": "chunk-1",
            },
        ),
    )

    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute("SELECT id FROM results").fetchall()
        commits = conn.execute(
            "SELECT commit_key FROM molsuite_output_commits WHERE sink_key = ?",
            ("sink",),
        ).fetchall()
    finally:
        conn.close()

    assert receipt["rows"] == 1
    assert rows == [(1,)]
    assert commits == [("chunk-1",)]
