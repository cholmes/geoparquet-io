import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.parquet_writer import ParquetWriteSettings


def test_get_pyarrow_kwargs_includes_page_index_flag():
    s = ParquetWriteSettings(write_page_index=True, data_page_size=32768)
    kwargs = s.get_pyarrow_kwargs()
    assert kwargs["write_page_index"] is True
    assert kwargs["data_page_size"] == 32768


def test_get_pyarrow_kwargs_defaults_page_index_false():
    s = ParquetWriteSettings()
    kwargs = s.get_pyarrow_kwargs()
    assert kwargs["write_page_index"] is False
    assert "data_page_size" not in kwargs  # omitted unless set


def test_page_index_written_via_write_table(tmp_path):
    tbl = pa.table({"x": list(range(2000))})
    s = ParquetWriteSettings(write_page_index=True, data_page_size=4096, row_group_rows=500)
    out = tmp_path / "pi.parquet"
    pq.write_table(tbl, out, **s.get_pyarrow_kwargs())
    col = pq.ParquetFile(out).metadata.row_group(0).column(0)
    assert col.has_column_index is True
    assert col.has_offset_index is True


def test_page_index_written_via_parquet_writer(tmp_path):
    tbl = pa.table({"x": list(range(2000))})
    s = ParquetWriteSettings(write_page_index=True, data_page_size=4096)
    kwargs = s.get_pyarrow_kwargs()
    kwargs.pop("row_group_size", None)  # ParquetWriter rejects this kwarg
    out = tmp_path / "pi_stream.parquet"
    with pq.ParquetWriter(out, tbl.schema, **kwargs) as w:
        for b in tbl.to_batches(max_chunksize=500):
            w.write_table(pa.Table.from_batches([b], schema=tbl.schema))
    col = pq.ParquetFile(out).metadata.row_group(0).column(0)
    assert col.has_column_index is True
    assert col.has_offset_index is True
