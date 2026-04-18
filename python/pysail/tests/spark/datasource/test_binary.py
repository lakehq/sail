import io

import numpy as np
import pytest
from PIL import Image
from pyspark.sql import functions as F  # noqa: N812


def _create_test_files(tmp_path):
    files = {}

    png_path = tmp_path / "test.png"
    png_array = np.zeros((10, 10, 3), dtype=np.uint8)
    png_array[:, :, 0] = np.linspace(0, 255, 10).astype(np.uint8).reshape(1, 10)
    png_array[:, :, 1] = np.linspace(0, 255, 10).astype(np.uint8).reshape(10, 1)
    png_array[:, :, 2] = 128
    png_img = Image.fromarray(png_array)
    png_buffer = io.BytesIO()
    png_img.save(png_buffer, format="PNG")
    png_content = png_buffer.getvalue()
    png_path.write_bytes(png_content)
    files["png"] = (png_path, png_content)

    jpeg_path = tmp_path / "test.jpeg"
    jpeg_array = np.random.randint(0, 256, (20, 20), dtype=np.uint8)
    jpeg_img = Image.fromarray(jpeg_array)
    jpeg_buffer = io.BytesIO()
    jpeg_img.save(jpeg_buffer, format="JPEG", quality=85)
    jpeg_content = jpeg_buffer.getvalue()
    jpeg_path.write_bytes(jpeg_content)
    files["jpeg"] = (jpeg_path, jpeg_content)

    pdf_path = tmp_path / "test.pdf"
    pdf_content = b"%PDF-1.4\n1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n2 0 obj\n<< /Type /Pages /Kids [] /Count 0 >>\nendobj\nxref\n0 3\n0000000000 65535 f\n0000000009 00000 n\n0000000058 00000 n\ntrailer\n<< /Size 3 /Root 1 0 R >>\nstartxref\n115\n%%EOF"
    pdf_path.write_bytes(pdf_content)
    files["pdf"] = (pdf_path, pdf_content)

    bin_path = tmp_path / "data.bin"
    bin_array = np.random.randint(0, 256, 256, dtype=np.uint8)
    bin_content = bin_array.tobytes()
    bin_path.write_bytes(bin_content)
    files["bin"] = (bin_path, bin_content)

    return files


def test_binary_read_basic(spark, tmp_path):
    files = _create_test_files(tmp_path)

    read_df = spark.read.format("binaryFile").load(str(tmp_path))
    assert read_df.columns == ["path", "modificationTime", "length", "content"]
    assert read_df.count() == len(files)

    rows = read_df.collect()
    for row in rows:
        filename = row.path.split("/")[-1]
        for file_path, expected_content in files.values():
            if file_path.name == filename:
                assert row.content == expected_content
                assert row.length == len(expected_content)
                break
        else:
            pytest.fail(f"Unexpected file returned: {filename}")


def test_binary_read_options(spark, tmp_path):
    (tmp_path / "image1.png").write_bytes(b"PNG1")
    (tmp_path / "image2.png").write_bytes(b"PNG2")
    (tmp_path / "document.pdf").write_bytes(b"PDF")
    (tmp_path / "photo.jpeg").write_bytes(b"JPEG")
    (tmp_path / "data.bin").write_bytes(b"BIN")
    (tmp_path / "test1.dat").write_bytes(b"DAT1")
    (tmp_path / "test2.dat").write_bytes(b"DAT2")

    png_df = spark.read.format("binaryFile").option("pathGlobFilter", "*.png").load(str(tmp_path))
    expected_png_count = 2
    assert png_df.count() == expected_png_count
    paths = [row.path for row in png_df.collect()]
    assert all("png" in p for p in paths)

    test_df = spark.read.format("binaryFile").option("pathGlobFilter", "test*").load(str(tmp_path))
    expected_test_count = 2
    assert test_df.count() == expected_test_count
    paths = [row.path for row in test_df.collect()]
    assert all("test" in p for p in paths)

    dat_df = spark.read.format("binaryFile").option("pathGlobFilter", "*.dat").load(str(tmp_path))
    expected_dat_count = 2
    assert dat_df.count() == expected_dat_count

    single_char_df = spark.read.format("binaryFile").option("pathGlobFilter", "image?.png").load(str(tmp_path))
    expected_single_count = 2
    assert single_char_df.count() == expected_single_count

    all_df = spark.read.format("binaryFile").load(str(tmp_path))
    expected_all_count = 7
    assert all_df.count() == expected_all_count


def test_binary_read_projections(spark, tmp_path):
    files = _create_test_files(tmp_path)

    read_df = spark.read.format("binaryFile").load(str(tmp_path))

    metadata_df = read_df.select("path", "length")
    assert metadata_df.columns == ["path", "length"]
    assert metadata_df.count() == len(files)

    rows = metadata_df.collect()
    for row in rows:
        assert hasattr(row, "path")
        assert hasattr(row, "length")
        assert not hasattr(row, "content")

    content_df = read_df.select("content")
    assert content_df.columns == ["content"]
    assert content_df.count() == len(files)

    reordered_df = read_df.select("length", "path", "modificationTime")
    assert reordered_df.columns == ["length", "path", "modificationTime"]

    count = read_df.select("path").count()
    assert count == len(files)

    total_size = read_df.agg(F.sum("length").alias("total_bytes")).collect()[0].total_bytes
    expected_size = sum(len(content) for _, content in files.values())
    assert total_size == expected_size

    min_file_size = 100
    large_files = read_df.filter(read_df.length > min_file_size).select("path", "length")
    large_count = large_files.count()
    expected_large = sum(1 for _, content in files.values() if len(content) > min_file_size)
    assert large_count == expected_large
