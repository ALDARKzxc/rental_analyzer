from datetime import date
from pathlib import Path
from types import SimpleNamespace

from openpyxl import load_workbook

from app.backend import deep_analysis as da
from app.backend.deep_analysis_export import (
    build_property_export_result,
    write_deep_analysis_xlsx,
)


def test_deep_analysis_xlsx_contains_matrix_filters_and_details():
    start = date(2026, 4, 30)
    date_pairs = da.generate_date_pairs(start, window=5)

    rows = []
    states = []
    reasons = []
    for ci, co in date_pairs:
        nights = (co - ci).days
        if ci == start and nights == 1:
            status = da._ROW_PRICED
            price = 4200
        elif ci > start and nights == 2:
            status = da._ROW_PRICED
            price = 7600
        else:
            status = da._ROW_SOLD_OUT
            price = None
        states.append(status)
        rows.append(da._format_row("Test object", ci, co, status=status, price=price))
        reasons.append("test")

    prop = SimpleNamespace(
        id=10,
        title="Test object",
        url="https://example.com/object",
        site="ostrovok",
        category="Apartment",
        address="Test address",
        guest_capacity=2,
        preview_path=None,
        is_own=False,
        description="Nice place",
        key_facts='["35 sqm", "2 guests"]',
        amenities='{"Amenities": ["Wi-Fi", "Parking"]}',
    )

    result = build_property_export_result(
        prop=prop,
        date_pairs=date_pairs,
        rows=rows,
        states=states,
        reasons=reasons,
    )
    path = Path("data") / "test_deep_analysis_export.xlsx"
    if path.exists():
        path.unlink()

    try:
        write_deep_analysis_xlsx(path, [result], date_pairs)

        wb = load_workbook(path)
        assert wb.sheetnames == ["Цены", "Детализация"]

        matrix = wb["Цены"]
        assert matrix.auto_filter.ref == "A1:H2"
        assert matrix.freeze_panes == "E2"
        assert [matrix.cell(1, col).value for col in range(1, 6)] == [
            "Объект",
            "Фото",
            "Описание",
            "Перечень услуг",
            "30.04",
        ]
        assert matrix["A2"].value == "Test object"
        assert matrix["E2"].value == 4200
        assert matrix["F2"].value == 7600

        details = wb["Детализация"]
        assert details.auto_filter.ref == "A1:K11"
        assert details["A2"].value == "Test object"
        assert details["I2"].value in (4200, 4200.0, None)
    finally:
        if path.exists():
            path.unlink()
