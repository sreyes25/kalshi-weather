from pathlib import Path

from kalshi_weather.cli.progression_graph import TemperatureProgressionViewer


def test_progression_viewer_loads_points_and_builds_chart(tmp_path: Path):
    city_dir = tmp_path / "NYC"
    city_dir.mkdir(parents=True, exist_ok=True)
    csv_path = city_dir / "2026-03-21.csv"
    csv_path.write_text(
        "\n".join(
            [
                "timestamp,city,target_date,raw_forecast_mean,final_model_mean,observed_high_f,forecast_std,final_model_std",
                "2026-03-21T09:00:00,New York City,2026-03-21,60.1,61.0,59.0,2.2,2.1",
                "2026-03-21T10:00:00,New York City,2026-03-21,60.2,61.5,60.3,2.2,2.0",
            ]
        ),
        encoding="utf-8",
    )

    viewer = TemperatureProgressionViewer(
        city_code="NYC",
        target_date="2026-03-21",
        base_dir=tmp_path,
        chart_width=20,
        chart_height=8,
    )

    points = viewer.load_points()
    assert len(points) == 2
    assert points[0].observed_high_f == 59.0
    assert points[1].final_model_mean == 61.5

    chart = viewer._build_chart_body(points)
    assert "Legend: observed_high_f=o  final_model_mean=x  overlap=*" in chart
