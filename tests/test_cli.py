from gbif_data_pipeline.cli import main


def test_cli_help(monkeypatch, capsys):
    monkeypatch.setattr(
        "sys.argv",
        ["gbif-pipeline", "--help"],
    )

    try:
        main()
    except SystemExit as exc:
        assert exc.code == 0

    captured = capsys.readouterr()

    assert "GBIF occurrence data pipeline" in captured.out
    assert "--scientific-name" in captured.out