import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from dce_to_input import convert_dce_json
from valhalla.readers import PlainTextReader


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "discord_embeds" / "valhalla_format_variants.json"


def test_all_embed_variants_are_rendered_as_messages(tmp_path):
    output_text, _ = convert_dce_json(FIXTURE)
    output_path = tmp_path / "variants.txt"
    output_path.write_text(output_text, encoding="utf-8")

    messages = PlainTextReader(str(output_path)).read()

    assert len(messages) == 45
    assert "Opened \u00b7 DLMM \u00b7 BidAsk 1-Sided" in output_text
    assert "STONK-SOL [https://dexscreener.com/solana/" in output_text
    assert "Valhalla \u00b7 HGh5...nXn9" in output_text
    gen3_output = output_text[output_text.index("Opened \u00b7 DLMM"):]
    assert "Starting SOL balance" not in gen3_output



def test_gen2_open_view_tx_uses_pipe_order_for_attribution(tmp_path):
    output_text, _ = convert_dce_json(FIXTURE)
    output_path = tmp_path / "variants.txt"
    output_path.write_text(output_text, encoding="utf-8")

    message = next(
        item for item in PlainTextReader(str(output_path)).read()
        if "Opened New DLMM Position! (4jbU...hYhE)" in item.clean_text
    )

    assert message.bot_tx_signatures == [
        "uRfD1B3FxHqiV2qRLEBkxR2YXe3As72f1YMAidsocUBsW9nb61Go5obv8W9yhqdzckSid2CMYXtLZt8pqDNQJ8a"
    ]
    assert message.target_tx_signatures == [
        "4DrwPytw3oVLuiehDvMXvhtf92drMb7vmhJ85PJ3WTwQa2wkdi1VFBoCNdTqMMDAsLzk2nn6AjfSA9hvTxD17GCx"
    ]
def test_multiple_embeds_are_separated_and_keep_all_literal_components(tmp_path):
    payload = {
        "messages": [{
            "timestamp": "2026-08-23T15:31:00+00:00",
            "author": {"name": "Valhalla Bot"},
            "content": "",
            "attachments": [],
            "embeds": [
                {"author": {"name": "First"}, "title": "A", "url": "https://a.test", "description": "[Orb](<https://orb.test/a>)"},
                {"author": {"name": "Second"}, "footer": {"text": "Footer"}},
            ],
        }]
    }
    source = tmp_path / "multi.json"
    source.write_text(json.dumps(payload), encoding="utf-8")

    output_text, _ = convert_dce_json(source, author_prefix="")

    assert "First\nA [https://a.test]\nOrb [https://orb.test/a]\n\nSecond\nFooter" in output_text


def test_empty_conversion_exits_three_and_reports_embed_authors(tmp_path):
    source = tmp_path / "empty.json"
    source.write_text(json.dumps({"messages": [{
        "timestamp": "2026-08-23T15:31:00+00:00",
        "author": {"name": "Valhalla Bot"},
        "content": "",
        "attachments": [],
        "embeds": [{}],
    }]}), encoding="utf-8")

    result = subprocess.run(
        [sys.executable, str(ROOT / "dce_to_input.py"), str(source), "--out", str(tmp_path / "out.txt")],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert result.returncode == 3
    assert "1 messages" in result.stderr
    assert "<none>" in result.stderr
    assert not (tmp_path / "out.txt").exists()

def test_converter_failure_retains_json_and_pipeline_aborts_before_recalc(tmp_path, monkeypatch, capsys):
    import dce_pull
    import run_pipeline

    fake_root = tmp_path / "project"
    fake_root.mkdir()
    (fake_root / "DiscordChatExporter.Cli.win-x64").mkdir()
    (fake_root / "DiscordChatExporter.Cli.win-x64" / "DiscordChatExporter.Cli.exe").touch()
    monkeypatch.setenv("DCE_TOKEN", "token")
    monkeypatch.setenv("DISCORD_CHANNEL_ID", "123")
    monkeypatch.setattr(dce_pull, "Path", lambda _: fake_root / "dce_pull.py")

    calls = []

    def fake_run(command, **_kwargs):
        calls.append(command)
        if len(calls) == 1:
            Path(command[-1]).write_text('{"messages": []}', encoding="utf-8")
            return subprocess.CompletedProcess(command, 0, "", "")
        return subprocess.CompletedProcess(command, 3, "", "converter failed")

    monkeypatch.setattr(dce_pull.subprocess, "run", fake_run)
    monkeypatch.setattr(sys, "argv", ["dce_pull.py", "--after", "2026-08-23"])
    try:
        dce_pull.main()
    except SystemExit as exc:
        assert exc.code == 3
    else:
        raise AssertionError("dce_pull.main() should propagate converter failure")

    retained = Path(calls[0][-1])
    assert retained.exists()
    assert str(retained) in capsys.readouterr().err

    pipeline_calls = []
    monkeypatch.setattr(run_pipeline, "_load_last_pull", lambda: datetime.now(timezone.utc))
    monkeypatch.setattr(run_pipeline, "_run", lambda label, *_args, **_kwargs: pipeline_calls.append(label) or 3)
    monkeypatch.setattr(run_pipeline, "_save_last_pull", lambda _dt: None)
    monkeypatch.setattr(sys, "argv", ["run_pipeline.py"])
    try:
        run_pipeline.main()
    except SystemExit as exc:
        assert exc.code == 3
    else:
        raise AssertionError("run_pipeline.main() should abort after pull failure")

    assert len(pipeline_calls) == 1
    assert pipeline_calls[0].startswith("Discord pull")
