from pysail.testing.spark.utils.plan import normalize_plan_text


def test_replay_scopes_keep_ordinals_and_repeated_identity():
    first = "01234567-0123-0123-0123-0123456789ab"
    second = "fedcba98-7654-3210-7654-321098765432"
    text = f"Replay: {first}:0\nReplayExec: {first}:1\nreplay={second}:0\nreplay={first}:1"
    expected = (
        "Replay: <replay_scope_0>:0\nReplayExec: <replay_scope_0>:1\n"
        "replay=<replay_scope_1>:0\nreplay=<replay_scope_0>:1"
    )
    assert normalize_plan_text(text) == expected
    assert (
        normalize_plan_text(text.replace(first, second.upper()).replace(f"replay={second}:0", f"replay={first}:0"))
        == expected
    )
    assert normalize_plan_text(f"Literal({first})") == f"Literal({first})"
