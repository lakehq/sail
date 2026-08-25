"""Sail diagnostics for Spark Connect DataFrames."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
from pyspark.sql.connect.plan import LogicalPlan

_EXPLAIN_RELATION_TYPE_URL = "type.googleapis.com/sail.spark.connect.v1.ExplainRelation"
_DISTRIBUTED_TYPE = 1
_JSON_FORMAT = 2


def _encode_varint(value: int) -> bytes:
    output = bytearray()
    while value >= 0x80:  # noqa: PLR2004
        output.append((value & 0x7F) | 0x80)
        value >>= 7
    output.append(value)
    return bytes(output)


def _encode_varint_field(number: int, value: int) -> bytes:
    return _encode_varint(number << 3) + _encode_varint(value)


def _encode_message_field(number: int, value: bytes) -> bytes:
    return _encode_varint((number << 3) | 2) + _encode_varint(len(value)) + value


def _encode_explain_relation(input_relation: Any, *, analyze: bool, verbose: bool) -> bytes:
    options = _encode_varint_field(1, _DISTRIBUTED_TYPE)
    options += _encode_varint_field(2, _JSON_FORMAT)
    if verbose:
        options += _encode_varint_field(3, 1)
    if analyze:
        options += _encode_varint_field(4, 1)
    return _encode_message_field(1, input_relation.SerializeToString()) + _encode_message_field(2, options)


class _DistributedExplainPlan(LogicalPlan):
    def __init__(self, child: LogicalPlan, *, analyze: bool, verbose: bool) -> None:
        super().__init__(child)
        self._analyze = analyze
        self._verbose = verbose

    def plan(self, session: Any) -> Any:
        relation = self._create_proto_relation()
        input_relation = self._child.plan(session)
        relation.extension.type_url = _EXPLAIN_RELATION_TYPE_URL
        relation.extension.value = _encode_explain_relation(
            input_relation,
            analyze=self._analyze,
            verbose=self._verbose,
        )
        return relation


@dataclass(frozen=True)
class DistributedStage:
    """A stage in a version 1 distributed plan."""

    id: int
    placement: str
    partitions: int
    output_mode: str
    distribution: dict[str, Any]
    operator_tree: str


@dataclass(frozen=True)
class DistributedEdge:
    """An exchange edge in a version 1 distributed plan."""

    from_stage: int
    to_stage: int
    exchange_kind: str
    distribution: dict[str, Any]
    channel_count: int


class ExplainReport:
    """A structured distributed explain result."""

    def __init__(self, model: dict[str, Any], output_format: str, *, verbose: bool) -> None:
        self._model = model
        self._output_format = output_format
        self._verbose = verbose
        self.stages = tuple(
            DistributedStage(
                id=stage["stage_id"],
                placement=stage["placement"],
                partitions=stage["partition_count"],
                output_mode=stage["output_mode"],
                distribution=stage["distribution"],
                operator_tree=stage["operator_tree"],
            )
            for stage in model["stages"]
        )
        self.edges = tuple(
            DistributedEdge(
                from_stage=edge["from_stage"],
                to_stage=edge["to_stage"],
                exchange_kind=edge["exchange_kind"],
                distribution=edge["distribution"],
                channel_count=edge["channel_count"],
            )
            for edge in model["edges"]
        )

    @property
    def schema_version(self) -> int:
        return self._model["schema_version"]

    @property
    def execution_mode(self) -> str:
        return self._model["execution_mode"]

    @property
    def executed(self) -> bool:
        return self._model["executed"]

    @property
    def job_id(self) -> int | None:
        execution = self._model.get("execution")
        return None if execution is None else execution["job_id"]

    @property
    def metrics(self) -> dict[str, int]:
        execution = self._model.get("execution")
        return {} if execution is None else dict(execution["metrics"])

    @property
    def text(self) -> str:
        """Render the report in the format requested by :func:`explain`."""
        if self._output_format == "json":
            return json.dumps(self._model, indent=2)
        if self._output_format == "graphviz":
            return self._render_graphviz()
        return self._render_text()

    def _render_text(self) -> str:
        lines = [
            "Distributed Plan",
            f"execution_mode={self.execution_mode.replace('_', '-')}",
            f"executed={str(self.executed).lower()}",
        ]
        if self.job_id is not None:
            lines.append(f"job_id={self.job_id}")
        for stage in self.stages:
            inputs = ", ".join(
                f"StageInput(stage={edge.from_stage}, mode={edge.exchange_kind.capitalize()})"
                for edge in self.edges
                if edge.to_stage == stage.id
            )
            lines.extend(
                [
                    "",
                    f"=== stage {stage.id} ===",
                    f"inputs=[{inputs}]",
                    f"placement={stage.placement}",
                    f"partitions={stage.partitions}",
                    f"output_mode={stage.output_mode}",
                    f"distribution={_format_distribution(stage.distribution)}",
                ]
            )
            if self._verbose:
                lines.append(stage.operator_tree.rstrip())
        if self.edges:
            lines.extend(["", "=== exchanges ==="])
            lines.extend(
                (
                    f"stage {edge.from_stage} -> stage {edge.to_stage}: "
                    f"kind={edge.exchange_kind}, distribution={_format_distribution(edge.distribution)}, "
                    f"channels={edge.channel_count}"
                )
                for edge in self.edges
            )
        return "\n".join(lines)

    def _render_graphviz(self) -> str:
        lines = ["digraph distributed_plan {", "  rankdir=LR;"]
        for stage in self.stages:
            label = (
                f"stage {stage.id}\nplacement={stage.placement}\npartitions={stage.partitions}"
                f"\noutput_mode={stage.output_mode}"
                f"\ndistribution={_format_distribution(stage.distribution)}"
            )
            if self._verbose:
                label += f"\n{stage.operator_tree.rstrip()}"
            lines.append(f'  stage_{stage.id} [shape=box, label="{_escape_graphviz(label)}"];')
        for edge in self.edges:
            label = f"{edge.exchange_kind} / {_format_distribution(edge.distribution)} / {edge.channel_count} channels"
            lines.append(f'  stage_{edge.from_stage} -> stage_{edge.to_stage} [label="{_escape_graphviz(label)}"];')
        lines.append("}")
        return "\n".join(lines)


def _format_distribution(distribution: dict[str, Any]) -> str:
    kind = distribution["kind"]
    if kind == "hash":
        return f"hash({', '.join(distribution['keys'])})"
    return kind


def _escape_graphviz(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\l")


def _dataframe_for_plan(plan: LogicalPlan, dataframe: ConnectDataFrame) -> ConnectDataFrame:
    if hasattr(ConnectDataFrame, "withPlan"):
        return ConnectDataFrame.withPlan(plan, dataframe._session)  # type: ignore[attr-defined]  # noqa: SLF001
    return ConnectDataFrame(plan, dataframe._session)  # noqa: SLF001


def explain(
    dataframe: Any,
    *,
    type: str = "distributed",  # noqa: A002
    format: str = "text",  # noqa: A002
    analyze: bool = False,
    verbose: bool = False,
) -> ExplainReport:
    """Return Sail's versioned distributed plan for a Spark Connect DataFrame.

    The input DataFrame is never modified, and this function does not alter or
    monkeypatch :meth:`DataFrame.explain`.
    """
    explain_type = type.lower()
    output_format = format.lower()
    if explain_type != "distributed":
        msg = f"unsupported explain type: {type!r}"
        raise ValueError(msg)
    if output_format not in {"text", "json", "graphviz"}:
        msg = f"unsupported explain format: {format!r}"
        raise ValueError(msg)
    if not isinstance(dataframe, ConnectDataFrame):
        msg = "pysail diagnostics require a Spark Connect DataFrame"
        raise TypeError(msg)
    if not isinstance(analyze, bool) or not isinstance(verbose, bool):
        msg = "analyze and verbose must be bool values"
        raise TypeError(msg)

    plan = _DistributedExplainPlan(dataframe._plan, analyze=analyze, verbose=verbose)  # noqa: SLF001
    result = _dataframe_for_plan(plan, dataframe).collect()
    if len(result) != 1:
        msg = f"distributed explain returned {len(result)} rows; expected exactly one"
        raise RuntimeError(msg)
    model = json.loads(result[0]["plan"])
    if model.get("schema_version") != 1:
        msg = f"unsupported distributed plan schema version: {model.get('schema_version')!r}"
        raise ValueError(msg)
    return ExplainReport(model, output_format, verbose=verbose)


__all__ = [
    "DistributedEdge",
    "DistributedStage",
    "ExplainReport",
    "explain",
]
