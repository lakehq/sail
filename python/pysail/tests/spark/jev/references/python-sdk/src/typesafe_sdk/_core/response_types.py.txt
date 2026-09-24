"""Ergonomic answer objects and response metadata, built on the generated wire schemas.

The wire models in `typesafe_sdk._schemas.models` mirror the OpenAPI schema. The public answer types
subclass them, adding immutability and integer-keyed score maps. Because the answer models are tagged,
the public `Answer` union validates by discriminator; per-answer dispatch is kept only to skip answer
kinds a future API adds without failing the whole response.
"""

from functools import cached_property
from typing import Annotated, Any, Literal, TypeAlias

import httpx2
from pydantic import ConfigDict, Field, ValidationError
from typing_extensions import Self, override

from typesafe_sdk._core.json import deserialize, serialize
from typesafe_sdk._core.logging import logger
from typesafe_sdk._core.schemas.base import Response, Schema, format_error_path, format_path, validation_error
from typesafe_sdk._schemas import models as wire


class NoulAnswer(wire.NoulAnswer):
    """A yes/no answer.

    See the [noul primitive](https://docs.typesafe.ai/primitives/noul) for details.
    """

    model_config = ConfigDict(extra="ignore", frozen=True, strict=True)

    type: Literal["noul"] = "noul"  # pyrefly: ignore[bad-override]


class ChoiceAnswer(wire.ChoiceAnswer):
    """A selected label and its probabilities.

    See the [choice primitive](https://docs.typesafe.ai/primitives/choice) for details.
    """

    model_config = ConfigDict(extra="ignore", frozen=True, strict=True)

    type: Literal["choice"] = "choice"  # pyrefly: ignore[bad-override]


class ScoreAnswer(wire.ScoreAnswer):
    """An expected score with its rubric and probabilities.

    See the [score primitive](https://docs.typesafe.ai/primitives/score) for details.
    """

    model_config = ConfigDict(extra="ignore", frozen=True, strict=True)

    type: Literal["score"] = "score"  # pyrefly: ignore[bad-override]
    # JSON object keys are strings; `dict[int, ...]` tells Pydantic to coerce them to the integer score
    # levels. `Any` (not the recursive JSON value) keeps the nested values decodable.
    legend: dict[int, str | dict[str, Any] | list[Any]]  # pyrefly: ignore[bad-override]
    """Rubric descriptions keyed by integer score."""
    probabilities: dict[int, float]  # pyrefly: ignore[bad-override]
    """Probabilities keyed by integer score."""


Answer: TypeAlias = Annotated[NoulAnswer | ChoiceAnswer | ScoreAnswer, Field(discriminator="type")]
"""An answer to a single question, identified by its `type`."""


class Usage(wire.Usage):
    """Token counts for a request, when reported by the API."""

    model_config = ConfigDict(extra="ignore", frozen=True, strict=True)

    input_tokens: int | None = None  # pyrefly: ignore[bad-override]
    """Number of input tokens used, or `None` when the API did not report it."""
    output_tokens: int | None = None  # pyrefly: ignore[bad-override]
    """Number of output tokens used, or `None` when the API did not report it."""


_ANSWER_TYPES = {"noul", "choice", "score"}


def _prepare_system_one_response(response: httpx2.Response, answer_fields: set[str]) -> dict[str, Any]:
    """Validate answers, drop future types, and lift declared answer fields into top-level keys."""
    decoded = deserialize(response.content)
    if not isinstance(decoded, dict):
        raise validation_error(response, "")
    answers = decoded.get("answers")
    if isinstance(answers, dict):
        for name, raw in list(answers.items()):
            if not isinstance(raw, dict) or not isinstance(raw.get("type"), str):
                raise validation_error(response, f"answers.{name}.type")
            if raw["type"] not in _ANSWER_TYPES:
                # Forward-compat: ignore answer types this SDK version does not model. The raw payload
                # is still available through `response.raw_http_response`.
                logger.warning("Ignoring answer %r with unrecognized type %r", name, raw["type"])
                del answers[name]
        for answer_field in answer_fields & answers.keys():
            decoded[answer_field] = answers[answer_field]
    return decoded


class SystemOneResponse(Response):
    """Answers grouped by question type with model and usage metadata.

    See [System One](https://docs.typesafe.ai/concepts/system-one) for details.
    """

    model: str
    """The model used to answer the request."""
    usage: Usage
    """Token usage for the request."""
    answers: dict[str, Answer] = Field(default_factory=dict)
    """All answer objects keyed by question name."""

    @cached_property
    def nouls(self) -> dict[str, NoulAnswer]:
        """Yes/no answers keyed by question name."""
        return {name: answer for name, answer in self.answers.items() if isinstance(answer, NoulAnswer)}

    @cached_property
    def choices(self) -> dict[str, ChoiceAnswer]:
        """Choice answers keyed by question name."""
        return {name: answer for name, answer in self.answers.items() if isinstance(answer, ChoiceAnswer)}

    @cached_property
    def scores(self) -> dict[str, ScoreAnswer]:
        """Score answers keyed by question name."""
        return {name: answer for name, answer in self.answers.items() if isinstance(answer, ScoreAnswer)}

    @classmethod
    @override
    def _decode(cls, response: httpx2.Response) -> Self:
        answer_fields = set(cls.model_fields) - set(SystemOneResponse.model_fields)
        decoded = _prepare_system_one_response(response, answer_fields)
        try:
            return cls.model_validate_json(serialize(decoded))
        except ValidationError as error:
            location = list(error.errors(include_url=False)[0]["loc"])
            match location:
                case ["answers", _, answer_type, *_] if answer_type in _ANSWER_TYPES:
                    del location[2]
            raise validation_error(response, format_path(location)) from error


class ModelMetadata(Schema):
    """Metadata describing a single available model."""

    name: str
    """Model name or alias accepted by a request's model field."""
    description: str
    """Human-readable description of the model and its capabilities."""
    release_date: str
    """Model release date, formatted as YYYY-MM-DD."""


class ListModelsResponse(Response):
    """The models available to the account."""

    models: tuple[ModelMetadata, ...]
    """The available models."""

    @classmethod
    @override
    def _decode(cls, response: httpx2.Response) -> Self:
        try:
            return cls.model_validate_json(response.content)
        except ValidationError as error:
            raise validation_error(response, format_error_path((), error)) from error
