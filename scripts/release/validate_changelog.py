#!/usr/bin/env python
"""Validate release changelog entries against Git history."""

import argparse
import re
import shlex
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

REPOSITORY_ROOT_PATH = Path(__file__).resolve().parents[2]
CHANGELOG_PATH = REPOSITORY_ROOT_PATH / Path("docs/reference/changelog/index.md")
GITHUB_PULL_URL_PATTERN = re.compile(r"^https://github\.com/lakehq/sail/pull/(\d+)(?:[/?#][^)\s]*)?$")
GITHUB_PULL_REF_PATTERN = re.compile(r"\(#(\d+)\)$")
MARKDOWN_HEADING_PATTERN = re.compile(r"^##\s+(.+?)\s*$")
MARKDOWN_LINK_PATTERN = re.compile(r"\[([^\]]+)]\(([^)]+)\)")


class ValidationError(Exception):
    pass


@dataclass
class ChangelogLine:
    line_number: int
    text: str


@dataclass
class Commit:
    title: str
    pull_request: str | None


@dataclass
class ValidationData:
    changelog_lines: list[ChangelogLine]
    changelog_pull_requests: Counter[str]
    commits: list[Commit]


def find_version_section(markdown: str, version: str) -> list[ChangelogLine]:
    lines = markdown.splitlines()
    start = None

    for index, line in enumerate(lines):
        match = MARKDOWN_HEADING_PATTERN.match(line)
        if match and match.group(1) == version:
            start = index + 1
            break

    if start is None:
        message = f"version {version!r} was not found in the changelog"
        raise ValidationError(message)

    end = len(lines)
    for index in range(start, len(lines)):
        if MARKDOWN_HEADING_PATTERN.match(lines[index]):
            end = index
            break

    return [ChangelogLine(line_number=index + 1, text=lines[index]) for index in range(start, end)]


def pull_requests_from_links(lines: list[ChangelogLine]) -> list[str]:
    pull_requests = []

    for line in lines:
        for text, url in MARKDOWN_LINK_PATTERN.findall(line.text):
            match = GITHUB_PULL_URL_PATTERN.match(url)
            if match is None:
                continue

            pull_request = match.group(1)
            expected_text = f"#{pull_request}"
            if text != expected_text:
                message = (
                    f"changelog line {line.line_number}: pull request link text {text!r} does not match URL {url!r}"
                )
                raise ValidationError(message)
            pull_requests.append(pull_request)

    return pull_requests


def git_log(commits: str) -> list[Commit]:
    command = ["git", "log", "--oneline", commits]
    result = subprocess.run(command, cwd=REPOSITORY_ROOT_PATH, capture_output=True, text=True, check=False)

    if result.returncode != 0:
        message = f"`{shlex.join(command)}` failed: stdout: {result.stdout.strip()} stderr: {result.stderr.strip()}"
        raise ValidationError(message)

    parsed_commits = []
    for line in result.stdout.splitlines():
        _, _, title = line.partition(" ")
        match = GITHUB_PULL_REF_PATTERN.search(title)
        parsed_commits.append(Commit(title=title, pull_request=match.group(1) if match else None))
    return parsed_commits


def append_section(lines: list[str], title: str, rows: list[str]) -> None:
    lines.extend([title, *(rows or ["(none)"])])


def markdown_link_text(text: str) -> str:
    return text.replace("\\", "\\\\").replace("]", "\\]")


def commit_titles_by_pull_request(commits: list[Commit]) -> dict[str, str]:
    titles = {}
    for commit in commits:
        if commit.pull_request is not None:
            titles.setdefault(commit.pull_request, commit.title)
    return titles


def annotated_changelog(lines: list[ChangelogLine], commits: list[Commit]) -> list[str]:
    titles = commit_titles_by_pull_request(commits)

    def replace_link(match: re.Match[str]) -> str:
        _, url = match.groups()
        url_match = GITHUB_PULL_URL_PATTERN.match(url)
        if url_match is None:
            return match.group(0)

        title = titles.get(url_match.group(1))
        if title is None:
            return match.group(0)
        return f"[{markdown_link_text(title)}]({url})"

    return [MARKDOWN_LINK_PATTERN.sub(replace_link, line.text) for line in lines]


def report(data: ValidationData) -> str:
    changelog_pull_request_set = set(data.changelog_pull_requests)
    known_pull_requests = {commit.pull_request for commit in data.commits if commit.pull_request is not None}

    known_rows = []
    for commit in data.commits:
        pull_request = commit.pull_request
        if pull_request is not None and pull_request in changelog_pull_request_set:
            count = data.changelog_pull_requests[pull_request]
            known_rows.append(f"* ({count}) {commit.title}")

    missing_rows = [
        f"* (0) {commit.title}" for commit in data.commits if commit.pull_request not in changelog_pull_request_set
    ]

    unknown_rows = [
        f"* ({data.changelog_pull_requests[pull_request]}) ??? (#{pull_request})"
        for pull_request in sorted(changelog_pull_request_set - known_pull_requests, key=int)
    ]

    lines = []
    append_section(lines, "## Known Commits in the Changelog", known_rows)
    lines.append("")
    append_section(lines, "## Known Commits Not in the Changelog", missing_rows)
    lines.append("")
    append_section(lines, "## Unknown Commits in the Changelog", unknown_rows)
    lines.append("")
    append_section(lines, "## Annotated Changelog", annotated_changelog(data.changelog_lines, data.commits))
    return "\n".join(lines) + "\n"


def has_unknown_commits(data: ValidationData) -> bool:
    changelog_pull_request_set = set(data.changelog_pull_requests)
    known_pull_requests = {commit.pull_request for commit in data.commits if commit.pull_request is not None}
    unknown = changelog_pull_request_set - known_pull_requests
    return bool(unknown)


def load_validation_data(version: str, commits: str) -> ValidationData:
    markdown = CHANGELOG_PATH.read_text(encoding="utf-8")
    changelog_lines = find_version_section(markdown, version)
    return ValidationData(
        changelog_lines=changelog_lines,
        changelog_pull_requests=Counter(pull_requests_from_links(changelog_lines)),
        commits=git_log(commits),
    )


def validate(version: str, commits: str) -> None:
    data = load_validation_data(version, commits)
    output = report(data)
    sys.stdout.write(output)
    sys.stdout.flush()

    if has_unknown_commits(data):
        message = "found unknown commits in the changelog"
        raise ValidationError(message)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--version", required=True, help="version heading to validate")
    parser.add_argument("--commits", required=True, help="commit range passed to `git log`")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        validate(args.version, args.commits)
    except ValidationError as error:
        sys.stderr.write(f"error: {error}\n")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
