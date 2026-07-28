#!/usr/bin/env python
"""List contributors for a commit range."""

import argparse
import json
import shlex
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPOSITORY = "lakehq/sail"
ORGANIZATION = "lakehq"
REPOSITORY_ROOT_PATH = Path(__file__).resolve().parents[2]


class ContributorError(Exception):
    pass


@dataclass(frozen=True)
class GitCommit:
    sha: str
    email: str
    additions: int
    deletions: int


@dataclass
class Contribution:
    commits: int = 0
    additions: int = 0
    deletions: int = 0

    @property
    def lines(self) -> int:
        return self.additions + self.deletions

    def add(self, commit: GitCommit) -> None:
        self.commits += 1
        self.additions += commit.additions
        self.deletions += commit.deletions


@dataclass
class GitHubUser:
    login: str
    is_bot: bool


@dataclass
class Contributor:
    contribution: Contribution
    user: GitHubUser
    is_first_time: bool
    is_member: bool


def run(command: list[str]) -> str:
    code, stdout, stderr = run_unchecked(command)
    if code != 0:
        message = f"`{shlex.join(command)}` failed: stdout: {stdout.strip()} stderr: {stderr.strip()}"
        raise ContributorError(message)
    return stdout


def run_unchecked(command: list[str]) -> tuple[int, str, str]:
    sys.stderr.write(f"{shlex.join(command)}\n")
    result = subprocess.run(command, cwd=REPOSITORY_ROOT_PATH, capture_output=True, text=True, check=False)
    return result.returncode, result.stdout, result.stderr


def parse_json(value: str) -> Any:
    try:
        return json.loads(value) if value.strip() else None
    except json.JSONDecodeError as error:
        message = f"invalid JSON: {value}"
        raise ContributorError(message) from error


def parse_numstat(value: str) -> int:
    return 0 if value == "-" else int(value)


def collect_git_commits(commits: str) -> list[GitCommit]:
    output = run(["git", "log", "--format=%x1e%H%x1f%ae", "--numstat", commits])
    parsed_commits = []

    for record in output.split("\x1e"):
        lines = [line for line in record.splitlines() if line]
        if not lines:
            continue

        try:
            sha, email = lines[0].split("\x1f", maxsplit=1)
        except ValueError as error:
            message = f"could not parse git log record: {lines[0]!r}"
            raise ContributorError(message) from error

        additions = 0
        deletions = 0

        for line in lines[1:]:
            parts = line.split("\t", maxsplit=2)
            if len(parts) == 3:  # noqa: PLR2004
                additions += parse_numstat(parts[0])
                deletions += parse_numstat(parts[1])

        parsed_commits.append(GitCommit(sha=sha, email=email, additions=additions, deletions=deletions))

    return parsed_commits


def first_commit(commits: str) -> str:
    lines = run(["git", "log", "--reverse", "--format=%H", commits]).splitlines()
    if not lines:
        message = f"no commits found for range {commits!r}"
        raise ContributorError(message)
    return lines[0]


def commit_date(commit: str) -> str:
    return run(["git", "show", "-s", "--format=%cI", commit]).strip()


def lookup_commit_user(commit: GitCommit) -> GitHubUser:
    command = [
        "gh",
        "api",
        f"repos/{REPOSITORY}/commits/{commit.sha}/pulls",
        "-X",
        "GET",
        "-H",
        "Accept: application/vnd.github+json",
        "-F",
        "per_page=1",
    ]
    pulls = parse_json(run(command))
    user = pulls[0].get("user") if pulls else None
    if user is None:
        message = f"could not resolve a pull request author for commit {commit.sha}"
        raise ContributorError(message)
    return GitHubUser(
        login=user["login"],
        is_bot=user.get("type") == "Bot" or user["login"].endswith("[bot]"),
    )


def is_first_time_contributor(login: str, before: str) -> bool:
    command = [
        "gh",
        "api",
        "search/commits",
        "-X",
        "GET",
        "-f",
        f"q=repo:{REPOSITORY} author:{login} committer-date:<{before}",
        "-F",
        "per_page=1",
    ]
    data = parse_json(run(command))
    return not data.get("total_count", 0)


def is_organization_member(login: str) -> bool:
    command = ["gh", "api", f"orgs/{ORGANIZATION}/members/{login}"]
    code, stdout, stderr = run_unchecked(command)
    data = parse_json(stdout)
    if code == 0:
        return True
    if isinstance(data, dict) and data.get("status") == "404":
        return False
    message = f"failed to check organization membership for {login}: data: {data!r} stderr: {stderr.strip()}"
    raise ContributorError(message)


def collect_contributors(commits: str) -> list[Contributor]:
    before = commit_date(first_commit(commits))
    contributors: dict[str, Contributor] = {}
    users_by_email: dict[str, GitHubUser] = {}

    for commit in collect_git_commits(commits):
        email = commit.email.casefold()
        user = users_by_email.get(email)
        if user is None:
            user = lookup_commit_user(commit)
            users_by_email[email] = user
        key = user.login.casefold()
        contributor = contributors.get(key)
        if contributor is None:
            # The GitHub commits API does not reliably find bot-authored commits, so skip this check for bots.
            is_first_time = False if user.is_bot else is_first_time_contributor(user.login, before)
            is_member = False if user.is_bot else is_organization_member(user.login)
            contributors[key] = Contributor(
                contribution=Contribution(),
                user=user,
                is_first_time=is_first_time,
                is_member=is_member,
            )
            contributor = contributors[key]
        contributor.contribution.add(commit)
    return list(contributors.values())


def contributor_sort_key(contributor: Contributor) -> tuple[int, int, str]:
    return (
        -contributor.contribution.commits,
        -contributor.contribution.lines,
        contributor.user.login.casefold(),
    )


def row(contributor: Contributor) -> str:
    contribution = contributor.contribution
    first_time = " (_first-time contributor_)" if contributor.is_first_time else ""
    label = f"@{contributor.user.login}"
    url = f"https://github.com/{contributor.user.login}"
    commit_word = "commit" if contribution.commits == 1 else "commits"
    return (
        f"* ({contribution.commits} {commit_word}, "
        f"+{contribution.additions} -{contribution.deletions}) [{label}]({url}){first_time}"
    )


def grouped_contributors(contributors: list[Contributor]) -> dict[str, list[Contributor]]:
    sections: dict[str, list[Contributor]] = defaultdict(list)
    for contributor in contributors:
        if contributor.user.is_bot:
            sections["Bots"].append(contributor)
        elif contributor.is_member:
            sections["Members"].append(contributor)
        else:
            sections["Community Contributors"].append(contributor)
    return sections


def report(contributors: list[Contributor]) -> str:
    sections = grouped_contributors(contributors)
    lines = []
    for title in ["Members", "Community Contributors", "Bots"]:
        lines.append(f"## {title}")
        rows = [row(contributor) for contributor in sorted(sections[title], key=contributor_sort_key)]
        lines.extend(rows or ["(none)"])
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--commits", required=True, help="commit range passed to `git log`")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        sys.stdout.write(report(collect_contributors(args.commits)))
        sys.stdout.flush()
    except ContributorError as error:
        sys.stderr.write(f"error: {error}\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
