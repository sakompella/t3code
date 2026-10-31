#!/usr/bin/env python3
"""Keep the fork as topic branches and build the integration branch from them.

FORK-TOPICS (on topic/meta) lists one topic per line, `<topic> <parent>`, where the
parent is `upstream` or an earlier topic. Line order is merge order. Topic X lives on
branch topic/X. Nothing is committed on the integration directly.

  rebuild [--no-fetch] [--same-tree-as REF]
                         rebase every topic onto its parent's new tip, then build the
                         candidate `integration/candidate` = upstream + --no-ff merge of each topic
  promote                tag the checked candidate `integration/YYYYMMDD-N` (deploy only from tags)
  land <branch> <topic>  keep the branch's original tip under archive/worker/, rebase it onto the
                         topic if the topic moved, merge it --no-ff into the topic
  collapse <topic>       replace the topic's commits by one commit with the same tree that names
                         the originals (kept under archive/topic/), and restack the topics above it
  status                 topics, their bases, the candidate and the last tag

All rebases and merges run in a dedicated worktree. A conflict that rerere cannot
resolve stops the command; resolve it in that worktree, `git rebase --continue`, and
rerun the same command. A run in progress is recorded in <git-common-dir>/fork-topics.json
and blocks land and collapse. Pre-run topic tips are kept under refs/fork-topics/<run>/.
"""

import argparse
import datetime
import json
import os
import re
import subprocess
import sys

REPO = os.environ.get("FORK_REPO", os.path.expanduser("~/repos/forks/t3code"))
WT = os.environ.get("FORK_REBUILD_WORKTREE", os.path.expanduser("~/.t3/worktrees/t3code/fork-rebuild"))
UPSTREAM = os.environ.get("FORK_UPSTREAM", "upstream/main")
CANDIDATE = "integration/candidate"
RERERE = ["-c", "rerere.enabled=true", "-c", "rerere.autoupdate=true", "-c", "merge.conflictStyle=zdiff3"]
NOW = datetime.datetime.now(datetime.timezone.utc)
TODAY = NOW.strftime("%Y%m%d")  # UTC, like the report and release names


def git(*args, cwd=REPO, check=True):
    r = subprocess.run(["git", "-C", cwd, *args], capture_output=True, text=True)
    if check and r.returncode != 0:
        sys.exit(f"git {' '.join(args)} failed:\n{r.stdout}{r.stderr}")
    return r


def out(*args, cwd=REPO):
    return git(*args, cwd=cwd).stdout.strip()


def rev(ref):
    return git("rev-parse", "--verify", "-q", ref + "^{commit}", check=False).stdout.strip() or None


def is_ancestor(a, b):
    return git("merge-base", "--is-ancestor", a, b, check=False).returncode == 0


STATE = os.path.join(out("rev-parse", "--path-format=absolute", "--git-common-dir"), "fork-topics.json")


def save(st):
    with open(STATE + ".tmp", "w") as f:
        json.dump(st, f, indent=1)
    os.replace(STATE + ".tmp", STATE)


def read_topics():
    topics = []
    for line in git("show", "topic/meta:FORK-TOPICS").stdout.splitlines():
        fields = line.split("#", 1)[0].split()
        if not fields:
            continue
        name, parent = fields
        if parent != "upstream" and parent not in dict(topics):
            sys.exit(f"FORK-TOPICS: {name}: parent {parent} must be listed before it")
        topics.append((name, parent))
    return topics


def worktree():
    if not os.path.isdir(WT):
        git("worktree", "add", "--detach", WT, UPSTREAM)
    gd = out("rev-parse", "--absolute-git-dir", cwd=WT)
    if any(os.path.exists(os.path.join(gd, p)) for p in ("rebase-merge", "rebase-apply", "MERGE_HEAD")):
        sys.exit(f"a rebase or merge is in progress in {WT}; finish it (`git rebase --continue`) and rerun")


def rebase(what, *args, log):
    """Rebase in the worktree; continue past stops rerere resolved completely, else stop."""
    r = subprocess.run(["git", "-C", WT, *RERERE, "rebase", *args], capture_output=True, text=True)
    gd = out("rev-parse", "--absolute-git-dir", cwd=WT)
    while os.path.exists(os.path.join(gd, "rebase-merge")):
        log += re.findall(r"(?:Resolved|Staged) '([^']+)' using previous resolution", r.stdout + r.stderr)
        unresolved = git("diff", "--name-only", "--diff-filter=U", cwd=WT).stdout.split()
        if unresolved:
            sys.exit(f"STOP in {what}: resolve {' '.join(unresolved)} in {WT}, `git rebase --continue`, rerun.")
        git("add", "-u", cwd=WT)
        r = subprocess.run(
            ["git", "-C", WT, *RERERE, "-c", "core.editor=true", "rebase", "--continue"], capture_output=True, text=True
        )
        if (
            r.returncode != 0
            and not git("diff", "--name-only", "--diff-filter=U", cwd=WT).stdout.split()
            and os.path.exists(os.path.join(gd, "rebase-merge"))
        ):
            sys.exit(f"STOP in {what}: git could not continue:\n{r.stdout}{r.stderr}")
    if r.returncode != 0 and "CONFLICT" not in r.stdout + r.stderr:
        sys.exit(f"{what} failed:\n{r.stdout}{r.stderr}")
    log += re.findall(r"(?:Resolved|Staged) '([^']+)' using previous resolution", r.stdout + r.stderr)


def start_run(upstream_tip, kind):
    topics = read_topics()
    run = f"{TODAY}-{NOW:%H%M%S}"
    old = {t: rev(f"topic/{t}") for t, _ in topics}
    st = {
        "run": run,
        "kind": kind,
        "upstream": upstream_tip,
        "topics": topics,
        "old": old,
        "done": [],
        "rerere": [],
        "merge_edits": [],
    }
    for t, tip in old.items():
        git("update-ref", f"refs/fork-topics/{run}/{t}", tip)
    save(st)
    return st


def restack(st):
    """Rebase each topic onto its parent's current tip; finished topics are skipped on rerun."""

    def new_tip(p):
        return st["upstream"] if p == "upstream" else rev(f"topic/{p}")

    for t, p in st["topics"]:
        if t in st["done"]:
            continue
        if rev(f"topic/{t}") == st["old"][t]:
            old_base = out("merge-base", st["old"][t], st["upstream"] if p == "upstream" else st["old"][p])
            st.setdefault("review", {})[t] = (
                f"git range-diff {old_base[:12]}..{st['old'][t][:12]} {new_tip(p)[:12]}..topic/{t}"
            )
            save(st)
            if old_base != new_tip(p):
                rebase(
                    f"rebase of topic/{t}",
                    "--rebase-merges",
                    "--onto",
                    new_tip(p),
                    old_base,
                    f"topic/{t}",
                    log=st["rerere"],
                )
        if not is_ancestor(new_tip(p), f"topic/{t}"):
            sys.exit(f"topic/{t} does not contain {p}'s new tip; fix topic/{t} in {WT} and rerun")
        # a replayed merge that differs from the automatic merge of its parents carries a hand edit
        for m in out("rev-list", "--merges", f"{new_tip(p)}..topic/{t}").split():
            auto = git("merge-tree", "--write-tree", f"{m}^1", f"{m}^2", check=False).stdout.split("\n")[0]
            if auto != out("rev-parse", f"{m}^{{tree}}"):
                st["merge_edits"].append(f"topic/{t} {m[:10]}")
        st["done"].append(t)
        save(st)


def rebuild(args):
    worktree()
    if os.path.exists(STATE):
        st = json.load(open(STATE))
    else:
        if not args.no_fetch:
            git("fetch", "--quiet", UPSTREAM.split("/")[0])
        st = start_run(rev(UPSTREAM), "rebuild")
    restack(st)
    inputs = [st["upstream"]] + [rev(f"topic/{t}") for t, _ in st["topics"]]
    if rev(CANDIDATE) and candidate_inputs() == inputs:
        os.remove(STATE)
        print(f"candidate unchanged: {rev(CANDIDATE)}")
        return
    git("checkout", "--quiet", "--detach", st["upstream"], cwd=WT)
    for t, _ in st["topics"]:
        r = git("merge", "--no-ff", "--no-verify", "-m", f"Merge topic {t}", f"topic/{t}", cwd=WT, check=False)
        if r.returncode != 0:
            git("merge", "--abort", cwd=WT, check=False)
            sys.exit(
                f"merging topic/{t} into the integration conflicts. Topics must not conflict with each other: "
                f"move the overlapping change into one topic, or stack topic/{t} on the topic it overlaps. "
                f"Then rerun."
            )
    head = out("rev-parse", "HEAD", cwd=WT)
    if (
        args.same_tree_as
        and git("diff", "--quiet", args.same_tree_as, head, "--", ".", ":!FORK-TOPICS", check=False).returncode != 0
    ):
        sys.exit(
            f"the rebuilt integration {head[:10]} differs from {args.same_tree_as}; a carve or re-parent must not change the tree "
            f"(`git diff {args.same_tree_as} {head}`). The run state is kept; fix the topics and rerun."
        )
    git("branch", "-f", CANDIDATE, head)
    os.replace(STATE, STATE.replace(".json", "-last.json"))
    print(f"candidate {CANDIDATE} = {head} (run {st['run']}, pre-run tips under refs/fork-topics/{st['run']}/)")
    for t, cmd in st.get("review", {}).items():
        print(f"  review topic/{t}: {cmd}")
    for f in sorted(set(st["rerere"])):
        print(f"  rerere reused a resolution in {f}: review it")
    for m in st["merge_edits"]:
        print(f"  replayed merge with a hand edit: {m}: compare with its original")


def candidate_inputs():
    merges = out("log", "--first-parent", "--reverse", "--format=%P", f"{UPSTREAM}..{CANDIDATE}").splitlines()
    return [out("merge-base", CANDIDATE, UPSTREAM)] + [m.split()[1] for m in merges if len(m.split()) == 2]


def tags():
    return git("tag", "-l", "integration/2*", "--sort=-version:refname").stdout.split()


def promote(args):
    cand = rev(CANDIDATE)
    last = tags()[0] if tags() else None
    if (
        last
        and rev(last)
        and out("rev-parse", f"{last}^{{tree}}") == out("rev-parse", f"{cand}^{{tree}}")
        and git("rev-list", "--count", f"{last}..{cand}").stdout.strip() == "0"
    ):
        print(f"{last} is already this build")
        return
    n = 1 + sum(1 for t in tags() if t.startswith(f"integration/{TODAY}-"))
    tag = f"integration/{TODAY}-{n}"
    parents = out("log", "--first-parent", "--format=%P", f"{UPSTREAM}..{cand}").splitlines()
    body = "".join(f"{p.split()[1]}\n" for p in parents if len(p.split()) == 2)
    git(
        "tag",
        "-a",
        tag,
        cand,
        "-m",
        f"Integration build {tag}\n\nupstream {out('merge-base', cand, UPSTREAM)}\ntopic tips:\n{body}",
    )
    print(f"{tag} = {cand}")


def land(args):
    worktree()
    if os.path.exists(STATE):
        sys.exit("a rebuild or collapse is in progress; finish it first")
    topic, branch = f"topic/{args.topic}", args.branch
    original = rev(branch)
    if git("reflog", "-1", "--format=%gs", branch).stdout.startswith(f"rebase (finish): refs/heads/{branch} onto"):
        original = rev(f"{branch}@{{1}}")  # rerun after a stopped land: the rebase already moved the branch
    archive = f"archive/worker/{branch}/{TODAY}-{original[:10]}"
    git("tag", "-f", archive, original)
    snaps = [rev(topic)] + git(
        "for-each-ref", "--sort=-refname", "--format=%(objectname)", f"refs/fork-topics/*/{args.topic}"
    ).stdout.split()
    base = next((s for s in snaps if is_ancestor(s, branch)), None)
    if base is None:
        sys.exit(f"{branch} is not based on any snapshot of {topic}; rebase it onto {topic} by hand")
    if base != rev(topic):
        worktrees = out("worktree", "list", "--porcelain").split("\n\n")
        busy = [
            w.split("\n")[0].removeprefix("worktree ")
            for w in worktrees
            if f"branch refs/heads/{branch}" in w.split("\n")
        ]
        if busy:
            sys.exit(f"{branch} is checked out in {busy[0]}; run `git -C {busy[0]} switch --detach` there and rerun")
        rebase(f"rebase of {branch} onto {topic}", "--rebase-merges", "--onto", rev(topic), base, branch, log=[])
    git("checkout", "--quiet", topic, cwd=WT)
    r = git(
        "merge",
        "--no-ff",
        "--no-verify",
        "-m",
        f"Merge {branch} into {topic}",
        "-m",
        f"Original tip: {original[:12]} ({archive})",
        branch,
        cwd=WT,
        check=False,
    )
    if r.returncode != 0:
        git("merge", "--abort", cwd=WT, check=False)
        sys.exit(f"merging {branch} into {topic} conflicts after the rebase; this should not happen:\n{r.stdout}")
    git("checkout", "--quiet", "--detach", cwd=WT)
    print(f"landed {branch} in {topic} (original tip {archive}); run `rebuild --no-fetch`")


def collapse(args):
    worktree()
    if os.path.exists(STATE):
        sys.exit("a rebuild or collapse is in progress; finish it first")
    topics = dict(read_topics())
    parent = topics[args.topic]
    topic = f"topic/{args.topic}"
    # collapse on top of the stack as it is: topics must already sit on their parents
    first_root = next(t for t, p in read_topics() if p == "upstream")
    base_up = out("merge-base", f"topic/{first_root}", UPSTREAM)
    base = base_up if parent == "upstream" else rev(f"topic/{parent}")
    if not is_ancestor(base, topic):
        sys.exit(f"{topic} is not on its parent's tip; run rebuild first")
    st = start_run(base_up, "collapse")
    tip = rev(topic)
    originals = out("rev-list", "--reverse", "--no-merges", f"{base}..{tip}").split()
    inherited = out("log", "--reverse", "--format=%(trailers:key=Squashed-from,valueonly)", f"{base}..{tip}").split()
    archive = f"archive/topic/{args.topic}/{TODAY}-{tip[:10]}"
    git("tag", "-f", archive, tip)
    msg = (
        f"{args.title or out('log', '-1', '--format=%s', originals[0])}\n\n"
        f"Topic {args.topic}, collapsed from {len(originals)} commits (archive tag {archive}).\n\n"
        + "".join(f"Squashed-from: {c[:12]}\n" for c in dict.fromkeys(inherited + originals))
    )
    new = out("commit-tree", out("rev-parse", f"{tip}^{{tree}}"), "-p", base, "-m", msg)
    git("update-ref", f"refs/heads/{topic}", new, tip)
    st["done"] = [t for t, _ in st["topics"] if t == args.topic or t not in descendants(args.topic, st["topics"])]
    save(st)
    restack(st)
    os.remove(STATE)
    print(f"{topic}: {tip[:10]} -> {new[:10]} (same tree), originals under {archive}; topics above restacked")


def descendants(name, topics):
    found = {name}
    for t, p in topics:
        if p in found:
            found.add(t)
    return found - {name}


def status(args):
    for t, p in read_topics():
        base = UPSTREAM if p == "upstream" else f"topic/{p}"
        ahead = out("rev-list", "--count", f"{base}..topic/{t}")
        behind = out("rev-list", "--count", f"topic/{t}..{base}")
        print(f"topic/{t:22} on {base:24} {ahead:>4} commits, {behind:>4} behind")
    print(
        f"candidate {rev(CANDIDATE)}; last tag {tags()[0] if tags() else None}; run in progress: {os.path.exists(STATE)}"
    )


ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
sub = ap.add_subparsers(required=True)
a = sub.add_parser("rebuild")
a.add_argument("--no-fetch", action="store_true")
a.add_argument(
    "--same-tree-as",
    metavar="REF",
    help="fail unless the new integration has REF's tree, FORK-TOPICS aside (for carves and re-parents)",
)
a.set_defaults(f=rebuild)
sub.add_parser("promote").set_defaults(f=promote)
a = sub.add_parser("land")
a.add_argument("branch")
a.add_argument("topic")
a.set_defaults(f=land)
a = sub.add_parser("collapse")
a.add_argument("topic")
a.add_argument("--title")
a.set_defaults(f=collapse)
sub.add_parser("status").set_defaults(f=status)
args = ap.parse_args()
args.f(args)
