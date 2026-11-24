# `todo.md` holds only what waits on a condition, and each item opens with it

Decided 2026-11-24.
Built.

## Decision

- An item enters `todo.md` only with the condition that ends its wait as its first words: a release, an upstream fix, a need that has not come, a measurement that has no occasion yet.
  The item then says what to do when the condition holds, and names the files, tests and `@todo` markers it changes.
- An ask of another project waits for the maintainer's word, and says so once under its heading: each item there names its project and whether it is one of the asks held back to go together.
- Work that can start now is no item.
  A finding outside the task at hand goes to the task's author, who places it in the plan.
- A pass that fixes an item removes it in the same change, with the `@todo` markers and the links in the docs that name it.
- Items are grouped by topic, not by where they came from.

## Why

- At `6452c99` the file held 83 items: 56 could start, 5 were obsolete, and 22 waited on a condition, 6 of them asks of other projects.
  A file that mixes the two cannot say what is owed and what is only waiting.
- Among the 56 was a defect no heading marked as one: an edit that removes `etcd.autoCompaction` from a class makes etcd-druid's schema refuse the `Etcd` of every tenant on it, and the item had stood since 2026-11-11.
  #68 and #99 had fixed items and left them in the file.

## Rejected

- An inbox of every finding outside a task, triaged when the task ends — the triage did not keep up, and 61 of the 83 items needed no condition.
- A mark on each item, "waits" or "can start", in one file — what can start would still sit there, and a defect among it would wait like the rest.

## Consequences

- A finding that can start is tracked in the repository only once its task is: until then it is in the report to the task's author.
- A workaround's `@todo` marker and its item are a pair, and the item holds the condition that undoes it.
- A doc may link a heading of the file, as `upgrades-and-maintenance-windows.md` does; renaming the heading moves the link in the same change.

## Code

- `todo.md` — the file.
- `.claude/rules/todo.md` — the rule for editing it, applied to `todo.md` alone.
- `CLAUDE.md`, "Working from a task" — where a finding outside the task goes.
