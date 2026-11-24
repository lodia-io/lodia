---
paths:
  - "todo.md"
---

# todo.md

- An item opens with the condition that ends its wait, `When …,`, and says what to do when it holds ([decision](../../docs/decisions/todo-holds-what-waits.md)).
- An ask of another project waits for the maintainer's word.
  An item that waits on it stays in its topic and names the ask, its project and whether it is one of the asks held back; an ask nothing in Lodia waits on goes under "Asks of other projects" and names the same.
- Work that can start now is no item: report it to the task's author, who places it in the plan.
- A pass that fixes an item removes it with the `@todo` markers that name it; a workaround keeps its item until its condition holds.
- Group by topic.
  A heading that a doc links stays, or the link moves with it.
