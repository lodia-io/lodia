# Lodia governance

This document says how Lodia is run.
It follows the maintainer-council template of the
[CNCF project template](https://github.com/cncf/project-template/blob/e66ba44f724d00f66c84932593c5a229fee3564b/GOVERNANCE-maintainer.md).

## Values

- **Openness.** Discussion and decisions happen in the open, in issues and
  pull requests on [github.com/lodia-io/lodia](https://github.com/lodia-io/lodia),
  and stay there for future reference.
- **Fairness.** Anyone can give feedback and contribute; contributions are
  judged on their merits.
- **Community over company.** Each contributor takes part as an individual.
- **Vendor neutrality.** Decisions rest on the project's merit, not on anyone's
  employer. Today every maintainer works for one company, Segla; the project
  aims for maintainers from more than one organization.
- **Participation.** Responsibility is earned by taking part, and the path to
  it is written down here.

## Maintainers

Maintainers have write access to the repository and can merge pull requests,
their own included, through the rules on `main`: a pull request, squashed,
with the CI checks and the DCO check green. The current maintainers are listed
in [MAINTAINERS.md](MAINTAINERS.md). Together they are the Maintainer Council,
the project's governing body.

### Becoming a maintainer

A contributor becomes a maintainer after showing, over three months or more:

- reviews of at least five non-trivial pull requests;
- at least five non-trivial pull requests of their own, merged;
- the quality of code or documentation that `CONTRIBUTING.md` asks for;
- an understanding of how the project works: the decisions in
  `docs/decisions/`, the roadmap's gates, the tests.

An existing maintainer proposes the contributor in a public issue. A simple
majority of the maintainers approves. Nominations are judged without regard
to employer and with an eye on the maintainers' organizational diversity.

### Leaving

A maintainer may step down at any time. A maintainer inactive for twelve
months, or one who breaks the Code of Conduct, may be removed by a two-thirds
vote of the other maintainers. A former maintainer may be listed as emeritus
in `MAINTAINERS.md`, without a vote or merge rights, and may return by a
simple majority vote.

## Meetings

There is no regular meeting yet; the project works in issues and pull
requests. Maintainers meet privately only to handle a security report or a
Code of Conduct report.

## Code of Conduct

Lodia follows the [CNCF Code of Conduct](https://github.com/cncf/foundation/blob/main/code-of-conduct.md).
Reports go to the address in [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md), which
reaches the maintainers, and are handled in private. A maintainer named in a
report takes no part in handling it.

## Security

The maintainers are the security response team. Reports follow
[SECURITY.md](SECURITY.md): GitHub's private vulnerability reporting.

## Voting

Most decisions are made by
[lazy consensus](https://community.apache.org/committers/lazyConsensus.html) in
issues and pull requests. Any maintainer may call for a vote. A vote passes
with a simple majority of all maintainers, except where this document asks for
two thirds. Votes on security or conduct matters are taken in private.

## Changing this document

A change to this document needs a two-thirds vote of the maintainers, in its
pull request.
