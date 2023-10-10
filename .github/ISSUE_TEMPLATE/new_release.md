---
name: New Release
about: Create a new release of TraceBase
title: ''
labels: ''
assignees: ''
---

<!-- markdownlint-disable-next-line first-line-heading -->
* [ ] `git pull`
* [ ] Polish `changelog.md`
* [ ] `git push`
* [ ] Ensure all tests pass
* [ ] `git tag -a [TAGNAME]`
* [ ] `git push origin [TAGNAME]`
* [ ] [Create a GitHub release](https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository#creating-a-release)
* [ ] Add *Unreleased* to `changelog.md`
* [ ] `git push`
  * [ ] If release branch - merge into main and push
* [ ] [Deploy to `tracebase.princeton.edu`](https://nplcadmindocs.princeton.edu/index.php/TraceBase#Code_Deployment)

See [NPLC Admin Docs:TraceBase:New
Release](https://nplcadmindocs.princeton.edu/index.php/TraceBase#New_Release)
for more details
