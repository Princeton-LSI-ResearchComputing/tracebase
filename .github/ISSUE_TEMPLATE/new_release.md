---
name: New Release
about: Create a new release of TraceBase
title: ''
labels: ''
assignees: ''
---

<!-- markdownlint-disable-next-line first-line-heading -->
* [ ] `git pull` - make sure local repository is up to date
* [ ] Polish `changelog.md` - get changelog ready for release
* [ ] `git push` - commit and push updated changelog
* [ ] Ensure all tests pass in GitHub actions
* [ ] `git tag -a [TAGNAME]`
* [ ] `git push origin [TAGNAME]`
* [ ] Follow steps to [create a release on GitHub](https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository#creating-a-release)
* [ ] Add *Unreleased* to `changelog.md`
* [ ] If this is a release branch (not `main`), merge changes into `main`
* [ ] `git push`
* [ ] [Deploy to `tracebase.princeton.edu`](https://nplcadmindocs.princeton.edu/index.php/TraceBase#Deploy_Update)

See [NPLC Admin Docs:TraceBase:New
Release](https://nplcadmindocs.princeton.edu/index.php/TraceBase#New_Release)
for more details
