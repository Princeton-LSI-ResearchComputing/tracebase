<!-- markdownlint-disable-next-line first-line-heading -->
## Summary Change Description

<!-- Briefly describe the changes in this pull request (put details in the
developer section of the linked issue(s)). -->

## Affected Issue/Pull Request Numbers

- Resolves #<!--issue_number-->
<!--
- Partially addresses #issue_number
- Depends on parent #pull_request_number
- Depends on child #pull_request_number
-->

## Code Review Notes

<!-- Describe any areas of concern that code reviewers should pay particular
attention to.  E.g. There are significant logic changes in function X. -->

## Checklist

A pull request can be merged once the following requirements are met.  The PR
author asserts that the following requirements are met by requesting a review:

- Review requirements
  - Minimum approvals: 2 <!-- Author: Edit as desired -->
  - No changes requested
  - All blocking issues acknowledged as resolved by reviewers
  - Specific approvals required
    <!-- Author: Approvals from the contributors you select are required
    regardless of minimum approvals.  Check or delete as desired. -->
    - [ ] @cbartman1
    - [ ] @hepcat72
    - [ ] @jcmatese
    - [ ] @fkang-pu
    - [ ] @lparsons
    - [ ] @mneinast
    - [ ] @narzouni
- Associated issue/pull request requirements:
  - [x] All "Resolves" issue requirements satisfied
  - All dependent pull requests are merged
- Basic requirements
  - [x] [All linters pass](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/main/CONTRIBUTING.md#linting).
  - [x] [All tests pass](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/main/CONTRIBUTING.md#quality-control)
  - [x] All conflicts resolved
- Overhead requirements
  - [x] [New method tests implemented *(or no new methods)*](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/main/CONTRIBUTING.md#test-implementation)
  - [x] Added qualifying changes to *Unreleased* section of [`changelog.md`](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/main/changelog.md).
  - [x] [Migrations created & committed *(or no model changes)*](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/main/CONTRIBUTING.md#migration-process)
