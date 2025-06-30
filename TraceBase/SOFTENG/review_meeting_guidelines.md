# REVIEW MEETING GUIDELINES

- Version 2.0
- Prepared by: Robert W. Leach

## PURPOSE

**Understanding Review Issues**: A primary purpose of a review meeting is to ensure that the author understands the review issues created by reviewers.

**Understanding the Changes**: A second primary purpose of a review meeting is to ensure that the reviewer understands the changes enough to be able to identify defects.

**Generally**: The general purpose of the review meeting is not to determine *how* to resolve an issue, but rather to define *what* an issue is.

A review meeting is necessary when 1 or more reviewers do not fully understand the changes in a code or design review, if the author does not fully understand a 1 or more review issues raised by a reviewer, or if the CM Leader (see the CM LEADER section) deems it necessary to clear up any confusion.

A review meeting is useful when the review process has stalled.

## GOALS

**Completing a Change Request**: The primary reason for having a review meeting is to facilitate the review process toward the goal of merging changes that meet the requirements of an approved change request without defects.

**Change Control**: A secondary goal of the review process is change control.  Uncontrolled change leads to defects and unpredictability.

**Teamwork**: A review process in general, is intended to establish roles, set expectations, and thereby mitigate or prevent contention by everyone understanding what their role is and what is expected of them.

**Generally**: The overarching goal is software quality assurance.

## SCOPE

A review meeting entails the following topics.

- The understanding of defects reported by the reviewer.
- The understanding of changes made by the author.

The following are outside the scope of a review meeting.

- Underlying requirements and design.^1
- Over-arching project-level topics.^2
- Debates over whether a perceived defect is a true defect.
- Discussions over how to solve a defect.

## ROLES & RESPONSIBILITIES

### CM LEADER (CONFIGURATION MANAGEMENT LEADER)

The CM Leader is the guardian of the repository and controls and oversees the processes governing all changes to it.

#### CM LEADER RESPONSIBILITIES

- Ensure the author has sufficient time to review issues before the meeting.
- Manages the scope and pace of review meetings.

#### CM LEADER LIMITATIONS

None

#### CM LEADER ACTIONS

- Calls, schedules, and runs review meetings.
- Manages the scope and pace of a review meeting using the following actions:
  - Declare a topic/discussion *Beyond Meeting Scope*^3
  - Declare a topic/discussion *Beyond Document Scope*^4
  - Change or table discussion topics in the interests of *Time*^5
- Ends a review meeting once all major and higher severity issues have been communicated and understood.
- Can assign reviewers to a pull request.

### REVIEWER

The reviewer is the bulwark of quality assurance.  They own the review issues they create and decide whether the author's work meets the requirements without defects.

#### REVIEWER RESPONSIBILITIES

- Communicate defects to the author.
- Ensure the author understands every reported defect enough to address it.
- Understand the reviewed changes enough to find defects.

#### REVIEWER LIMITATIONS

A review meeting is limited to understanding changes and review issues.  It is not the reviewer's responsibility to:

- Make suggestions about how to address a review issue.
- Reject a merge request or change request.
- Advocate for a change to an approved change request's design or requirements.

#### REVIEWER ACTIONS

- Request time extensions from the CM Leader if unable to complete a review before a review meeting.
- Describe each defect they found during their independent review of the changes.
- Answer questions the author asks to help them understand the review issue.
- Ask for clarifications of changes, when unclear.

### AUTHOR

The author is the owner of the implementation/changes and is the only one who is allowed to modify those changes relating to the taken issue.  The author is the only one who can decide how to address a review issue.

#### AUTHOR RESPONSIBILITIES

The author's responsibility is as it pertains to addressing defects reported by reviewers.  The author's responsibilities during a review meeting are to:

- Understand all defects reported by reviewers enough to address them.
- Ensure reviewers understand the changes enough to find defects.

#### AUTHOR LIMITATIONS

A review meeting is limited to understanding changes and review issues.  It is not the reviewer's responsibility to:

- Explain how they intend to address a review issue.
- Reject a review issue.
- Defend a change.

#### AUTHOR ACTIONS

- Answer reviewer questions about the content of the changes.
- Ask for clarifications about reported defects, if unclear.

## CONTEXT

The following describes the overall context surrounding a code review meeting to contextualize the point when a review meeting occurs.

### SOFTWARE ENGINEERING PHASES

This document is a part of the SCMP (Software Configuration Management Plan).  It is created by the CM Leader at the start of a project.

1. Planning
   - Software Project Management Plan (SPMP)
   - Software Configuration Management Plan (SCMP)
   - Software Quality Assurance Plan (SQAP)
   - Software Testing Plan (STP)
2. Requirements
3. Design
4. Implementation
5. Testing
6. Quality Assurance  **WE ARE HERE**
7. Maintenance

### CHANGE CONTROL PROCESS (QUALITY ASSURANCE PHASE)

Each step described here is dependent on the steps before it.  The process for each step should be described in the SCMP, e.g. the process by which a change request is approved.  This document does not describe the processes outside the scope of a review meeting.

The overall process starts with a "Change Request" / "Issue".  Each change request is a potential mini-project in and of itself, going through every phase described in SOFTWARE ENGINEERING PHASES above from Requirements through Testing.

1. Change request submitted
2. Change request review process
3. Change request approved^3 (if rejected, stop here)
4. Change request taken by an author (a.k.a. "developer")
5. Author works up a design^4
6. Author requests design review
7. Design review process
8. Design approved (if rejected, return to step 5)
9. Author works on the changes ("Implementation")
10. Author requests independent review
11. Independent review process
12. Request a review meeting (if necessary)
13. Schedule a review meeting (if necessary)
14. Review meeting (if necessary)  **WE ARE HERE**
15. Changes approved (if rejected, return to step 9)
16. Changes merged into the repository

## REVIEW MEETING PROCESS

An author or reviewer can request the CM to schedule a review meeting if they are unclear or confused about a review issue or the changes being reviewed and believe that the pull request interface is inadequate to the task of communicating the details.  The CM leader may schedule a review meeting at their discretion.  When that meeting takes place, it will proceed with the following process.

1. The CM leader starts the review meeting and calls on each reviewer, one by one, to go through their unresolved issues.
2. For each reviewer issue:
   1. The reviewer:
      - Describes a review issue issue or
      - Asks a question to clear up confusion they have about a changes
      - Responds to any question the author posed about confusion they have about the issue
   2. The author:
      - May indicate that they understand the issue
      - May ask a question to clear up confusion about the review issue
      - Responds to any question the reviewer posed about confusion they have about the changes
   3. The CM leader may interject at any point to:
      - Declare a topic/discussion *Beyond Meeting Scope*^3
      - Declare a topic/discussion *Beyond Document Scope*^4
      - Change or table discussion topics in the interests of *Time*^5
      - End the meeting
3. Once all review issues have been covered, the CM ends the meeting

After the meeting, the review/re-review process continues, but the CM may take action to resolve any review issue.

## FOOTNOTES

1. *Unless the changes being reviewed are requirements or design, all planning leading up to the implementation of a change request are beyond the scope of a review meeting.  If any such defect arises during a review meeting, the parties broaching the issue will be directed by the CM leader to create an issue to be addressed as a change request.*
2. *Defects involving project-level topics such as project planning, principles, conventions, general requirements, etc. are beyond the scope of a review meeting.*
3. *If interactions stray beyond the conveyance of defects found, or a clarification of changes, the CM Leader may declare the topic "beyond meeting scope" and either direct the reviewer to advance to the next issue or re-orient the discussion back toward the identification of the reviewer's interpretation of defects and ensure that the defect pointed out is understood by the author.*
4. *If the CM Leader determines that the discussion has gone beyond the scope of document changes, the CM Leader may declare the topic "beyond document scope" and either direct the reviewer to advance to the next issue or re-orient the discussion back toward the identification of the reviewer's interpretation of defects and ensure that the defect pointed out is understood by then author.*
5. *If the CM Leader determines that a discussion, while productive, may be more effectively/efficiently addressed outside of the meeting, they may suggest that the topic be discussed after the meeting and direct the reviewer to advance to the next issue.*
6. *The change request approval process should involve prioritization and estimation of cost that govern when an issue is available to be taken up by an author.*
7. *For the purposes of succinctness, "design" encompasses requirements, limitations, assumptions, affected components, etc.*

## REFERENCES

1. `document_review_form.txt` version 1.3 *from the TreeView3 project*
2. `review_meeting_guidelines.txt` version 1.3 *from the TreeView3 project*
3. `SCMP_TreeView3.doc` 8/14/2015 *from the TreeView3 project*
