// pr_review_nudge.js - the body of .github/workflows/pr-review-nudge.yml.
//
// Nudges the assignees of every open PR who have not submitted a verdict on the
// current head commit, by re-issuing GitHub's native review request - subject to
// a per-reviewer floor (NUDGE_AFTER_DAYS) that holds back anyone already
// contacted about that PR recently, and to an opt-out for anyone who removed
// their own review request. Lives here rather than inline in the workflow so it
// can be unit tested: see the sibling test_pr_review_nudge.js, which drives it
// with a mocked Octokit, and the pr-review-nudge-test job in reviewdog.yml.
//
// Loaded by actions/github-script as:
//
//   const nudge = require(`${process.env.GITHUB_WORKSPACE}/.github/bin/pr_review_nudge.js`);
//   await nudge({ github, context, core });
//
// Two more inputs come from the step's environment rather than that call, and
// they are the ones that decide what the run does: DRY_RUN ('true' logs the
// planned nudges and mutates nothing; anything else mutates) and
// NUDGE_AFTER_DAYS (the per-reviewer floor in whole days; blank or malformed
// means 7, '0' turns the floor and the opt-out off).
//
// The decisions that are pure functions of their inputs - the floor parse, the
// assignee filter, the staleness predicate, the contact walk, the opt-out rule
// and the due/held partition - hang off that callable as properties so the
// tests can call them without standing up a fake Octokit.
//
// Run the tests: node .github/bin/test_pr_review_nudge.js

'use strict';

const MS_PER_DAY = 24 * 60 * 60 * 1000;

// The floor every trigger actually applies. A schedule run sends no value at
// all and an untouched dispatch sends the input's own `default: '7'`, which is
// there for the dispatch form's sake - this constant is the only place the
// number is implemented.
const DEFAULT_NUDGE_AFTER_DAYS = 7;

// A floor commensurate with the schedule has to tolerate cron drift. The
// default 7 days is exactly the weekday period of '30 3 * * 1,3,5', so the
// instant a reviewer becomes due lands inside the very run meant to nudge them,
// and which side of it they land on is decided by GitHub's queue delay - which
// GitHub documents as unbounded under load. Without slack, a run that starts a
// minute earlier than last week's defers that nudge a whole period, oscillating
// the effective cadence between 7 and 9 days for no reason the log could
// explain. Half a day is far larger than any plausible delay and far smaller
// than the Mon-to-Wed gap, so it cannot make a reviewer due twice in one week.
const FLOOR_SLACK_MS = MS_PER_DAY / 2;

// The largest floor worth accepting. Nothing depends on the exact bound; it
// exists so a well-formed but absurd digit string ('99999999999') cannot make
// every reviewer permanently undue and turn the job into a green no-op.
// Exponent forms like '1e308' never reach it - ^\d+$ below rejects those first.
const MAX_NUDGE_AFTER_DAYS = 365;

// NUDGE_AFTER_DAYS arrives from a free-text dispatch input, so malformed values
// are expected rather than exotic - and Number() maps ' ', '0x0', '0e5' and
// '+0' all to 0, which is the one value carrying a side effect: it disables the
// floor. A guard whose failure mode is "notify everyone about everything" must
// refuse to guess, so parse strictly and say what was concluded. ^\d+$ rejects
// whitespace, hex, exponent and signed forms while honouring a deliberate '0',
// and rejects the fractional days nothing documents or tests.
function parseNudgeAfterDays(raw, core) {
  const text = String(raw ?? '').trim();

  // Unset and blank both mean "no opinion", which is the default rather than
  // an error - the workflow's `|| 7` only catches the empty string, so a
  // dispatch field holding a stray space reaches here verbatim.
  if (text === '') { return DEFAULT_NUDGE_AFTER_DAYS; }

  if (!/^\d+$/.test(text) || Number(text) > MAX_NUDGE_AFTER_DAYS) {
    core.warning(`NUDGE_AFTER_DAYS=${JSON.stringify(text)} is not a whole number of`
      + ` days between 0 and ${MAX_NUDGE_AFTER_DAYS}`
      + ` - falling back to ${DEFAULT_NUDGE_AFTER_DAYS}`);
    return DEFAULT_NUDGE_AFTER_DAYS;
  }

  return Number(text);
}

async function listOpenPrs(github, owner, repo) {
  // Oldest first: pulls.list paginates by offset into a result set recomputed
  // per request, so with the default newest-first order every PR opened
  // mid-walk shifts later pages down one. Ascending creation order keeps new
  // PRs at the tail, out of the walk's way. It is not a guarantee, only a
  // narrowing: a PR reopened mid-walk re-enters at its creation position and
  // can still shift a page, which is what the de-dup below is for. A PR merged
  // or closed mid-walk shifts later pages up instead, silently dropping one
  // open PR from this run - accepted, since the next scheduled run picks it up.
  const listed = await github.paginate(github.rest.pulls.list, {
    owner,
    repo,
    state: 'open',
    sort: 'created',
    direction: 'asc',
    per_page: 100,
  });

  return [...new Map(listed.map(pr => [pr.number, pr])).values()];
}

// GitHub rejects a review request naming the PR's own author, so an author
// assigned to their own PR is never a nudge target. It refuses nothing about
// any other assignee, which is why the review_requested walk below records a
// request whoever made it; the actor is read only for review_request_removed,
// to tell a reviewer's own "not me" from a maintainer's reassignment.
function assignedReviewers(pr) {
  return (pr.assignees ?? [])
    .map(a => a.login)
    .filter(login => login !== pr.user?.login);
}

// Collect - not nudge; the floor and the opt-out below filter this further -
// every assigned reviewer who has not submitted a verdict on the current head.
// Reviews are matched by commit id rather than by timestamp:
// commit.committer.date is the contributor's local clock at git commit time,
// not push time, so comparing it to a GitHub-assigned submitted_at silently
// under-nudges. COMMENTED carries no verdict, so leaving inline notes is not
// reviewing.
function staleReviewers(reviewers, reviews, headSha) {
  return reviewers.filter(login =>
    !reviews.some(r => r.user?.login === login
                    && r.commit_id === headSha
                    && (r.state === 'APPROVED' || r.state === 'CHANGES_REQUESTED')));
}

// How recently was each reviewer actually contacted about this PR, has anyone
// asked them again, and has any of them said "not me"?
//
// review_requested is GitHub-assigned and per login, and the nudge below
// creates one - so the workflow reads its own prior nudges back out of the
// event feed and needs no comment, label or external store to rate-limit
// itself. pr.updated_at would be the wrong clock: it moves on any activity at
// all, so a PR with lively discussion and no review would keep resetting its
// own floor and never be nudged - exactly the PRs this exists for.
//
// Two clocks, because two questions: `contacted` answers "did we bother them
// recently" (the floor) and is fed by review requests and by the login's own
// reviews on the current head; `requested` answers "did someone ask again"
// (the opt-out override) and is fed only by review_requested events. Folding
// them together lets a reviewer's own COMMENTED review - no verdict, so still
// stale - silently revoke their own standing opt-out.
function contactByLogin(reviews, events, headSha) {
  const contacted = new Map();
  const requested = new Map();
  const optedOut = new Map();

  // Keeps the newest entry per login rather than the last one seen, since the
  // order events arrive in is GitHub's to choose. Number.isFinite is the
  // explicit half of a rejection the comparison would make anyway - a malformed
  // date parses to NaN, and NaN is greater than nothing - and is kept because a
  // clock this decision rests on should say out loud which values it refuses.
  const record = (into, login, when) => {
    if (!login || !when) { return; }

    const at = new Date(when).getTime();

    if (Number.isFinite(at) && at > (into.get(login) ?? 0)) { into.set(login, at); }
  };

  for (const e of events) {
    // requested_reviewer is absent when a *team* was requested.
    if (e.event === 'review_requested') {
      record(contacted, e.requested_reviewer?.login, e.created_at);
      record(requested, e.requested_reviewer?.login, e.created_at);
      continue;
    }

    // Removing your own review request is the one gesture GitHub gives a
    // reviewer for saying "not me". Without reading it back, the next run
    // whose floor has expired silently reverses that gesture, and the only
    // other escapes are to un-assign yourself - which destroys the assignment
    // signal for everyone else - or to submit a verdict you do not have. That
    // is a loop the reviewer cannot exit.
    //
    // actor === requested_reviewer narrows this to a deliberate self-removal.
    // The same event is emitted when someone *else* strips a reviewer during
    // reassignment churn - a maintainer's call, not the reviewer's - and by
    // this workflow itself on every nudge of a pending reviewer, which without
    // the comparison would make the script's own DELETE an opt-out from its
    // own POST.
    if (e.event === 'review_request_removed'
        && e.actor?.login
        && e.actor.login === e.requested_reviewer?.login) {
      record(optedOut, e.requested_reviewer.login, e.created_at);
    }
  }

  // A review of their own counts as contact too, whatever its state: someone
  // who left inline notes yesterday is stale by the predicate above -
  // COMMENTED carries no verdict - but should not be pinged today for it.
  // Free, since the reviews are already in hand.
  //
  // Only on the current head, though. A review of a superseded commit is not
  // contact about the PR as it stands, and the push that superseded it is
  // exactly what makes a re-review needed: someone who requested changes
  // yesterday and had them addressed this morning is the person most worth
  // nudging, not least.
  //
  // Contact only - deliberately not `requested`. A reviewer's own review is a
  // reason not to ping them this week; it is not someone asking them again,
  // and must not revoke their standing opt-out.
  for (const r of reviews) {
    if (r.commit_id === headSha) { record(contacted, r.user?.login, r.submitted_at); }
  }

  return { contacted, requested, optedOut };
}

// A self-removal only stands until someone asks again: a maintainer who
// re-requests the reviewer after their removal has overridden it deliberately,
// and the nudge should follow the newer signal. Compared against `requested`
// rather than `contacted`: only a review_requested event is someone asking
// again, whoever asked. That walk reads no actor, deliberately, and narrowing
// it by one is wrong in both directions: GitHub does not guarantee an actor on
// review_requested, so a presence test drops the actor-less events, and it
// refuses only a request naming the PR's author, so an inequality test drops a
// collaborator asking for their own review back. Either way the request never
// reaches `requested` and the reviewer stays opted out with nothing able to
// clear it - not even, in the second case, herself.
function hasOptedOut(login, requested, optedOut) {
  return (optedOut.get(login) ?? 0) > (requested.get(login) ?? 0);
}

// Nudges one reviewer. Returns 'nudged', 'unrequestable' or 'failed'; every
// failure is reported here, so the caller only has to tally the outcome.
// Splits the reviewers we want to nudge into those due and those the floor
// holds back. One pass returning both halves, rather than a filter plus its
// complement, because the counter and the log have to agree on the held-back
// set: derived as a size, it was incremented for reviewers no log line named.
//
// Never contacted -> contacted at 0 -> always due, which is the case that
// matters most: being an assignee is not being asked to review (`assigned` and
// `review_requested` are separate events), so for these the nudge is the first
// review request they have ever had. It is also why every held login has a
// `contacted` entry, which is what lets the caller name them: floorMs is at
// least the 12h of slack, and `now - 0` is not.
function dueAndHeld(wanted, contacted, now, floorMs) {
  const due = [];
  const held = [];

  for (const login of wanted) {
    ((now - (contacted.get(login) ?? 0) >= floorMs) ? due : held).push(login);
  }

  return { due, held };
}

async function nudgeReviewer(github, core, ref, login, wasPending) {
  // A fresh object per call rather than one shared between three of them: the
  // request array is the only thing standing between this and a wider strip.
  const justThisLogin = () => ({ ...ref, reviewers: [login] });
  const request = () => github.rest.pulls.requestReviewers(justThisLogin());
  const strip = () => github.rest.pulls.removeRequestedReviewers(justThisLogin());

  // POST first, even for a reviewer who already holds a live request - for
  // whom it notifies nobody - because it proves the login is requestable
  // before anything is removed. Stripping first opens a window in which the
  // DELETE has succeeded and the POST has not, destroying a live review
  // request; compensating for that means re-issuing the identical call that
  // just failed, microseconds later, which only recovers errors that self-heal
  // in microseconds. Those are precisely the errors that never reach here: the
  // action's retries: 3 absorbs 5xx, and its retry-exempt list leaves this
  // handler a 422 or a 403.
  let phase = 'request';

  try {
    await request();

    // Re-requesting someone who is already a pending reviewer notifies nobody,
    // so their request has to be dropped and re-made for the nudge to land.
    // Anyone who already submitted a review is no longer a requested reviewer,
    // and the POST above was the whole nudge.
    if (wasPending) {
      phase = 'strip';
      await strip();

      phase = 're-request';
      await request();
    }

    core.info(`PR #${ref.pull_number}: nudged ${login}`);
    return 'nudged';
  } catch (err) {
    // The phase decides both what state is left behind and what anyone reading
    // the log should do about it, so it has to be named. A failed request lost
    // nothing. A failed strip left the reviewer's existing request intact and
    // cost only the notification. A failed re-request destroyed a live review
    // request - the only one of the three that is an error rather than a
    // warning. Even that one recovers without help: the strip recorded no
    // contact, so the login is still stale and still due next run, and it is
    // no longer pending, so the next run nudges it with a single POST.
    const report = phase === 're-request' ? core.error : core.warning;

    report(`PR #${ref.pull_number}: ${phase} failed for ${login} - `
      + `HTTP ${err.status ?? '?'}: ${err.message}`);

    // A 422 on the proving request is the one failure that says something
    // about the login rather than about the moment - GitHub rejects a review
    // request for someone who cannot be a reviewer, and no retry fixes it. It
    // gets its own counter so a standing misconfiguration reads as a row
    // naming the cause instead of only as the same red run three mornings a
    // week. Anywhere else, a 422 is a stale snapshot and is merely a failure.
    return phase === 'request' && err.status === 422 ? 'unrequestable' : 'failed';
  }
}

async function writeSummary(core, dryRun, nudgeAfterDays, prCount, counts) {
  // Without a denominator, "no output" reads the same whether every PR was
  // correctly skipped or the loop died on the third one.
  //
  // PR counts and reviewer counts are labelled as such because they are
  // different units: one PR can contribute several reviewers, and a PR whose
  // nudge succeeded for one login and failed for another contributes to both
  // reviewer rows. The floor is neither - it is the setting the reviewer rows
  // have to be read against, and it gets its own row rather than being
  // interpolated into one, so the table's key set stays the same across runs
  // however the floor is tuned.
  await core.summary
    .addHeading(dryRun ? 'PR review nudge (dry run)' : 'PR review nudge')
    .addTable([
      [{ data: 'Metric', header: true }, { data: 'Value', header: true }],
      ['PRs scanned', String(prCount)],
      ['PRs skipped', String(counts.skipped)],
      ['PRs abandoned', String(counts.abandoned)],
      ['Nudge floor (days)', String(nudgeAfterDays)],
      [dryRun ? 'Reviewers that would be nudged' : 'Reviewers nudged', String(counts.nudged)],
      ['Reviewers held back by the floor', String(counts.withinFloor)],
      ['Reviewers who removed their own request', String(counts.optedOut)],
      ['Reviewers failed', String(counts.failed)],
      ['Reviewers not requestable', String(counts.unrequestable)],
      ['Reviewers on abandoned PRs', String(counts.abandonedReviewers)],
    ])
    .write();
}

const nudge = async ({ github, context, core }) => {
  const dryRun = process.env.DRY_RUN === 'true';
  const { owner, repo } = context.repo;

  // Don't re-contact a reviewer who was contacted about this PR this recently.
  // Without a floor the predicate has no time term at all: the nudge mutates
  // neither of its own operands, so a reviewer stays eligible until a human
  // acts and every run emits an identical notification. The cost is borne per
  // person rather than per PR - one reviewer assigned to six open PRs collects
  // six notifications every run, indefinitely - and the channel is shared with
  // every genuine first-time review request in the org, so a signal that fires
  // forever is one reviewers learn to filter.
  //
  // What the floor bounds is the *frequency* per (PR, reviewer) pair, not the
  // size of a single run's burst: the map below is built from one PR's events
  // and discarded with the iteration, so a reviewer assigned to six stale PRs
  // still collects six notifications at once - weekly rather than three times
  // a week. Capping the burst as well needs a run-level per-login budget,
  // which is deliberately not here.
  //
  // 0 disables the floor - and with it the self-removal opt-out, since both
  // are read from the same event walk this skips. That is not an exact revert:
  // a nudge such a run issues to an opted-out reviewer writes a real
  // review_requested event, which every later run reads back as "someone
  // asked again" - so a floor-0 run permanently clears the standing opt-outs
  // it nudges through, and only the reviewer can re-assert one.
  const nudgeAfterDays = parseNudgeAfterDays(process.env.NUDGE_AFTER_DAYS, core);
  const floorEnabled = nudgeAfterDays > 0;
  // Math.max never clamps while the floor is on - floorEnabled means at least
  // one whole day against 12h of slack - and floorMs is read nowhere else. It
  // stays as the precondition made explicit, not as a live branch.
  const floorMs = Math.max(0, nudgeAfterDays * MS_PER_DAY - FLOOR_SLACK_MS);

  // The one input value that disables a guard rather than tuning it, and the
  // only irreversible thing this job does. Annotated for the same reason a
  // malformed floor is: the run's own record has to say what was concluded.
  //
  // The permanence is claimed only where it can happen. A dry run issues no
  // request, so it writes no review_requested event and clears nothing - and
  // the dry run is the path a human exercises first, so it is the worst place
  // to assert a consequence the run is structurally incapable of having.
  if (!floorEnabled) {
    core.warning('NUDGE_AFTER_DAYS=0 - the floor and the self-removal opt-out'
      + ' are both off for this run.'
      + (dryRun
        ? ' Nothing is written, so no standing opt-out is cleared - but the same'
          + ' dispatch with dry_run off would permanently clear the opt-out of'
          + ' every reviewer it nudged.'
        : ' Any nudge issued under a floor of 0 writes a real review_requested'
          + ' event that later runs read back as "someone asked again",'
          + ' permanently clearing that reviewer\'s standing opt-out.'));
  }

  const prs = await listOpenPrs(github, owner, repo);

  const counts = {
    skipped: 0,             // PRs
    abandoned: 0,           // PRs, given up before their nudge decision could be acted on
    abandonedReviewers: 0,  // reviewers stranded on those PRs
    nudged: 0,              // reviewers
    withinFloor: 0,         // reviewers held back by the floor
    optedOut: 0,            // reviewers who removed their own review request
    failed: 0,              // reviewers
    unrequestable: 0,       // reviewers GitHub refused with a 422
  };

  // One instant for the whole run, so whether a reviewer is due cannot depend
  // on this job's position in its own walk.
  const now = Date.now();

  try {
    for (const pr of prs) {
      const skip = (why) => {
        counts.skipped++;
        core.info(`PR #${pr.number}: skip - ${why}`);
      };

      // The two paginated reads below - listReviews for every non-draft PR
      // with a non-author assignee, and the event walk for every PR with a
      // stale reviewer - are the most-executed calls in the job. Outside this
      // try a single 502 or secondary-rate-limit would propagate out of the
      // loop, abandoning every PR after it (the newest ones, since the walk is
      // oldest-first). `reading` names which of the two died, because they
      // fail differently: losing the reviews means no verdict set and so no
      // decision at all, losing the events means only that the floor was
      // unavailable.
      let reading = 'listReviews';
      let reviewersOnThisPr = 0;

      try {
        if (pr.draft) { skip('draft'); continue; }
        if (!pr.assignees?.length) { skip('no assignees'); continue; }

        const reviewers = assignedReviewers(pr);

        if (!reviewers.length) { skip('author-only assignees'); continue; }

        reviewersOnThisPr = reviewers.length;

        // Every review submitted on this PR, oldest first.
        const reviews = await github.paginate(github.rest.pulls.listReviews, {
          owner,
          repo,
          pull_number: pr.number,
          per_page: 100,
        });

        const stale = staleReviewers(reviewers, reviews, pr.head.sha);

        // Says "assigned reviewer", not "assignee": an author assigned to
        // their own PR was dropped above and never reviews, so "all assignees
        // reviewed" would be false on exactly the case that filter exists for.
        if (!stale.length) {
          skip('every assigned reviewer has a verdict on the current head');
          continue;
        }

        let contacted = new Map();
        let requested = new Map();
        let optedOut = new Map();

        if (floorEnabled) {
          reading = 'the event walk';

          // issues.listEvents, not listEventsForTimeline. Both carry
          // review_requested and review_request_removed with the same fields,
          // and neither offers a server-side type filter, so the endpoint is
          // the only lever - and the timeline is the widest feed GitHub has,
          // the only one including `commented`, `committed` and
          // `cross-referenced`. On a code-review PR those dominate, so the
          // timeline is the steeper of the two reads that scale with a PR's
          // activity rather than with PR count - which is exactly the kind of
          // PR this workflow exists to nudge.
          const events = await github.paginate(github.rest.issues.listEvents, {
            owner,
            repo,
            issue_number: pr.number,
            per_page: 100,
          });

          ({ contacted, requested, optedOut } = contactByLogin(reviews, events, pr.head.sha));
        }

        const declined = stale.filter(login => hasOptedOut(login, requested, optedOut));
        const wanted = stale.filter(login => !declined.includes(login));

        if (declined.length) {
          counts.optedOut += declined.length;
          core.info(`PR #${pr.number}: not nudging ${declined.join(', ')}`
            + ' - they removed their own review request');
        }

        const { due, held } = floorEnabled
          ? dueAndHeld(wanted, contacted, now, floorMs)
          : { due: wanted, held: [] };

        counts.withinFloor += held.length;

        // Name them and say how long ago. This is the line an operator hits
        // when they ask the only question this workflow generates - "why did
        // nobody ping X about this PR?" - and both halves of the answer are in
        // `contacted`, which the iteration is about to discard. In steady state
        // it is also most of what the job prints.
        const heldNames = held
          .map((login) => {
            const days = Math.floor((now - contacted.get(login)) / MS_PER_DAY);

            return `${login} (${days}d ago)`;
          })
          .join(', ');

        if (!due.length) {
          if (!wanted.length) {
            skip(`all ${stale.length} stale reviewer(s) removed their own`
              + ' review request');
            continue;
          }

          skip(`contacted within the last ${nudgeAfterDays} day(s): ${heldNames}`);
          continue;
        }

        // Some reviewers due on this PR and some not, so the PR is not skipped
        // and the held-back ones would otherwise reach no log line at all.
        if (held.length) {
          core.info(`PR #${pr.number}: not nudging ${heldNames}`
            + ` - contacted within the last ${nudgeAfterDays} day(s)`);
        }

        if (dryRun) {
          counts.nudged += due.length;
          core.info(`PR #${pr.number}: dry run - would nudge ${due.join(', ')}`);
          continue;
        }

        const pending = new Set((pr.requested_reviewers ?? []).map(r => r.login));
        const ref = { owner, repo, pull_number: pr.number };

        // One request per login rather than one array per PR. GitHub validates
        // the reviewers array as a unit and rejects all of it with 422 when
        // any single login is not requestable (a member who lost access), so a
        // batched call lets one such login suppress every other assignee on
        // that PR - on every run, indefinitely, since nothing about it changes
        // between runs. Per login, a failure costs only the login that caused
        // it. Up to 3 calls per pending reviewer instead of 2 per PR, which is
        // well inside the job's budget at this repo's PR count.
        for (const login of due) {
          counts[await nudgeReviewer(github, core, ref, login, pending.has(login))]++;
        }
      } catch (err) {
        counts.abandoned++;
        counts.abandonedReviewers += reviewersOnThisPr;
        core.warning(`PR #${pr.number}: abandoned during ${reading} - `
          + `HTTP ${err.status ?? '?'}: ${err.message}`);
      }
    }
  } finally {
    // Written from a finally so that a throw escaping the loop still publishes
    // what happened before it; the throw then propagates and the step goes
    // red. Wrapped, because a throw out of a finally *replaces* the exception
    // in flight rather than chaining it - so on exactly the path this block
    // exists for, a summary write that failed would have destroyed both the
    // summary and the diagnosis of what killed the run, and reported a missing
    // $GITHUB_STEP_SUMMARY instead. The counters go to the log in that case,
    // since the table that would have carried them is what just failed.
    try {
      await writeSummary(core, dryRun, nudgeAfterDays, prs.length, counts);
    } catch (summaryErr) {
      core.warning(`could not publish the job summary - ${summaryErr.message}`);
      core.info(`counters: ${JSON.stringify(counts)}`);
    }
  }

  // core.warning and core.error only emit annotations; setFailed is the sole
  // function in @actions/core that sets an exit code. Without this, a run in
  // which every nudge failed is a green check - and scheduled runs notify only
  // on failure, so nobody would ever find out.
  const notNudged = counts.failed + counts.unrequestable;

  if (notNudged || counts.abandoned) {
    core.setFailed(`${notNudged} reviewer(s) not nudged, `
      + `${counts.abandoned} PR(s) abandoned - see the job summary.`);
  }
};

module.exports = nudge;

// Exported for direct unit testing. These are the decisions with no I/O in
// them, which is where every subtle rule in this file lives - commit-id
// matching, COMMENTED carrying no verdict, own-review-on-head as contact, team
// requests excluded, self-removal as an opt-out - and asserting them through a
// fake Octokit costs a fixture per boolean.
module.exports.parseNudgeAfterDays = parseNudgeAfterDays;
module.exports.dueAndHeld = dueAndHeld;
module.exports.assignedReviewers = assignedReviewers;
module.exports.staleReviewers = staleReviewers;
module.exports.contactByLogin = contactByLogin;
module.exports.hasOptedOut = hasOptedOut;
