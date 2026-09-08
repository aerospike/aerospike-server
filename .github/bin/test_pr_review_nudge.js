#!/usr/bin/env node
// test_pr_review_nudge.js - unit tests for pr_review_nudge.js.
//
// The nudge script's mutating half - the request/strip/re-request sequence and
// its failure handling - fires only against live review-request state, and only
// when a request fails. It cannot be rehearsed against the real API:
// requestReviewers 422s for a non-collaborator, but GitHub only lets you assign
// users who already have access, so manufacturing the failure means revoking a
// real person's access mid-run. A mocked Octokit is not the second-best
// instrument here, it is the only one that reaches this code.
//
// What the mock does NOT reach, stated so it is not mistaken for covered:
// paginate() returns each fixture array whole, so no test crosses a page
// boundary. The ascending-order and de-dup reasoning in pr_review_nudge.js
// therefore has no executed evidence anywhere in this PR - the de-dup itself is
// pinned below, but the pagination behaviour it defends against is argued, not
// demonstrated.
//
// Zero dependencies, no network. Run: node .github/bin/test_pr_review_nudge.js

'use strict';

const test = require('node:test');
const assert = require('node:assert');

const nudge = require('./pr_review_nudge.js');

const HEAD = 'headsha0000000000000000000000000000000';
const OLD = 'oldsha00000000000000000000000000000000';

// --- fixture builders -------------------------------------------------------

function pull(number, extra = {}) {
  return {
    number,
    draft: false,
    user: { login: 'author' },
    head: { sha: HEAD },
    assignees: [{ login: 'alice' }],
    requested_reviewers: [],
    ...extra,
  };
}

function review(login, state, commit = HEAD, submittedAt = null) {
  return { user: { login }, state, commit_id: commit, submitted_at: submittedAt };
}

function hoursAgo(n) {
  return new Date(Date.now() - n * 60 * 60 * 1000).toISOString();
}

function daysAgo(n) {
  return hoursAgo(n * 24);
}

// review_requested carries the actor who asked, but the script reads only
// requested_reviewer: any such event is someone asking again, whoever asked and
// whether or not an actor is there at all. `null` builds the actor-less form -
// GitHub does not guarantee the field - and the reviewer's own login builds the
// self-request, which GitHub permits from any non-author. Both edges need their
// own test, because a fixture can only kill the narrowings its own actor field
// falls on the wrong side of: an actor-less one kills a presence test, a
// self-actor one kills an inequality. The load-bearing actor is removedEvent's,
// which drives the genuine self-removal narrowing.
function requestedEvent(login, at, actor = 'maintainer') {
  return {
    event: 'review_requested',
    created_at: at,
    ...(actor === null ? {} : { actor: { login: actor } }),
    requested_reviewer: { login },
  };
}

// review_request_removed carries the same payload shape as review_requested,
// plus the actor who did the removing - which is what separates a reviewer
// declining from a maintainer tidying the queue. `null` builds the actor-less
// form, which is nobody's decline: a removal naming no one who made it cannot
// be read as the reviewer's own.
function removedEvent(login, at, actor = login) {
  return {
    event: 'review_request_removed',
    created_at: at,
    ...(actor === null ? {} : { actor: { login: actor } }),
    requested_reviewer: { login },
  };
}

function httpError(status, message) {
  const err = new Error(message);
  err.status = status;
  return err;
}

// A namespace that throws on any property the script has no business touching,
// and records every property it does touch. The throw is what makes "never
// posts a PR comment" enforceable - issues.createComment and pulls.createReview
// are not stubbed, so reaching for either fails whichever test touches it - and
// the record is what lets one test assert the whole reached surface directly
// rather than inferring it from the absence of failures elsewhere.
function strictNamespace(name, methods, touched = []) {
  return new Proxy(methods, {
    get(target, prop) {
      touched.push(`${name}.${String(prop)}`);
      if (prop in target) { return target[prop]; }
      throw new Error(`unexpected API call: ${name}.${String(prop)}`);
    },
  });
}

// Builds a fake github/context/core triple and runs the script against it.
//
//   reviews:  { [prNumber]: review[] }        - what listReviews returns
//   events:   { [prNumber]: event[] }         - what issues.listEvents returns
//   onRemove: (params, callIndex) => void     - throw to reject the DELETE
//   onRequest:(params, callIndex) => void     - throw to reject the POST
//   listReviewsFails: { [prNumber]: Error }   - reject listReviews for that PR
//   eventsFail:       { [prNumber]: Error }   - reject the event walk for that PR
//   dryRun: true | false | null               - null leaves DRY_RUN unset
//   nudgeAfterDays                            - the floor, verbatim; omit for
//                                               the default. Not coerced to a
//                                               number, so a malformed dispatch
//                                               value can be exercised.
async function run({
  prs = [],
  reviews = {},
  events = {},
  onRemove = () => {},
  onRequest = () => {},
  listReviewsFails = {},
  eventsFail = {},
  dryRun = false,
  nudgeAfterDays = undefined,
} = {}) {
  const listCalls = [];
  const removeCalls = [];
  const requestCalls = [];
  const eventsCalls = [];
  const reviewsCalls = [];
  // Ordered, because which call runs first is the whole point of the sequence
  // in nudgeReviewer: proving before removing is what closes the window in
  // which a DELETE has succeeded and its POST has not.
  const order = [];
  const logs = { info: [], warning: [], error: [], failed: [] };
  const summaries = [];
  const touched = [];

  const pulls = {
    list: function list() {},
    listReviews: function listReviews() {},
    async removeRequestedReviewers(params) {
      removeCalls.push(params);
      order.push(`DELETE #${params.pull_number} ${params.reviewers.join()}`);
      onRemove(params, removeCalls.length - 1);
    },
    async requestReviewers(params) {
      requestCalls.push(params);
      order.push(`POST #${params.pull_number} ${params.reviewers.join()}`);
      onRequest(params, requestCalls.length - 1);
    },
  };

  // issues is reachable only for listEvents; createComment is not stubbed, so
  // the "never comments" guarantee survives opening the namespace.
  const issues = { listEvents: function listEvents() {} };

  const rest = strictNamespace('rest', {
    pulls: strictNamespace('pulls', pulls, touched),
    issues: strictNamespace('issues', issues, touched),
  }, touched);

  const summary = {
    _rows: null,
    addHeading(text) { this._heading = text; return this; },
    addTable(rows) { this._rows = rows; return this; },
    async write() { summaries.push({ heading: this._heading, rows: this._rows }); return this; },
  };

  // Strict at the roots too, not only under github.rest: a reach for
  // github.graphql or a core method the script has no business using would
  // otherwise go unpoliced.
  const github = strictNamespace('github', {
    rest,
    async paginate(fn, params) {
      if (fn === pulls.list) {
        listCalls.push(params);
        return prs;
      }
      if (fn === pulls.listReviews) {
        reviewsCalls.push(params);
        const fail = listReviewsFails[params.pull_number];
        if (fail) { throw fail; }
        return reviews[params.pull_number] ?? [];
      }
      if (fn === issues.listEvents) {
        eventsCalls.push(params);
        const fail = eventsFail[params.issue_number];
        if (fail) { throw fail; }
        return events[params.issue_number] ?? [];
      }
      throw new Error('unexpected paginate target');
    },
  }, touched);

  const core = strictNamespace('core', {
    info: (m) => logs.info.push(m),
    warning: (m) => logs.warning.push(m),
    error: (m) => logs.error.push(m),
    setFailed: (m) => logs.failed.push(m),
    summary,
  }, touched);

  const context = { repo: { owner: 'citrusleaf', repo: 'aerospike-server' } };

  const prevDry = process.env.DRY_RUN;
  const prevFloor = process.env.NUDGE_AFTER_DAYS;

  if (dryRun === null) {
    delete process.env.DRY_RUN;
  } else {
    process.env.DRY_RUN = dryRun ? 'true' : 'false';
  }

  if (nudgeAfterDays === undefined) {
    delete process.env.NUDGE_AFTER_DAYS;
  } else {
    process.env.NUDGE_AFTER_DAYS = String(nudgeAfterDays);
  }

  const restore = (key, value) => {
    if (value === undefined) { delete process.env[key]; } else { process.env[key] = value; }
  };

  try {
    await nudge({ github, context, core });
  } finally {
    restore('DRY_RUN', prevDry);
    restore('NUDGE_AFTER_DAYS', prevFloor);
  }

  // The strict proxy's throw is only as good as the assertion that notices it,
  // and the script swallows in-loop throws by design - so on any branch no
  // test happens to inspect, a forbidden call would surface only as an
  // abandoned-PR warning that nobody reads. Checked here, once, for every
  // branch present and future. The phrase is the harness's own (see
  // strictNamespace), so a real GitHub message cannot false-positive it.
  const leaked = logs.warning.concat(logs.error, logs.failed)
    .filter(m => /unexpected API call/.test(m));

  assert.deepEqual(leaked, [], 'the script reached for an API it may not touch');

  // The same check for a forbidden call the script caught itself. The proxy
  // records the property before it throws, so a `try { createComment } catch`
  // leaves a trace here that no log line carries - and the reached-surface
  // assertion that would see it runs over one fixture, which walks neither the
  // abandon path, the early skips, nor a failed re-request. A subset test, not
  // an equality one, so a branch this fixture does not reach costs nothing and
  // the surface's exact contents stay the property of their own test.
  const allowed = new Set([
    'core.error', 'core.info', 'core.setFailed', 'core.summary', 'core.warning',
    'github.paginate', 'github.rest', 'issues.listEvents', 'pulls.list',
    'pulls.listReviews', 'pulls.removeRequestedReviewers', 'pulls.requestReviewers',
    'rest.issues', 'rest.pulls',
  ]);

  assert.deepEqual([...new Set(touched)].filter(prop => !allowed.has(prop)).sort(), [],
    'the script reached for an API it may not touch, and swallowed the refusal');

  // Universal for the same reason the check above is: every per-test assertion
  // on these two calls projects onto `.reviewers`, so a sibling parameter rides
  // along unseen. Both endpoints accept `team_reviewers`, and on the DELETE that
  // strips a whole team's live review request - the one hazard the per-login
  // scoping exists to prevent, in the one field no test looks at.
  for (const params of [...removeCalls, ...requestCalls]) {
    assert.deepEqual(Object.keys(params).sort(),
      ['owner', 'pull_number', 'repo', 'reviewers'],
      'a mutating call carried a parameter it has no business carrying');
  }

  return {
    listCalls,
    removeCalls,
    requestCalls,
    eventsCalls,
    reviewsCalls,
    order,
    logs,
    summaries,
    touched: [...new Set(touched)].sort(),
    counts: counts(summaries),
  };
}

// Reads the metric rows back out of the summary table, which is the only place
// the script publishes its counters. Deliberately asserts nothing: cardinality
// is a property one test owns, and asserting it here attributed a single
// regression to every test that reads a counter.
function counts(summaries) {
  const out = {};

  for (const row of summaries[summaries.length - 1].rows.slice(1)) {
    out[row[0]] = Number(row[1]);
  }

  return out;
}

// --- the predicate, called directly -----------------------------------------

test('assignedReviewers drops the author and tolerates a missing key', () => {
  assert.deepEqual(
    nudge.assignedReviewers(pull(1, { assignees: [{ login: 'author' }, { login: 'alice' }] })),
    ['alice']);
  assert.deepEqual(nudge.assignedReviewers({ user: { login: 'author' } }), []);
  assert.deepEqual(
    nudge.assignedReviewers({ user: null, assignees: [{ login: 'alice' }] }),
    ['alice'],
    'a PR opened by a since-deleted account still has assignees');
});

test('staleReviewers wants a verdict from this login on this head', () => {
  const stale = (reviews) => nudge.staleReviewers(['alice'], reviews, HEAD);

  assert.deepEqual(stale([]), ['alice'], 'no review at all is stale');
  assert.deepEqual(stale([review('alice', 'APPROVED')]), []);
  assert.deepEqual(stale([review('alice', 'CHANGES_REQUESTED')]), []);
  assert.deepEqual(stale([review('alice', 'COMMENTED')]), ['alice'],
    'COMMENTED carries no verdict');
  assert.deepEqual(stale([review('alice', 'APPROVED', OLD)]), ['alice'],
    'a verdict on a superseded head is not a verdict on this one');
  assert.deepEqual(stale([review('bob', 'APPROVED')]), ['alice'],
    "someone else's approval is not alice's");
  assert.deepEqual(stale([{ user: null, state: 'APPROVED', commit_id: HEAD }]), ['alice'],
    'a review by a deleted account exempts nobody');
});

test('contactByLogin reads only the events and reviews that are contact', () => {
  const { contacted, requested, optedOut } = nudge.contactByLogin(
    [
      review('alice', 'COMMENTED', HEAD, daysAgo(1)),
      review('bob', 'CHANGES_REQUESTED', OLD, daysAgo(1)),
    ],
    [
      requestedEvent('carol', daysAgo(3)),
      { event: 'review_requested', created_at: daysAgo(0), requested_team: { slug: 'core' } },
      { event: 'assigned', created_at: daysAgo(0), assignee: { login: 'dave' } },
      removedEvent('erin', daysAgo(2)),
      removedEvent('frank', daysAgo(2), 'maintainer'),
      requestedEvent('grace', 'not-a-date'),
    ],
    HEAD);

  assert.deepEqual([...contacted.keys()].sort(), ['alice', 'carol'],
    'own review on the head and a personal review request; not a superseded'
    + ' review, not a team request, not `assigned`, not a bad timestamp');
  assert.deepEqual([...requested.keys()], ['carol'],
    'only a review request is someone asking - a login\'s own review is'
    + ' contact for the floor, never an ask');
  assert.deepEqual([...optedOut.keys()], ['erin'],
    'only a self-removal is an opt-out');
});

test('dueAndHeld partitions the wanted set, and never contacted means due', () => {
  const now = 1_000 * 86_400_000;
  const floor = 7 * 86_400_000;
  const at = (map) => new Map(Object.entries(map));

  const { due, held } = nudge.dueAndHeld(['alice', 'bob', 'carol'],
    at({ alice: now - 1 * 86_400_000, bob: now - 30 * 86_400_000 }), now, floor);

  assert.deepEqual(due, ['bob', 'carol'],
    'outside the floor is due, and never contacted at all is always due');
  assert.deepEqual(held, ['alice']);

  // The invariant the caller's counter rests on, and the reason the held set is
  // returned rather than recomputed as a complement.
  assert.equal(held.length, 3 - due.length);
  assert.deepEqual([...due, ...held].sort(), ['alice', 'bob', 'carol']);

  // Every held login has a contact entry, which is what lets the caller say how
  // long ago. A login with none has `now - 0` behind it and cannot be held.
  const uncontacted = nudge.dueAndHeld(['dave'], new Map(), now, floor);

  assert.deepEqual(uncontacted, { due: ['dave'], held: [] });
});

test('hasOptedOut holds until someone asks again', () => {
  const at = (map) => new Map(Object.entries(map));

  assert.equal(nudge.hasOptedOut('alice', at({}), at({ alice: 200 })), true);
  assert.equal(nudge.hasOptedOut('alice', at({ alice: 300 }), at({ alice: 200 })), false,
    'a later review request overrides an earlier self-removal');
  assert.equal(nudge.hasOptedOut('alice', at({ alice: 100 }), at({ alice: 200 })), true);
  assert.equal(nudge.hasOptedOut('alice', at({ alice: 100 }), at({})), false);
});

test('parseNudgeAfterDays refuses to guess, and never fails open', () => {
  const warnings = [];
  const core = { warning: (m) => warnings.push(m) };
  const parse = (raw) => nudge.parseNudgeAfterDays(raw, core);

  // Unset and blank are "no opinion", which is the default, not the off switch.
  assert.equal(parse(undefined), 7);
  assert.equal(parse(''), 7);
  assert.equal(parse(' '), 7);
  assert.equal(parse('\t'), 7);
  assert.equal(warnings.length, 0, 'an absent opinion is not worth an annotation');

  // A deliberate 0 is the documented revert path and must survive.
  assert.equal(parse('0'), 0);
  assert.equal(parse('14'), 14);
  assert.equal(parse('365'), 365, 'the documented bound is inclusive');

  // Everything Number() used to coerce to 0 - which disabled the floor.
  for (const raw of ['0x0', '0e5', '+0', '1e-400', '-0']) {
    assert.equal(parse(raw), 7, `${raw} must not disable the floor`);
  }

  // Everything that used to be swallowed silently, or to overflow the product
  // to Infinity and make every reviewer permanently undue.
  for (const raw of ['abc', '7d', '-1', 'Infinity', '0.5', '1e308', '366', '400']) {
    assert.equal(parse(raw), 7, `${raw} must fall back`);
  }

  assert.equal(warnings.length, 13, 'every rejected value is annotated');
  assert.match(warnings[0], /NUDGE_AFTER_DAYS="0x0" is not a whole number of days/);
});

// --- the predicate, through the entry point ---------------------------------

test('COMMENTED on the head does not exempt; APPROVED does', async () => {
  // The discriminating pair: same login, same head, one variable. If COMMENTED
  // were ever treated as a verdict, #1 flips from nudged to skipped.
  const r = await run({
    prs: [
      pull(1, { assignees: [{ login: 'cinterloper' }] }),
      pull(2, { assignees: [{ login: 'cinterloper' }] }),
    ],
    reviews: {
      1: [review('cinterloper', 'COMMENTED')],
      2: [review('cinterloper', 'APPROVED'), review('cinterloper', 'COMMENTED')],
    },
  });

  assert.deepEqual(r.requestCalls.map(c => c.pull_number), [1]);
  assert.equal(r.counts['PRs skipped'], 1);
  assert.equal(r.counts['Reviewers nudged'], 1);
});

test('a verdict on an older head does not exempt', async () => {
  const r = await run({
    prs: [pull(1)],
    reviews: { 1: [review('alice', 'APPROVED', OLD)] },
  });

  assert.deepEqual(r.requestCalls.map(c => c.reviewers), [['alice']]);
});

test('draft, no-assignee and author-only PRs are skipped with a reason', async () => {
  const r = await run({
    prs: [
      pull(1, { draft: true }),
      pull(2, { assignees: [] }),
      pull(3, { assignees: [{ login: 'author' }] }),
      pull(4, { assignees: undefined }),
    ],
  });

  assert.equal(r.removeCalls.length, 0);
  assert.equal(r.requestCalls.length, 0);
  assert.equal(r.counts['PRs skipped'], 4);
  assert.deepEqual(r.logs.info.map(m => m.replace(/^PR #\d+: /, '')), [
    'skip - draft',
    'skip - no assignees',
    'skip - author-only assignees',
    'skip - no assignees',
  ]);
});

test('the all-reviewed skip message does not claim anything about the author', async () => {
  // pr.user is also an assignee, so "all assignees reviewed" would be false.
  const r = await run({
    prs: [pull(1, { assignees: [{ login: 'author' }, { login: 'alice' }] })],
    reviews: { 1: [review('alice', 'APPROVED')] },
  });

  assert.equal(r.counts['PRs skipped'], 1);
  assert.match(r.logs.info[0], /every assigned reviewer has a verdict on the current head/);
});

test('the PR walk asks this repo for open PRs, oldest first', async () => {
  const r = await run({ prs: [pull(1)] });

  assert.equal(r.listCalls.length, 1);
  assert.equal(r.listCalls[0].state, 'open', 'a closed or merged PR must never be nudged');
  assert.equal(r.listCalls[0].sort, 'created');
  assert.equal(r.listCalls[0].direction, 'asc');
  assert.equal(r.listCalls[0].owner, 'citrusleaf');
  assert.equal(r.listCalls[0].repo, 'aerospike-server');
  assert.equal(r.listCalls[0].per_page, 100);

  // Every paginated read is scoped and sized here, not only the PR walk: an
  // unscoped listReviews reads another repo's verdicts, and a missing per_page
  // triples the request count the rate-limit budget was measured against.
  assert.deepEqual(r.reviewsCalls,
    [{ owner: 'citrusleaf', repo: 'aerospike-server', pull_number: 1, per_page: 100 }]);
});

test('a PR listed twice is nudged once', async () => {
  // Offset pagination over a result set recomputed per request can hand the
  // same PR back on two pages; the de-dup is the only thing between that and a
  // doubled notification.
  const r = await run({ prs: [pull(1), pull(1)] });

  assert.deepEqual(r.requestCalls.map(c => c.pull_number), [1]);
  assert.equal(r.counts['PRs scanned'], 1);
});

// --- the mutating half ------------------------------------------------------

test('a pending reviewer is proven, stripped, then re-requested', async () => {
  // The POST comes first even though it notifies nobody: it proves the login is
  // requestable before the DELETE removes anything.
  const r = await run({
    prs: [pull(1, { requested_reviewers: [{ login: 'alice' }] })],
  });

  assert.deepEqual(r.order, ['POST #1 alice', 'DELETE #1 alice', 'POST #1 alice'],
    'the proving POST must precede the DELETE');
  assert.equal(r.counts['Reviewers nudged'], 1);

  for (const call of [...r.requestCalls, ...r.removeCalls]) {
    assert.equal(call.owner, 'citrusleaf');
    assert.equal(call.repo, 'aerospike-server');
    assert.equal(call.pull_number, 1);
  }
});

test('a non-pending reviewer is requested without a strip', async () => {
  const r = await run({ prs: [pull(1)] });

  assert.equal(r.removeCalls.length, 0);
  assert.deepEqual(r.requestCalls.map(c => c.reviewers), [['alice']]);
});

test('a pending reviewer who is not a nudge target is left alone', async () => {
  // The DELETE is the only irreversible call in the job, and its safety rests
  // entirely on being scoped to the one login being nudged. Widen it to the
  // whole pending set and bob loses a live review request, silently, on every
  // run.
  const r = await run({
    prs: [pull(1, {
      assignees: [{ login: 'alice' }],
      requested_reviewers: [{ login: 'alice' }, { login: 'bob' }],
    })],
  });

  assert.deepEqual(r.removeCalls.map(c => c.reviewers), [['alice']],
    "bob's live review request must not be stripped");
  assert.deepEqual(r.requestCalls.map(c => c.reviewers), [['alice'], ['alice']]);
});

test('one non-requestable login does not suppress the others on the same PR', async () => {
  // The batch-vs-per-login case: a single call carrying [alice, dave] 422s as
  // a unit and alice gets nothing. Per login, only dave fails.
  const r = await run({
    prs: [pull(1, { assignees: [{ login: 'alice' }, { login: 'dave' }] })],
    onRequest: (params) => {
      if (params.reviewers[0] === 'dave') { throw httpError(422, 'not a collaborator'); }
    },
  });

  assert.deepEqual(r.requestCalls.map(c => c.reviewers), [['alice'], ['dave']]);
  assert.equal(r.counts['Reviewers nudged'], 1);
  assert.equal(r.counts['Reviewers not requestable'], 1,
    'a 422 on the proving request is about the login, not the moment');
  assert.equal(r.counts['Reviewers failed'], 0);
  assert.equal(r.logs.failed.length, 1, 'it still turns the run red');
  assert.match(r.logs.warning[0], /request failed for dave - HTTP 422/);
});

test('a 422 on the proving request removes nothing', async () => {
  const r = await run({
    prs: [pull(1, { requested_reviewers: [{ login: 'alice' }] })],
    onRequest: () => { throw httpError(422, 'not a collaborator'); },
  });

  assert.equal(r.removeCalls.length, 0,
    'the strip must never run for a login GitHub just refused');
  assert.equal(r.requestCalls.length, 1);
  assert.equal(r.logs.error.length, 0, 'nothing was destroyed, so nothing is an error');
  assert.equal(r.counts['Reviewers not requestable'], 1);
});

test('a 422 on the strip costs the nudge and leaves the request intact', async () => {
  // The realistic strip failure, and the one the old order could not express:
  // retries: 3 absorbs 5xx before the script sees it and 422 is on the action's
  // exempt list, so a stale `pending` snapshot arrives here immediately. The
  // policy is that it costs that reviewer's nudge and turns the run red - not
  // that it is swallowed and the re-request proceeds anyway.
  const r = await run({
    prs: [pull(1, { requested_reviewers: [{ login: 'alice' }] })],
    onRemove: () => { throw httpError(422, 'Reviewer is not requested'); },
  });

  assert.equal(r.requestCalls.length, 1, 'only the proving request may have run');
  assert.equal(r.counts['Reviewers failed'], 1);
  assert.equal(r.counts['Reviewers not requestable'], 0, 'the login is requestable');
  assert.equal(r.logs.error.length, 0, "alice's request was never removed");
  assert.match(r.logs.warning[0], /PR #1: strip failed for alice - HTTP 422/);
});

test('a failing strip logs a warning, not an error, and the loop advances', async () => {
  const r = await run({
    prs: [
      pull(1, { requested_reviewers: [{ login: 'alice' }] }),
      pull(2),
    ],
    onRemove: () => { throw httpError(500, 'Server Error'); },
  });

  assert.deepEqual(r.requestCalls.map(c => c.pull_number), [1, 2],
    'PR #1 got its proving request; PR #2 must still be processed');
  assert.equal(r.logs.error.length, 0, 'nothing was lost, so nothing may be reported lost');
  assert.match(r.logs.warning[0], /PR #1: strip failed for alice - HTTP 500: Server Error/);
  assert.equal(r.counts['Reviewers failed'], 1);

  // Both terms of the aggregate, not just the 422 one: a 500 on the strip is
  // the failure class `retries: 3` does not absorb, and it has to turn the run
  // red on its own. Scheduled runs notify only on failure.
  assert.equal(r.logs.failed.length, 1, 'a failed nudge turns the run red too');
  assert.match(r.logs.failed[0], /1 reviewer\(s\) not nudged/);
});

test('a failing re-request is an error, because it destroyed a live request', async () => {
  // The one failure in the sequence that leaves the PR worse than it started:
  // the request was stripped and could not be re-made. It self-heals - the
  // strip recorded no contact, so the login is still due, and it is no longer
  // pending, so the next run needs one POST - but it is the line a human must
  // see, so it is an error and not a warning.
  const r = await run({
    prs: [
      pull(1, { requested_reviewers: [{ login: 'alice' }] }),
      pull(2),
    ],
    onRequest: (params, n) => {
      if (params.pull_number === 1 && n === 1) { throw httpError(403, 'secondary rate limit'); }
    },
  });

  assert.equal(r.removeCalls.length, 1);
  assert.equal(r.logs.error.length, 1);
  assert.match(r.logs.error[0], /PR #1: re-request failed for alice - HTTP 403/);
  assert.equal(r.logs.warning.length, 0);
  assert.ok(r.requestCalls.some(c => c.pull_number === 2), 'PR #2 must still be processed');
  assert.equal(r.counts['Reviewers nudged'], 1);
  assert.equal(r.counts['Reviewers failed'], 1);
});

test('a failure on one PR does not abort the ones after it', async () => {
  const r = await run({
    prs: [pull(1), pull(2), pull(3)],
    onRequest: (params) => {
      if (params.pull_number === 2) { throw httpError(403, 'secondary rate limit'); }
    },
  });

  assert.deepEqual(r.requestCalls.map(c => c.pull_number), [1, 2, 3]);
  assert.equal(r.counts['Reviewers nudged'], 2);
  assert.equal(r.counts['Reviewers failed'], 1);
  assert.match(r.logs.warning[0], /HTTP 403: secondary rate limit/);
});

test('a listReviews failure costs only its own PR, and says which read died', async () => {
  // Two of the four abandon, so the counters are observed accumulating rather
  // than at a cardinality of one, where `=` and `+=` are indistinguishable.
  const r = await run({
    prs: [pull(1), pull(2), pull(3), pull(4)],
    listReviewsFails: {
      2: httpError(502, 'Bad Gateway'),
      4: httpError(502, 'Bad Gateway'),
    },
  });

  assert.deepEqual(r.requestCalls.map(c => c.pull_number), [1, 3]);
  assert.equal(r.counts['PRs abandoned'], 2, 'one per PR, not the last one');
  assert.equal(r.counts['Reviewers on abandoned PRs'], 2,
    'the reviewers those PRs stranded must land in some row, one per PR');
  assert.match(r.logs.warning[0],
    /PR #2: abandoned during listReviews - HTTP 502: Bad Gateway/);
  assert.equal(r.logs.failed.length, 1, 'an abandoned PR must turn the run red');
});

test('an event-walk failure is distinguished from a listReviews failure', async () => {
  // Losing the reviews means no verdict set, so no decision at all. Losing the
  // events means only that the floor was unavailable. Different fix, different
  // urgency, and this is the sole diagnostic for a red scheduled run.
  const r = await run({
    prs: [pull(1), pull(2)],
    eventsFail: { 1: httpError(502, 'Bad Gateway') },
    nudgeAfterDays: 7,
  });

  assert.deepEqual(r.requestCalls.map(c => c.pull_number), [2]);
  assert.equal(r.counts['PRs abandoned'], 1);
  assert.match(r.logs.warning[0],
    /PR #1: abandoned during the event walk - HTTP 502: Bad Gateway/);
});

test('a PR or a review by a deleted account does not abort the PR', async () => {
  // user: null is what a deleted account leaves behind on both. Without the
  // optional chains that is a TypeError, caught by the per-PR handler, so the
  // symptom is a live PR quietly reported as abandoned.
  const r = await run({
    prs: [pull(1, { user: null }), pull(2)],
    reviews: { 2: [{ user: null, state: 'APPROVED', commit_id: HEAD, submitted_at: null }] },
  });

  assert.deepEqual(r.requestCalls.map(c => c.pull_number), [1, 2]);
  assert.equal(r.counts['PRs abandoned'], 0);
});

test('a PR with no requested_reviewers key is requested without a strip', async () => {
  const r = await run({ prs: [pull(1, { requested_reviewers: undefined })] });

  assert.equal(r.removeCalls.length, 0);
  assert.deepEqual(r.requestCalls.map(c => c.reviewers), [['alice']]);
});

// --- the floor --------------------------------------------------------------

test('a reviewer contacted inside the floor is held back, by name', async () => {
  const r = await run({
    prs: [pull(1)],
    events: { 1: [requestedEvent('alice', daysAgo(2))] },
    nudgeAfterDays: 7,
  });

  assert.equal(r.requestCalls.length, 0);
  assert.equal(r.counts['Reviewers held back by the floor'], 1);
  assert.match(r.logs.info[0], /contacted within the last 7 day\(s\): alice \(2d ago\)/,
    'the only question this workflow generates is who, and how long ago');
});

test('a reviewer contacted outside the floor is nudged', async () => {
  const r = await run({
    prs: [pull(1)],
    events: { 1: [requestedEvent('alice', daysAgo(30))] },
    nudgeAfterDays: 7,
  });

  assert.deepEqual(r.requestCalls.map(c => c.reviewers), [['alice']]);
  assert.equal(r.counts['Reviewers held back by the floor'], 0);
});

test('the floor tolerates cron drift rather than slipping a whole period', async () => {
  // 7 days is exactly the weekday period of the schedule, so a contact made by
  // last week's run is ~7 days old to within GitHub's queue delay. Compared
  // with no slack, an hour of drift defers the nudge to the next run - a 9-day
  // effective cadence decided by noise.
  const r = await run({
    prs: [pull(1)],
    events: { 1: [requestedEvent('alice', hoursAgo(167))] },
    nudgeAfterDays: 7,
  });

  assert.deepEqual(r.requestCalls.map(c => c.reviewers), [['alice']],
    'an hour short of 7 days is still due');
});

test('the floor tolerates a multi-hour queue delay', async () => {
  // Brackets the slack from below: together with the daysAgo(6) case in the
  // default-floor test, which pins it under 24h, the slack is held to (8h,
  // 24h). Without this, a silent shrink to an hour reintroduces the 7-to-9-day
  // cadence oscillation and CI cannot tell.
  const r = await run({
    prs: [pull(1)],
    events: { 1: [requestedEvent('alice', hoursAgo(160))] },
    nudgeAfterDays: 7,
  });

  assert.deepEqual(r.requestCalls.map(c => c.reviewers), [['alice']],
    '8 hours short of 7 days is still due');
});

test('the most recent contact wins, whichever order it arrives in', async () => {
  // Both orders, because the feed's order is GitHub's to choose: a walk that
  // simply keeps the last entry it sees is right on one of these and wrong on
  // the other.
  const ascending = await run({
    prs: [pull(1)],
    events: { 1: [requestedEvent('alice', daysAgo(40)), requestedEvent('alice', daysAgo(1))] },
    nudgeAfterDays: 7,
  });
  const descending = await run({
    prs: [pull(1)],
    events: { 1: [requestedEvent('alice', daysAgo(1)), requestedEvent('alice', daysAgo(40))] },
    nudgeAfterDays: 7,
  });

  assert.equal(ascending.requestCalls.length, 0,
    'the 1-day-old request must win over the 40-day-old one');
  assert.equal(descending.requestCalls.length, 0, 'and must still win when it comes first');
});

test('a reviewer never contacted is nudged - being assigned is not being asked', async () => {
  // The majority case on any real queue: `assigned` and `review_requested` are
  // separate events and only the latter puts a PR in someone's review queue, so
  // for these the nudge is the first review request they have ever received. A
  // floor keyed on pr.updated_at would have delayed exactly these.
  const r = await run({
    prs: [pull(1)],
    events: { 1: [{ event: 'assigned', created_at: daysAgo(0), assignee: { login: 'alice' } }] },
    nudgeAfterDays: 7,
  });

  assert.deepEqual(r.requestCalls.map(c => c.reviewers), [['alice']]);
});

test("a reviewer's own recent review counts as contact", async () => {
  // Someone who left inline notes on the current head within the floor:
  // COMMENTED carries no verdict so they are stale, but pinging them the day
  // after they engaged is the habituation failure the floor exists to avoid.
  const r = await run({
    prs: [pull(1)],
    reviews: { 1: [review('alice', 'COMMENTED', HEAD, daysAgo(1))] },
    events: { 1: [requestedEvent('alice', daysAgo(40))] },
    nudgeAfterDays: 7,
  });

  assert.equal(r.requestCalls.length, 0, 'a review submitted yesterday is contact');
  assert.equal(r.counts['Reviewers held back by the floor'], 1);
});

test('a review of a superseded head is not contact', async () => {
  // A reviewer who requested changes inside the floor and whose objection the
  // author has since pushed past. Counting that as contact would hold the nudge
  // back on the PR whose author is actively waiting - the person most worth
  // nudging, suppressed hardest.
  const r = await run({
    prs: [pull(1)],
    reviews: { 1: [review('alice', 'CHANGES_REQUESTED', OLD, daysAgo(1))] },
    events: { 1: [requestedEvent('alice', daysAgo(40))] },
    nudgeAfterDays: 7,
  });

  assert.deepEqual(r.requestCalls.map(c => c.reviewers), [['alice']]);
  assert.equal(r.counts['Reviewers held back by the floor'], 0);
});

test('a team review request is not contact for any individual', async () => {
  // requested_reviewer is absent when a team was requested; treating it as
  // contact would silently exempt whoever the Map defaulted to.
  const r = await run({
    prs: [pull(1)],
    events: { 1: [{ event: 'review_requested', created_at: daysAgo(0), requested_team: { slug: 'core' } }] },
    nudgeAfterDays: 7,
  });

  assert.deepEqual(r.requestCalls.map(c => c.reviewers), [['alice']]);
});

test('the floor holds back only the reviewers inside it', async () => {
  const r = await run({
    prs: [pull(1, { assignees: [{ login: 'alice' }, { login: 'dave' }] })],
    events: { 1: [requestedEvent('alice', daysAgo(1)), requestedEvent('dave', daysAgo(20))] },
    nudgeAfterDays: 7,
  });

  assert.deepEqual(r.requestCalls.map(c => c.reviewers), [['dave']]);
  assert.equal(r.counts['Reviewers nudged'], 1);
  assert.equal(r.counts['Reviewers held back by the floor'], 1);

  // The PR is not skipped, so the held-back half reaches no skip() line. Named
  // here or nowhere: this is the mixed case, where an operator has positive
  // evidence the job ran on the PR and chose not to ping alice.
  assert.ok(r.logs.info.includes('PR #1: not nudging alice (1d ago)'
    + ' - contacted within the last 7 day(s)'),
    'a reviewer held back on a PR that was still nudged is named anyway');

  // That line has to come before the dry-run branch returns, because a dry run
  // is where an operator checks who the floor is holding.
  const dry = await run({
    prs: [pull(1, { assignees: [{ login: 'alice' }, { login: 'dave' }] })],
    events: { 1: [requestedEvent('alice', daysAgo(1)), requestedEvent('dave', daysAgo(20))] },
    nudgeAfterDays: 7,
    dryRun: true,
  });

  assert.equal(dry.requestCalls.length, 0);
  assert.ok(dry.logs.info.includes('PR #1: not nudging alice (1d ago)'
    + ' - contacted within the last 7 day(s)'),
    'a dry run names the held-back reviewers too');
});

test('the floor counter aggregates across PRs', async () => {
  const r = await run({
    prs: [pull(1), pull(2), pull(3, { assignees: [{ login: 'dave' }] })],
    events: {
      1: [requestedEvent('alice', daysAgo(1))],
      2: [requestedEvent('alice', daysAgo(2))],
      3: [requestedEvent('dave', daysAgo(30))],
    },
    nudgeAfterDays: 7,
  });

  assert.equal(r.counts['Reviewers held back by the floor'], 2, 'one per PR, not the last one');
  assert.equal(r.counts['Reviewers nudged'], 1);
});

test('the floor defaults to 7 days', async () => {
  const inside = await run({ prs: [pull(1)], events: { 1: [requestedEvent('alice', daysAgo(6))] } });
  const outside = await run({ prs: [pull(1)], events: { 1: [requestedEvent('alice', daysAgo(8))] } });

  assert.equal(inside.requestCalls.length, 0, '6 days is inside the default floor');
  assert.equal(outside.requestCalls.length, 1, '8 days is outside it');
});

test('a floor of 0 disables the floor and issues no event call', async () => {
  const r = await run({
    prs: [pull(1)],
    events: { 1: [requestedEvent('alice', daysAgo(0))] },
    nudgeAfterDays: 0,
  });

  assert.equal(r.eventsCalls.length, 0, 'a disabled floor must not pay for the event walk');
  assert.deepEqual(r.requestCalls.map(c => c.reviewers), [['alice']]);
  assert.equal(r.counts['Nudge floor (days)'], 0);
  assert.equal(r.counts['Reviewers held back by the floor'], 0);

  // The summary's `Nudge floor (days): 0` is a number, not a consequence, and
  // the dispatch input's description is visible only before the run. The one
  // irreversible thing this job does has to be in its own record.
  assert.ok(r.logs.warning.some(m => /NUDGE_AFTER_DAYS=0 - the floor and the self-removal opt-out/.test(m)
                                  && /permanently clearing/.test(m)),
    'a floor of 0 announces that it clears standing opt-outs');

  // A dry run issues no request, so it writes no review_requested event and
  // clears nothing. The disabled guards are still worth saying; the permanence
  // is the one claim it must not make.
  const dry = await run({
    prs: [pull(1)],
    events: { 1: [requestedEvent('alice', daysAgo(0))] },
    nudgeAfterDays: 0,
    dryRun: true,
  });

  assert.equal(dry.requestCalls.length, 0);
  assert.ok(dry.logs.warning.some(m => /the floor and the self-removal opt-out are both off/.test(m)
                                    && /Nothing is written, so no standing opt-out is cleared/.test(m)
                                    && /dry_run off would permanently clear/.test(m)),
    'a floor-0 dry run names the disabled guards and what a real one would do');
  assert.ok(!dry.logs.warning.some(m => /permanently clearing/.test(m)),
    'a dry run must not report an opt-out as already cleared');
});

test('a malformed floor falls back to 7 days rather than disabling it', async () => {
  const inside = await run({
    prs: [pull(1)],
    events: { 1: [requestedEvent('alice', daysAgo(6))] },
    nudgeAfterDays: 'soon',
  });
  const outside = await run({
    prs: [pull(1)],
    events: { 1: [requestedEvent('alice', daysAgo(8))] },
    nudgeAfterDays: 'soon',
  });

  assert.equal(inside.requestCalls.length, 0, 'the floor must still be in force');
  assert.equal(outside.requestCalls.length, 1);
  assert.equal(inside.counts['Nudge floor (days)'], 7);
  assert.match(inside.logs.warning[0], /NUDGE_AFTER_DAYS="soon"/);
});

test('a blank floor is unset, not "floor disabled"', async () => {
  // The workflow's `|| 7` only catches the empty string, so a dispatch field
  // holding a stray space arrives here verbatim - and Number(' ') is 0.
  const r = await run({
    prs: [pull(1)],
    events: { 1: [requestedEvent('alice', daysAgo(6))] },
    nudgeAfterDays: ' ',
  });

  assert.equal(r.requestCalls.length, 0);
  assert.equal(r.eventsCalls.length, 1, 'a blank value must not skip the event walk');
  assert.equal(r.counts['Nudge floor (days)'], 7);
  assert.deepEqual(r.logs.warning, [],
    'a floor still in force must not carry the floor-disabled annotation');
});

test('a huge floor is refused rather than making everyone permanently undue', async () => {
  // '1e308' used to overflow the day count to Infinity: nobody was ever due,
  // the job was a green no-op, and it still paid for every event walk.
  const r = await run({
    prs: [pull(1)],
    events: { 1: [requestedEvent('alice', daysAgo(30))] },
    nudgeAfterDays: '1e308',
  });

  assert.deepEqual(r.requestCalls.map(c => c.reviewers), [['alice']]);
  assert.equal(r.counts['Nudge floor (days)'], 7);
});

test('the event walk is fetched for the PR under consideration', async () => {
  const r = await run({
    prs: [pull(7), pull(9)],
    events: {
      7: [requestedEvent('alice', daysAgo(1))],
      9: [requestedEvent('alice', daysAgo(30))],
    },
    nudgeAfterDays: 7,
  });

  assert.deepEqual(r.eventsCalls.map(c => c.issue_number), [7, 9],
    "each PR's floor must come from its own events");
  assert.deepEqual(r.requestCalls.map(c => c.pull_number), [9]);

  for (const call of r.eventsCalls) {
    assert.equal(call.owner, 'citrusleaf');
    assert.equal(call.repo, 'aerospike-server');
    assert.equal(call.per_page, 100);
  }
});

test('the event walk is fetched only for PRs that have a stale reviewer', async () => {
  const r = await run({
    prs: [pull(1), pull(2, { draft: true }), pull(3)],
    reviews: { 3: [review('alice', 'APPROVED')] },
    nudgeAfterDays: 7,
  });

  assert.deepEqual(r.eventsCalls.map(c => c.issue_number), [1],
    'draft and fully-reviewed PRs must not pay for an event walk');
});

test('a malformed timestamp is not treated as contact', async () => {
  const r = await run({
    prs: [pull(1)],
    events: { 1: [requestedEvent('alice', 'not-a-date')] },
    nudgeAfterDays: 7,
  });

  assert.deepEqual(r.requestCalls.map(c => c.reviewers), [['alice']]);
});

// --- the opt-out ------------------------------------------------------------

test('a reviewer who removed their own review request is not re-requested', async () => {
  // The one gesture GitHub gives a reviewer for saying "not me". Re-issuing the
  // request reverses it, and a week later reverses it again - a loop whose only
  // exits are un-assigning yourself or inventing a verdict.
  const r = await run({
    prs: [pull(1), pull(2)],
    events: {
      1: [requestedEvent('alice', daysAgo(30)), removedEvent('alice', daysAgo(1))],
      2: [requestedEvent('alice', daysAgo(30)), removedEvent('alice', daysAgo(1))],
    },
    nudgeAfterDays: 7,
  });

  assert.equal(r.requestCalls.length, 0);
  assert.equal(r.counts['Reviewers who removed their own request'], 2,
    'one per PR, not the last one');
  assert.equal(r.counts['Reviewers held back by the floor'], 0, 'declining is not a floor hit');
  assert.match(r.logs.info[0], /not nudging alice - they removed their own review request/);
  assert.match(r.logs.info[1], /all 1 stale reviewer\(s\) removed their own review request/,
    'quantified over the set the branch examined - alice may not be the'
    + ' only assigned reviewer');
});

test('a removal by someone else is not an opt-out', async () => {
  // review_request_removed is also emitted when a maintainer tidies the queue,
  // and by this workflow itself on every nudge of a pending reviewer - which
  // without the actor comparison would make the script's own DELETE an opt-out
  // from its own POST.
  const r = await run({
    prs: [pull(1)],
    events: {
      1: [requestedEvent('alice', daysAgo(30)), removedEvent('alice', daysAgo(1), 'maintainer')],
    },
    nudgeAfterDays: 7,
  });

  assert.deepEqual(r.requestCalls.map(c => c.reviewers), [['alice']]);
  assert.equal(r.counts['Reviewers who removed their own request'], 0);
});

test('a review request after a self-removal overrides it', async () => {
  const r = await run({
    prs: [pull(1)],
    events: { 1: [removedEvent('alice', daysAgo(30)), requestedEvent('alice', daysAgo(20))] },
    nudgeAfterDays: 7,
  });

  assert.deepEqual(r.requestCalls.map(c => c.reviewers), [['alice']],
    'someone asked again after she declined, which is theirs to do');
});

// The three tests below pin the request walk against being narrowed by actor.
// They are a set, not a list: each fixture kills only the narrowings its own
// actor field falls on the wrong side of, so no two of them are redundant and
// no one of them is sufficient. Named here because the coverage is a property
// of the set, which no single test can state.
//
//   no actor          kills a presence test  (actor?.login && ...)
//   actor === login   kills an inequality    (actor?.login !== reviewer)
//   no actor, in floor  kills the same, applied to `contacted` alone

test('a review request with no actor still overrides a self-removal', async () => {
  // GitHub does not guarantee a populated actor on review_requested, and the
  // code reads only requested_reviewer - any such event is someone asking
  // again. A presence test drops the actor-less ones, and a reviewer whose
  // re-request arrived without one then stays opted out with nothing able to
  // clear it.
  const r = await run({
    prs: [pull(1)],
    events: {
      1: [removedEvent('alice', daysAgo(30)), requestedEvent('alice', daysAgo(20), null)],
    },
    nudgeAfterDays: 7,
  });

  assert.deepEqual(r.requestCalls.map(c => c.reviewers), [['alice']],
    'an actor-less request is still someone asking again');
});

test('a reviewer re-requesting their own review overrides their self-removal', async () => {
  // What GitHub refuses is a request naming the PR's author, not a request
  // naming its own sender - so a non-author collaborator asking for their own
  // review back is a real event, and it is someone asking again. An inequality
  // test on the actor drops exactly these, which strands the reviewer who
  // declined and then changed her mind: the one person who cannot un-decline
  // is the one whose decline it was.
  const r = await run({
    prs: [pull(1)],
    events: {
      1: [removedEvent('alice', daysAgo(30)), requestedEvent('alice', daysAgo(20), 'alice')],
    },
    nudgeAfterDays: 7,
  });

  assert.deepEqual(r.requestCalls.map(c => c.reviewers), [['alice']],
    'asking for your own review back is still someone asking again');
});

test('an actor-less review request is contact for the floor too', async () => {
  // The walk feeds two maps from one event, and a narrowing can be applied to
  // either. Pinning only the opt-out override leaves the floor half free: drop
  // the actor-less events from `contacted` alone and this reviewer, asked
  // yesterday, is nudged again today.
  const r = await run({
    prs: [pull(1)],
    events: { 1: [requestedEvent('alice', daysAgo(1), null)] },
    nudgeAfterDays: 7,
  });

  assert.equal(r.requestCalls.length, 0, 'asked yesterday, actor or no actor');
  assert.equal(r.counts['Reviewers held back by the floor'], 1);
});

test('a removal nobody signed is not an opt-out', async () => {
  // The removal walk is the one that does read the actor, so it has to survive
  // the actor not being there - and a team removal carries no
  // requested_reviewer either. None of the three is a reviewer declining, so
  // alice is still nudged. The first two are refused by the actor presence
  // guard and by the equality test respectively; the third is the one only the
  // guard can refuse - with neither side present, an equality test that
  // optional-chains both sides is true, and the dereference below it then
  // throws and costs the PR.
  const r = await run({
    prs: [pull(1)],
    events: {
      1: [
        removedEvent('alice', daysAgo(1), null),
        { event: 'review_request_removed', created_at: daysAgo(1),
          actor: { login: 'maintainer' }, requested_team: { slug: 'core' } },
        { event: 'review_request_removed', created_at: daysAgo(1),
          requested_team: { slug: 'core' } },
      ],
    },
    nudgeAfterDays: 7,
  });

  assert.deepEqual(r.requestCalls.map(c => c.reviewers), [['alice']],
    'a removal that names no one who made it is not hers');
  assert.equal(r.counts['Reviewers who removed their own request'], 0);
});

test("a reviewer's own comment after a self-removal does not override it", async () => {
  // alice was asked, declined by removing her own request, then left a
  // COMMENTED review ("not me - ask bob"). The review is contact for the
  // floor's purposes, but nobody asked again, so the opt-out must stand.
  // Weighing the removal against `contacted` instead of `requested` POSTs to
  // her here - and that POST writes a review_requested event that keeps her
  // opt-out revoked on every later run. One comment, re-requested for good.
  const r = await run({
    prs: [pull(1)],
    reviews: { 1: [review('alice', 'COMMENTED', HEAD, daysAgo(9))] },
    events: { 1: [requestedEvent('alice', daysAgo(20)), removedEvent('alice', daysAgo(10))] },
    nudgeAfterDays: 7,
  });

  assert.equal(r.requestCalls.length, 0, 'a comment is not someone asking again');
  assert.equal(r.counts['Reviewers who removed their own request'], 1);
});

test('review_request_removed is not contact, only a possible opt-out', async () => {
  // It carries requested_reviewer just like review_requested does, so a filter
  // that keyed on the field rather than the event type would read this run's
  // own DELETEs back as contact - the definition of the floor drifting in the
  // direction of the script's own writes.
  const r = await run({
    prs: [pull(1)],
    events: { 1: [removedEvent('alice', daysAgo(1), 'maintainer')] },
    nudgeAfterDays: 7,
  });

  assert.deepEqual(r.requestCalls.map(c => c.reviewers), [['alice']]);
  assert.equal(r.counts['Reviewers held back by the floor'], 0);
});

// --- reporting --------------------------------------------------------------

test('a run in which every nudge fails is not green', async () => {
  const r = await run({
    prs: [pull(1), pull(2)],
    onRequest: () => { throw httpError(422, 'not a collaborator'); },
  });

  assert.equal(r.counts['Reviewers nudged'], 0);
  assert.equal(r.counts['Reviewers not requestable'], 2);
  assert.equal(r.logs.failed.length, 1);
  assert.match(r.logs.failed[0], /2 reviewer\(s\) not nudged/);
});

test('a clean run sets no exit code', async () => {
  const r = await run({ prs: [pull(1)] });

  assert.equal(r.logs.failed.length, 0);
  assert.equal(r.logs.warning.length, 0);
  assert.equal(r.logs.error.length, 0);
});

test('the summary states the floor in force and keys its rows on nothing else', async () => {
  // The row *keys* are the table's schema: interpolating the floor into one
  // meant two runs with different floors could not be diffed, and the one
  // number needed to interpret a run appeared nowhere.
  const seven = await run({ prs: [pull(1)] });
  const three = await run({ prs: [pull(1)], nudgeAfterDays: 3 });

  assert.deepEqual(Object.keys(seven.counts), Object.keys(three.counts));
  assert.equal(seven.counts['Nudge floor (days)'], 7);
  assert.equal(three.counts['Nudge floor (days)'], 3);
});

test('the summary is written exactly once', async () => {
  const r = await run({ prs: [pull(1), pull(2, { draft: true })] });

  assert.equal(r.summaries.length, 1);
});

test('the summary is published even when the loop throws', async () => {
  // The per-PR handler is the last thing standing between a defect and the
  // loop; a throw from inside it escapes. Stood up here by failing a read and
  // then blowing up the core.warning that reports it - the summary must still
  // publish what happened before the abort, and the throw must still propagate.
  const boom = new Error('summary must still publish');

  const summaries = [];
  const summary = {
    addHeading() { return this; },
    addTable(rows) { this._rows = rows; return this; },
    async write() { summaries.push(this._rows); return this; },
  };

  const pulls = { list: function list() {}, listReviews: function listReviews() {} };
  const github = {
    rest: strictNamespace('rest', { pulls: strictNamespace('pulls', pulls) }),
    async paginate(fn) {
      if (fn === pulls.list) { return [pull(1), pull(2)]; }
      throw httpError(502, 'Bad Gateway');
    },
  };
  const core = {
    info() {},
    warning() { throw boom; },
    error() {},
    setFailed() {},
    summary,
  };

  await assert.rejects(
    () => nudge({ github, context: { repo: { owner: 'o', repo: 'r' } }, core }),
    (err) => err === boom,
    'the throw must propagate so the step goes red');

  assert.equal(summaries.length, 1, 'the summary must still have been written');
  assert.deepEqual(summaries[0][1], ['PRs scanned', '2'], 'and must report what it saw');
});

test('a failing summary write does not replace the error that killed the run', async () => {
  // A throw out of a finally *replaces* the exception in flight rather than
  // chaining it, so an unwrapped write() would cost both artefacts at once: no
  // summary, and a message about $GITHUB_STEP_SUMMARY in place of the diagnosis.
  const boom = new Error('the real failure');
  const writeErr = new Error('Unable to find environment variable for $GITHUB_STEP_SUMMARY');

  const logs = { info: [], warning: [] };
  const summary = {
    addHeading() { return this; },
    addTable() { return this; },
    async write() { throw writeErr; },
  };

  const pulls = { list: function list() {}, listReviews: function listReviews() {} };
  const github = {
    rest: strictNamespace('rest', { pulls: strictNamespace('pulls', pulls) }),
    async paginate(fn) {
      if (fn === pulls.list) { return [pull(1)]; }
      throw httpError(502, 'Bad Gateway');
    },
  };
  const core = {
    info: (m) => logs.info.push(m),
    warning: (m) => { if (logs.warning.push(m) === 1) { throw boom; } },
    error() {},
    setFailed() {},
    summary,
  };

  await assert.rejects(
    () => nudge({ github, context: { repo: { owner: 'o', repo: 'r' } }, core }),
    (err) => err === boom,
    'the in-flight exception must survive the cleanup');

  assert.match(logs.warning.at(-1), /could not publish the job summary/);
  assert.match(logs.info.at(-1), /counters:.*"abandoned":1/,
    'the counters must survive the table that would have carried them');
});

// --- dry run ----------------------------------------------------------------

test('a dry run touches nothing, over a fixture where every PR is nudgeable', async () => {
  const r = await run({
    dryRun: true,
    prs: [
      pull(1),
      pull(2, { requested_reviewers: [{ login: 'alice' }] }),
      pull(3, { assignees: [{ login: 'alice' }, { login: 'dave' }] }),
    ],
  });

  assert.equal(r.removeCalls.length, 0, 'a dry run must issue no DELETE');
  assert.equal(r.requestCalls.length, 0, 'a dry run must issue no POST');
  assert.equal(r.counts['Reviewers that would be nudged'], 4);
  assert.equal(r.summaries[0].heading, 'PR review nudge (dry run)');

  // Naming them is the only reason to run a dry run before turning the
  // schedule on, so a dry run that collapsed to the number 4 is not green.
  assert.ok(r.logs.info.includes('PR #3: dry run - would nudge alice, dave'),
    'a dry run has to name who a real run would have nudged');

  // The dry run is the path a human exercises first, and the strict proxy only
  // fails a test if some assertion notices the throw it raises. The script
  // swallows in-loop throws by design, so without these a forbidden call on
  // this path would be caught, counted as an abandoned PR, and asserted by
  // nobody.
  assert.equal(r.counts['PRs abandoned'], 0);
  assert.deepEqual(r.logs.warning, []);
  assert.deepEqual(r.logs.error, []);
  assert.equal(r.logs.failed.length, 0);
});

test('DRY_RUN absent mutates - a schedule run has no inputs context', async () => {
  const r = await run({ dryRun: null, prs: [pull(1)] });

  assert.deepEqual(r.requestCalls.map(c => c.reviewers), [['alice']]);
  assert.equal(r.summaries[0].heading, 'PR review nudge');
  assert.ok(r.logs.info.includes('PR #1: nudged alice'), 'the run states who it nudged');
});

// --- what it must never do --------------------------------------------------

test('the script never posts a comment or a review', async () => {
  // Asserted against the surface the script actually reached, over a fixture
  // exercising every branch that could reach for one: a nudge, a strip and
  // re-request, a floor walk, an opt-out, a failure and a dry run.
  const reached = new Set();

  for (const dryRun of [false, true]) {
    const r = await run({
      dryRun,
      prs: [
        pull(1),
        pull(2, { requested_reviewers: [{ login: 'alice' }] }),
        pull(3, { assignees: [{ login: 'alice' }, { login: 'dave' }] }),
        pull(4),
      ],
      reviews: { 4: [review('alice', 'COMMENTED', HEAD, daysAgo(1))] },
      events: { 3: [removedEvent('dave', daysAgo(1))] },
      nudgeAfterDays: 7,
      onRequest: (params) => {
        if (params.pull_number === 3) { throw httpError(422, 'not a collaborator'); }
      },
    });

    for (const prop of r.touched) { reached.add(prop); }
  }

  assert.deepEqual([...reached].sort(), [
    'core.info',
    'core.setFailed',
    'core.summary',
    'core.warning',
    'github.paginate',
    'github.rest',
    'issues.listEvents',
    'pulls.list',
    'pulls.listReviews',
    'pulls.removeRequestedReviewers',
    'pulls.requestReviewers',
    'rest.issues',
    'rest.pulls',
  ], 'the whole API surface this script may touch');

  // And the proxy really does refuse the rest, so the list above is a
  // restriction rather than a description.
  const rest = strictNamespace('rest', {
    pulls: strictNamespace('pulls', { list() {} }),
    issues: strictNamespace('issues', { listEvents() {} }),
  });

  assert.doesNotThrow(() => rest.issues.listEvents);
  assert.throws(() => rest.issues.createComment, /unexpected API call: issues.createComment/);
  assert.throws(() => rest.pulls.createReview, /unexpected API call: pulls.createReview/);
  assert.throws(() => rest.repos, /unexpected API call: rest.repos/);
});
