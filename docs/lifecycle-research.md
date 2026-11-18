# Per-object lifecycle research

Issue #245 asks one question. Can any supported object store delete a
paste at its own `expires-at` time without a dayfly sweeper? This report
answers that question. It also compares the four options from the issue
and estimates the cost of option 2.

Status: research only. This report changes no code and no behavior.

Checked: 2026-11-18. Dayfly main: `3a7397e`. Garage version: 2.5.0.

## Short answer

No store deletes one object at an arbitrary timestamp. All four stores
use whole days from creation. All four run their lifecycle worker once
per day at most. All four act asynchronously. The sweeper stays the only
way to delete a paste at its exact `expires-at` time.

## Verified provider table

| Provider | Lifecycle filter | Expiration basis | Per-object `expires-at`? | Notes |
| --- | --- | --- | --- | --- |
| AWS S3 | prefix, tags, size | age in days, or fixed date | No | Tag filters work. A rule expires after N days from creation, not at an arbitrary timestamp. Minimum 1 day. Actions run asynchronously with no completion guarantee. Limit: 1,000 rules per bucket. |
| Backblaze B2 (native API) | file-name prefix only | days from upload to hide, days from hide to delete | No | No tag filter. No file-info filter. Minimum 1 day for each step. Rules run in background processes. |
| Backblaze B2 (S3-compatible API) | prefix only, as documented | age in days | No | `PutBucketLifecycleConfiguration` works. Expiration hides the current version. A second rule deletes old versions. Tag filter support is not documented. |
| DigitalOcean Spaces | prefix only | age in days, or date; abort incomplete multipart upload | No | Tag-based rules are not supported. Spaces supports no object tagging and no bucket tagging at all. Rules run once per day. |
| Garage 2.5.0 | prefix, object size | age in days, or date; abort incomplete multipart upload | No | No tag filters. No `ExpiredObjectDeleteMarker`. The lifecycle worker runs daily at midnight UTC by default. Lifecycle API calls need an owner key in 2.5.0. |

## AWS S3

The AWS User Guide and API reference confirm the issue table. A filter
uses a prefix, one or more tags, an object size, or an `And` combination
of those parts. Expiration uses `Days`, a non-zero positive integer, or
a fixed `Date` at midnight UTC. Tag-based filters are evaluated daily.
The action is queued for asynchronous processing.

Two facts extend the issue table. A bucket holds at most 1,000 rules.
AWS gives no completion guarantee: the guide warns that an object can
remain days or even weeks after its expiration date passes. AWS also
states that a fixed `Date` applies to the whole rule, not to one object.

Sources: AWS S3 User Guide, "Expiring objects"; AWS S3 API Reference,
`PutBucketLifecycleConfiguration`. Checked 2026-11-18.

## Backblaze B2

The native API confirms the issue table. A rule has three parts:
`fileNamePrefix`, `daysFromUploadingToHiding`, and
`daysFromHidingToDeleting`. Each day value is null or 1 or more. The
value 0 is not allowed. A rule hides a file first and deletes it later.
No tag filter exists. No file-info filter exists.

One fact extends the issue table. B2 now also accepts S3-compatible
lifecycle rules through `PutBucketLifecycleConfiguration`. The Backblaze
blog describes the mapping. Prefix filters map to file name prefixes.
An expiration hides the current version. A noncurrent-version
expiration deletes old versions. An `ExpiredObjectDeleteMarker` rule is
needed to remove abandoned hide markers. The blog describes prefix
filters only. It does not state whether a tag filter is accepted or
rejected. That question stays open until a bucket test answers it.

Rules are evaluated by background processes. They are not evaluated in
real time. Execution is eventually consistent. Large buckets can need
more than one pass.

Sources: Backblaze B2 lifecycle rules documentation; Backblaze blog,
"A Deeper Look at S3 Compatible Lifecycle Rules in Backblaze B2".
Checked 2026-11-18.

## DigitalOcean Spaces

The DigitalOcean S3 compatibility page confirms the issue table. Its
row for bucket lifecycle reads: "Supported for time-based expiration
and removing incomplete multipart uploads. Tag-based lifecycle rules
are not supported." The page was last updated 2026-05-13.

One fact extends the issue table. Spaces supports no object tagging and
no bucket tagging at all. A tag scheme cannot work on Spaces even if
the rule form were accepted. Lifecycle rules run once per day, with no
stated time of day. That schedule comes from a secondary source, not
from the DigitalOcean pages checked here.

Source: DigitalOcean documentation, "Spaces S3 Compatibility".
Checked 2026-11-18.

## Garage 2.5.0

The issue names Garage 2.5. The current release is 2.5.0, published
2026-10-24. The version in the issue is current.

The official S3 compatibility page confirms the issue table. Garage
implements `PutBucketLifecycleConfiguration` in part. The only actions
are `AbortIncompleteMultipartUpload` and `Expiration`, without the
`ExpiredObjectDeleteMarker` field. The deprecated rule-level `Prefix`
is rejected. A prefix must sit inside the `Filter` structure.

Two facts extend the issue table. Filters accept a prefix and object
size bounds. Tag filters are not accepted. Garage has no tagging
endpoints at all, so a tag filter could not match any object. The
lifecycle worker runs daily at midnight UTC by default. That schedule
comes from operator documentation derived from the Garage API, not
from the compatibility page itself.

A third fact is new in 2.5.0. Access to the lifecycle API calls is now
restricted to keys with owner permission. The dayfly README already
requires an owner key for lifecycle work, so current practice already
matches this change.

Sources: Garage S3 compatibility page (documentation source in the
Garage repository); Garage 2.5.0 release notes as packaged by
openSUSE. Checked 2026-11-18.

## Corrections to the issue table

The issue table is correct in its main claims. Four additions apply.

1. B2 gains an S3-compatible lifecycle path with prefix filters. The
   filter claim stays prefix-only as documented. Tag filter support is
   undocumented and unverified.
2. Garage gains two limits: no `ExpiredObjectDeleteMarker`, and the
   lifecycle API needs an owner key in 2.5.0.
3. S3 gains two limits: at most 1,000 rules per bucket, and no
   completion guarantee. AWS warns that deletion can lag days or weeks.
4. Spaces gains one limit: no tagging at all, so no tag scheme can
   work there.

One claim needs a sharper form. The minimum granularity is 1 day on
all four stores. No store honors a sub-day TTL. A paste with a
90-minute TTL cannot die by lifecycle before its class age passes.

## How dayfly stores expiry today

This section grounds the option 2 estimate in the code at main
`3a7397e`. All paths are in `src/store.rs` unless stated otherwise.

- A paste key is `pastes/<date>/<random>`, where the date is the
  creation date in UTC. `key_for` builds the key. Appended segments
  live under the base key.
- An alias object is `words/<phrase>`. Its body holds the target key.
  It carries its own `expires-at` value.
- A TTL is exact to the second. Sub-day TTLs are allowed. The maximum
  is 3650 days. The default is 7 days. `DAYFLY_MIN_TTL_DAYS` and
  `DAYFLY_MAX_TTL_DAYS` bound an explicit TTL.
- Read-time expiry enforces the exact expiry on every read. The
  lifecycle rule is a backstop only. The README states this directly.
- `set_ttl` rewrites the base object, every segment, and the alias
  with `CopyObject`. A paste above the 5 GiB copy limit cannot change
  its TTL.
- Cleanup lists the `pastes/` prefix and the `words/` prefix. It reads
  `expires-at` with one `HeadObject` call per key. It deletes each
  expired paste through `delete`, which removes the base object, its
  segments, and its alias.
- Listing walks one `pastes/<date>/` prefix per day, newest first.
  A bare-id lookup scans the `pastes/` prefix for a suffix match.
- `src/lifecycle.rs` applies a bucket lifecycle document. It supports
  an ID, a status, one prefix filter, a whole-day expiration, and an
  abort age. The example in `docs/lifecycle-example.json` uses a
  3651-day backstop, one day past the maximum TTL.

## Option comparison

| Option | Exact `expires-at`? | Works on all four stores? | Key change? | Sweeper removed? |
| --- | --- | --- | --- | --- |
| 1. Keep the sweeper | Yes | Yes | No | No |
| 2. TTL-class prefixes | No (day granularity) | Yes | Yes | Only in part (see below) |
| 3. Backend-specific tag rules (S3 only) | No (day granularity) | No | No | No |
| 4. A backend with object-level expiry | — | No such backend found | — | — |

### Option 1: keep the sweeper

The sweeper deletes at the exact `expires-at` time, including sub-day
TTLs. It works on every backend with one code path. Its cost is an
O(n) scan and a scheduler. Issue #246 adds an in-process loop as an
alternative scheduler, so the CronJob is no longer the only scheduler.

### Option 2: TTL-class prefixes

The store would write `pastes/<class>/<date>/<id>`, for example
`pastes/1d/`, `pastes/7d/`, and `pastes/30d/`, with one lifecycle rule
per class. The backend would delete by age from creation. The estimate
below lists the cost.

### Option 3: backend-specific tag rules

Dayfly writes no tags today. Garage and Spaces have no tagging
endpoints. B2 native rules have no tag filter. `src/lifecycle.rs`
supports prefix filters only. A tag scheme would need tagging on
upload for S3 only, a wider lifecycle document, and still a fixed
class per tag. It cannot express an arbitrary `expires-at` value. It
is not portable.

### Option 4: a backend with object-level expiry

No reviewed store exposes per-object absolute expiry through S3
lifecycle. Two nearby features do not qualify. The S3
`x-amz-expiration` header reports scheduled expiry. It does not set
per-object expiry. S3 Object Lock retention locks an object. It does
not delete it, and it needs versioning, which Garage and Spaces do
not support.

## Option 2: migration-cost estimate

### Key shape

The key becomes `pastes/<class>/<date>/<id>`. This change touches
`key_for`, `validate_id`, `is_paste_key`, `resolve_key`, and every
caller that builds or parses a key. Segment keys follow the base key,
so they need no separate rule. Listing cursors embed a date and a key,
so the cursor format must record the class or change.

Public IDs change, or lookup pays per class. The public ID today is
`<date>/<random>` or a bare `<random>`. Two designs exist. Design A
puts the class in the public ID, for example `7d/2026-11-18/abc...`.
Every existing URL and stored ID breaks. Design B keeps the public ID
and scans one prefix per class on resolve. Each bare-id lookup then
costs one prefix scan per class instead of one scan in total.

### Alias objects

An alias key is `words/<phrase>`. It carries no class. Three designs
exist.

Design A keeps `words/` flat and sets one `words/` rule at the longest
class age. Alias objects then linger until that rule fires, up to
3651 days in the current backstop. A stale alias still points at a
deleted paste. Reads stay correct because resolve treats the target
as lost, but dead alias objects accumulate in storage and in listing.

Design B puts the class in the alias key, for example
`words/7d/<phrase>`. Alias lookup then needs one attempt per class,
because the phrase alone does not name the class. Alias claim must
check all classes for a phrase collision. A TTL change that moves the
class must release the old alias and claim the new one, with a race
window in between. Garage ignores conditional writes, so the code
already verifies an alias after claim.

Design C keeps the sweeper for alias objects only. Option 2 then does
not remove the sweeper at all. It only moves paste deletion to the
backend.

### TTL changes

Today `set_ttl` rewrites the base object, every segment, and the alias
in place with `CopyObject`. Under option 2, a TTL change that crosses
a class bound must copy every object to a new key. It must also rebind
the alias and delete the old objects. A paste above the 5 GiB copy limit cannot
move at all, so its TTL cannot cross a class bound.

The main hazard is a missed move. A lifecycle deadline derives from
creation time, not from `expires-at`. If code extends a TTL past the
class bound without moving the object, the backend deletes a live
paste at the old class deadline. The `expires-at` value still says
the paste is valid. The data is gone anyway. Every TTL extension path
must move the object, and a missed path deletes live data.

### Sub-day and custom TTLs

No class exists below 1 day. A 90-minute paste would sit in class
`1d`. The backend would delete it up to about 2 days after creation:
1 day of age, rounded up to midnight UTC, plus worker lag. Read-time
expiry still hides it at 90 minutes, so reads stay correct. Storage
lingers. A 3-day TTL fits no `1d`/`7d`/`30d` set unless it rounds up
to 7 days. More classes reduce the waste and add rules, alias lookup
attempts, and resolve scans.

The retention curve only shortens expiry. A shortened paste still
deletes at its class age on the backend. Reads enforce the shorter
expiry. Storage lingers. This is safe but wasteful.

### Migration of existing objects

S3 has no rename. Each existing paste needs one `CopyObject` call per
object (base plus segments), an alias rebind, and a delete of the old
objects. A paste created during the migration needs a dual-read path
or a maintenance window. The alias rebind has a race. The old alias
must be released before the new one is claimed. A second process can
claim the phrase in between. The migration must also rewrite list
cursors and any stored IDs that embed the old key shape. The lifecycle
document and the README lifecycle section must change to per-class
rules. Rule count stays far below the S3 limit of 1,000 rules.

### What option 2 does not remove

Option 2 does not remove read-time expiry. Reads must still enforce
the exact expiry for objects the backend has not yet deleted. It does
not remove the lifecycle backstop. It removes the paste-deletion scan
only, and only under alias design B or C. Under design A it leaves
stale aliases. Under design C it leaves the sweeper in place for
aliases.

### Size of the change

The change touches key construction and validation, ID resolution,
listing and cursors, alias claim and resolve and verify, `set_ttl`,
cleanup, the lifecycle document, the README, a migration tool, and
tests for each of those parts. Its size is comparable to the provider
series in issues #121 to #124, which landed as four pull requests of
30 to 62 files each. Option 2 is one coherent change of that order,
not a small patch.

## Recommendation

Keep the sweeper (option 1). Do not add a TTL-class mode. Do not add
backend-specific tag rules.

The reasons are:

- The sweeper deletes at the exact `expires-at` time, including
  sub-day TTLs. No lifecycle rule can do that on any reviewed store.
- TTL changes work without cross-key copies. Option 2 turns every
  class-crossing TTL extension into a copy of every object, and a
  missed move deletes a live paste.
- One code path serves all four stores. Option 2 needs per-class
  rules on each store and per-store behavior notes. Option 3 needs
  S3-only tagging.
- The lifecycle rule already serves as the backstop it was built to
  be. The 3651-day rules in `docs/lifecycle-example.json` cover the
  case where the sweeper misses an object.

Three facts would change this recommendation.

1. Bucket tests show lifecycle deletion latency is acceptable for the
   deployment, and a product decision restricts TTLs to fixed
   whole-day classes. That decision must also drop TTL extension
   across classes, or accept the copy cost on every extension.
2. The bucket grows until the O(n) scan dominates. The project notes
   already name that slowdown as the trigger for a database. A
   database would replace the scan without the key-shape change.
3. A supported backend gains true per-object expiry. Re-check option
   4 if that happens.

## Tests not run

No bucket test ran for this report. This VM holds no S3, B2, or
Spaces credentials. Lifecycle observation needs a window of many
days. Each test below states what it needs.

### S3 tag-filter rule

Needs: an AWS account, a disposable bucket, and permission to set a
lifecycle configuration. Upload two objects with a test tag and one
without it. Apply a rule with a tag filter and `Days` 1. Poll
`HeadObject` for up to 48 hours or more. S3 gives no completion
guarantee, so allow more time. Record the time from rule creation to
removal. Check that the untagged object stays. Measure deletion
latency and confirm the 1-day minimum.

### B2 native prefix rules

Needs: a Backblaze B2 account and a disposable bucket. Set a native
rule on a test prefix with `daysFromUploadingToHiding` 1 and
`daysFromHidingToDeleting` 1. Upload test objects. Poll
`b2_list_file_versions` for the hide marker and then for deletion.
Record hide latency and delete latency. Confirm that 1 is the
minimum for each step.

### B2 S3-compatible rules

Needs: the same B2 bucket. Apply `PutBucketLifecycleConfiguration`
with a prefix filter and `Days` 1. Apply a second configuration with
a tag filter and record whether the service accepts or rejects it.
That answer is undocumented today. Measure hide and delete latency
the same way as the native test.

### Spaces prefix rules

Needs: a DigitalOcean Spaces account and a disposable Space. Apply a
time-based expiration rule with `Days` 1 on a test prefix. Poll for
deletion over 48 hours or more. Record deletion latency. Apply a
tag-based rule and record the rejection. Confirm the 1-day minimum.

### Garage prefix rules

Needs: a Garage deployment, the homelab cluster or a local
single-node Garage. Apply a lifecycle configuration with a prefix
filter and `Days` 1. Measure deletion latency across the daily worker
run. Apply a configuration with `ExpiredObjectDeleteMarker` and
record the rejection. Apply a configuration with a tag filter and
record the rejection. Confirm that lifecycle API calls need an owner
key on 2.5.0.

## Sources

Checked 2026-11-18. Each source is the vendor documentation or the
vendor blog unless marked otherwise.

- AWS S3 User Guide, "Expiring objects":
  https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-expire-general-considerations.html
- AWS S3 API Reference, `PutBucketLifecycleConfiguration`:
  https://awscli.amazonaws.com/v2/documentation/api/2.4.18/reference/s3api/put-bucket-lifecycle-configuration.html
- AWS S3 API Reference, lifecycle rule elements:
  https://docs.aws.amazon.com/boto3/latest/reference/services/s3/bucketlifecycleconfiguration/rules.html
- Backblaze B2 lifecycle rules (native API), Backblaze blog,
  "Introducing Backblaze B2 Cloud Storage Lifecycle Rules":
  https://www.backblaze.com/blog/backblaze-b2-lifecycle-rules/
- Backblaze blog, "A Deeper Look at S3 Compatible Lifecycle Rules in
  Backblaze B2":
  https://www.backblaze.com/blog/a-deeper-look-at-s3-compatible-lifecycle-rules-in-backblaze-b2/
- DigitalOcean documentation, "Spaces S3 Compatibility":
  https://docs.digitalocean.com/products/spaces/reference/s3-compatibility/index.html.md
- Garage S3 compatibility page (documentation source in the Garage
  repository):
  https://github.com/deuxfleurs-org/garage/blob/HEAD/doc/book/reference-manual/s3-compatibility.md
- Garage 2.5.0 release notes, as packaged by openSUSE:
  https://fr.rpmfind.net/linux/RPM/opensuse/tumbleweed/x86_64/garage-2.5.0-1.1.x86_64.html
- Garage current version, cross-checked against Arch Linux package
  data: https://archlinux.org/packages/extra/x86_64/garage/

Secondary sources used for worker schedules only: the Garage operator
CRD documentation for the daily midnight UTC worker run, and a
DigitalOcean Spaces guide for the once-per-day rule run. The vendor
pages checked here do not state those schedules.
