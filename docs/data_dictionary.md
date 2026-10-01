\# Data Dictionary



\## Bronze



\### `workspace.oss\_ecosystem.bronze\_github\_events`



Grain: one unique GitHub event.



| Column | Type | Description |

|---|---|---|

| `event\_id` | STRING | GitHub event identifier and Bronze deduplication key |

| `event\_type` | STRING | GitHub event type |

| `created\_at` | STRING | Original source event timestamp |

| `actor\_json` | STRING | Raw nested actor object serialized as JSON |

| `repo\_json` | STRING | Raw nested repository object serialized as JSON |

| `org\_json` | STRING | Raw nested organization object serialized as JSON |

| `payload\_json` | STRING | Event-specific payload serialized as JSON |

| `public` | BOOLEAN | GitHub public-event flag |

| `raw\_json` | STRING | Complete original source record |

| `source\_file` | STRING | GH Archive hourly filename |

| `source\_hour` | TIMESTAMP | Hour represented by the source file |

| `ingested\_at` | TIMESTAMP | Bronze ingestion timestamp |



\## Ingestion Control



\### `workspace.oss\_ecosystem.ingestion\_file\_log`



Grain: one GH Archive source file.



| Column | Type | Description |

|---|---|---|

| `source\_file` | STRING | Source filename |

| `source\_hour` | TIMESTAMP | Hour represented by the file |

| `file\_status` | STRING | Current ingestion state |

| `discovered\_at` | TIMESTAMP | Time the file was registered |

| `ingestion\_started\_at` | TIMESTAMP | Start of processing |

| `ingestion\_completed\_at` | TIMESTAMP | Successful completion time |

| `row\_count` | BIGINT | Number of source records associated with the ingestion |

| `error\_message` | STRING | Error details when ingestion fails |



\## Silver Core



\### `workspace.oss\_ecosystem.silver\_events\_core`



Grain: one unique GitHub event.



| Column | Type | Description |

|---|---|---|

| `event\_id` | STRING | Unique event identifier |

| `event\_type` | STRING | Normalized GitHub event type |

| `event\_ts` | TIMESTAMP | Parsed event timestamp |

| `actor\_id` | BIGINT | Stable GitHub actor ID |

| `actor\_login` | STRING | Actor login observed on the event |

| `repo\_id` | BIGINT | Stable GitHub repository ID |

| `repo\_name` | STRING | Repository name observed on the event |

| `org\_id` | BIGINT | GitHub organization ID when available |

| `public` | BOOLEAN | Public-event flag |

| `source\_file` | STRING | Originating GH Archive file |

| `source\_hour` | TIMESTAMP | Originating source hour |

| `ingested\_at` | TIMESTAMP | Original Bronze ingestion timestamp |



\## Event-Specific Silver Tables



\### `silver\_push\_events`



Grain: one PushEvent.



Additional payload fields:



\- `push\_id`

\- `ref`

\- `before\_sha`

\- `head\_sha`



\### `silver\_pull\_request\_events`



Grain: one PullRequestEvent.



Additional payload fields:



\- `action`

\- `pr\_number`

\- `pr\_id`



\### `silver\_issue\_events`



Grain: one IssuesEvent.



Additional payload fields:



\- `action`

\- `issue\_id`

\- `issue\_number`

\- `state`



\### `silver\_issue\_comment\_events`



Grain: one IssueCommentEvent.



Additional payload fields:



\- `action`

\- `issue\_id`

\- `issue\_number`

\- `comment\_id`



\### `silver\_watch\_events`



Grain: one WatchEvent.



Additional payload field:



\- `action`



Watch events are used as the project-level approximation for star activity.



\### `silver\_fork\_events`



Grain: one ForkEvent.



Additional payload fields:



\- `forkee\_repo\_id`

\- `forkee\_repo\_name`



\### `silver\_release\_events`



Grain: one ReleaseEvent.



Additional payload fields:



\- `action`

\- `release\_id`

\- `tag\_name`

\- `is\_draft`

\- `is\_prerelease`



All event-specific Silver tables also retain:



\- `event\_id`

\- `event\_ts`

\- `actor\_id`

\- `repo\_id`

\- `source\_file`

\- `source\_hour`

\- `ingested\_at`



\## Actor Identity



\### `workspace.oss\_ecosystem.dim\_actor\_identity`



Grain: one stable GitHub actor ID.



Purpose:



\- consolidate multiple observed logins for the same actor ID

\- preserve the latest observed login

\- track first and last observation

\- record login variation

\- classify obvious bots



Bot classification rule:



An actor is considered a bot when at least one observed login ends in `\[bot]`.



All other actors are classified as `human\_or\_unknown`.



This rule intentionally favors precision over aggressive bot detection.



\## Gold Repository Daily Activity



\### `workspace.oss\_ecosystem.gold\_repo\_daily\_activity`



Grain:



`repo\_id + event\_date`



| Metric | Description |

|---|---|

| `repo\_id` | Stable GitHub repository identifier |

| `event\_date` | Calendar date derived from event timestamp |

| `repo\_name` | Most recently observed repository name for that repository-day |

| `total\_events` | Number of repo-attributable events |

| `active\_contributors` | Distinct actors active in the repository that day |

| `push\_events` | PushEvent count |

| `pull\_request\_events` | PullRequestEvent count |

| `issue\_events` | IssuesEvent count |

| `issue\_comment\_events` | IssueCommentEvent count |

| `watch\_events` | WatchEvent count |

| `fork\_events` | ForkEvent count |

| `release\_events` | ReleaseEvent count |

| `prs\_opened` | Pull requests with action `opened` |

| `prs\_closed` | Pull requests with action `closed` |

| `issues\_opened` | Issues with action `opened` |

| `issues\_closed` | Issues with action `closed` |

| `stars` | Watch events used as observed star activity |

| `forks` | Fork events |

| `top\_contributor\_share` | Largest single-actor event contribution divided by total contributor events for the repository-day |

| `bot\_events` | Events attributed to actors classified as bots |

| `human\_or\_unknown\_events` | Events attributed to all other actors |

| `bot\_contributors` | Distinct bot actors |

| `human\_or\_unknown\_contributors` | Distinct non-bot or unclassified actors |

| `newly\_observed\_contributors` | Human-or-unknown contributors first observed in that repository within the dataset on that date |

| `returning\_contributors` | Human-or-unknown contributors previously observed in the same repository on an earlier dataset date |



\### Important Metric Limitations



`newly\_observed\_contributors` does not represent historically new GitHub contributors.



It represents first observation within the project dataset.



`returning\_contributors` is also relative only to the observation window.



`active\_contributors` includes all actor types, while lifecycle metrics are restricted to `human\_or\_unknown` actors.



`top\_contributor\_share = 1.0` is common in low-activity repository-days where one actor generated all observed events.



\## Ecosystem Daily Summary



\### `workspace.oss\_ecosystem.gold\_ecosystem\_daily\_summary`



Grain: one calendar day.



Key fields include:



| Metric | Description |

|---|---|

| `event\_date` | Calendar date |

| `total\_events` | Total repository-attributable events |

| `active\_repositories` | Number of repositories with observed activity |

| `bot\_events` | Total bot-attributed events |

| `human\_or\_unknown\_events` | Total other events |

| `newly\_observed\_contributors` | Sum of repository-level newly observed contributors |

| `returning\_contributors` | Sum of repository-level returning contributors |

| `bot\_event\_share` | Bot events divided by total events |

| `returning\_contributor\_share` | Returning contributor occurrences divided by newly observed plus returning contributor occurrences |

| `stars` | Observed star activity |

| `forks` | Observed fork activity |

| `prs\_opened` | Opened pull requests |

| `issues\_opened` | Opened issues |



Contributor counts aggregated across repositories represent repository-contributor occurrences, not globally unique GitHub users.

