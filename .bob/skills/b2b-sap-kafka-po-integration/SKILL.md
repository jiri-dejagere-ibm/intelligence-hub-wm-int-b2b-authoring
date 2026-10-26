---
name: b2b-sap-kafka-po-integration
description: >
  Step-by-step playbook for building the end-to-end B2B EDI X12 5010 850 Purchase Order
  integration: webMethods B2B → FlowService → SAP S/4HANA OData V2 + Confluent Kafka.
  Use when implementing or re-running the KiteSports POLARIS-2784 solution authoring demo,
  or any similar B2B-to-ERP-to-Kafka pipeline. Trigger phrases: "B2B purchase order demo",
  "POLARIS-2784", "EDI 850 to SAP", "B2B to SAP and Kafka", "solution authoring demo",
  "KiteSports integration".
---

# B2B EDI 850 → SAP S/4HANA + Kafka — End-to-End Playbook

Repeatable runbook for the KiteSports solution authoring demo (POLARIS-2784).
Covers all steps in order, with every lesson learned from the initial run called out as
**⚠️ Pitfall** blocks.

---

## Prerequisites — confirm before starting

| Asset | Where to check | Expected state |
|---|---|---|
| Partner `KiteTechPartner` | webMethods B2B → Partners | Active |
| Partner `IBM Enterprise` | webMethods B2B → Partners | Active |
| Project `kitesports_solutionauthoring` | webMethods Integration → Projects | Exists |
| Account `SAP_HANA_KiteSports_Conn` | Project accounts | Active |
| Account `Kafka_KiteSports_Account` | Project accounts | Active |
| Kafka topic `orders.created` | Confluent | Exists |
| DAF custom action `CreatePurchaseOrder` | Project DAF services | Available |
| DAF custom action `TrackCreatedPurchaseOrder` | Project DAF services | Available |

Use `context-search` (via the Context Engine) to resolve all IDs upfront. Required IDs:
- `KiteTechPartner` partner ID → used in `sender_ids`
- `IBM Enterprise` partner ID → used in `receiver_ids`
- X12 5010 850 document ID (`WMX12-5010--850---------`) → used in `document_type_ids`
- Project alias for `kitesports_solutionauthoring` → used in `list_integration_services`

---

## Phase 1 — Read the JIRA requirement

1. Call `jira_get_issue` with the JIRA key (e.g. `POLARIS-2784`).
2. Extract: FlowService name, processing rule name, SAP connection name, Kafka topic, partner names.
3. Note the **v2** suffix on the rule name — an older `v1` rule may already exist (Inactive). Do not
   reuse or activate it; always create the new `v2` rule as specified.

---

## Phase 2 — Context Engine discovery

Call `context-search` for each asset class separately (never in one broad query):

```
query: "KiteTechPartner partner B2B"          capability: "webMethods B2B Integration"   asset_type: "partner"
query: "IBM Enterprise partner B2B"           capability: "webMethods B2B Integration"   asset_type: "partner"
query: "X12 5010 850 document type"           capability: "webMethods B2B Integration"   asset_type: "document"
query: "kitesports_solutionauthoring project" capability: "webMethods Integration"        asset_type: "Project"
query: "SAP_HANA_KiteSports_Conn"             capability: "webMethods Integration"        asset_type: "account"
query: "Kafka_KiteSports_Account"             capability: "webMethods Integration"        asset_type: "account"
```

Record every `id` / `alias` field returned — you will need them in later phases.

---

## Phase 3 — Author and deploy the FlowService

### 3.1 Generate FSL

Use the `flow-orchestrator` skill to author `ProcessPurchaseOrder850ToSAPV2` in project
`kitesports_solutionauthoring`. Write the generated FSL to `tmp/ProcessPurchaseOrder850ToSAPV2.fsl`
before deploying — this gives a local audit trail and makes it easy to re-deploy or diff on the
next run. The `tmp/` folder at the repo root is the designated scratch area for all intermediate
build artefacts in this demo; it is excluded from version control via `.gitignore`.

The service must implement:

1. **Parse EDI** — invoke `wm.b2b.edi:convertToValues` on `request/content` using the
   `X12_5010_850_DT` document type.
2. **Map to SAP** — transform parsed EDI fields to `A_PurchaseOrderType` deep-insert structure:
   - `BEG03` → `PurchaseOrder`
   - `N1*BY` Loop → `PurchaseOrderType`, `CompanyCode`, `PurchasingOrganization`
   - `PO1` Loop → `to_PurchaseOrderItem` line items
3. **Create SAP order** — call DAF action `CreatePurchaseOrder` (connection: `SAP_HANA_KiteSports_Conn`).
4. **Publish Kafka event** — on SAP success (no error), call DAF action `TrackCreatedPurchaseOrder`
   (connection: `Kafka_KiteSports_Account`, topic: `orders.created`) with fields:
   `poNumber`, `sapOrderId`, `totalAmount`, `timestamp`.
5. **Error handling** — if SAP call fails, set `error/code` + `error/message` on output; do NOT
   publish to Kafka.

### 3.2 Deploy

Call `deploy_flow_service` (or equivalent FSL build tool) targeting project `kitesports_solutionauthoring`.

### 3.3 Create B2B ingress ← **⚠️ MUST NOT BE SKIPPED**

> **Pitfall (learned the hard way):** Without this step the processing rule will resolve the service
> name but the B2B runtime has no registered ingress to route the call through. Transactions will
> fall through to the `Default rule` instead of executing the FlowService.

Immediately after a successful deploy, call:

```
tool: create_b2b_ingress_for_flow
  project_name: kitesports_solutionauthoring
  service_name: ProcessPurchaseOrder850ToSAPV2
```

Confirm the call succeeds before moving to Phase 4.

---

## Phase 4 — Resolve DAF serviceFullName

> **Pitfall:** `list_integration_services` returns `serviceFullName: null` for DAF services.
> `create_rule` requires `integration_ns_name` for DAF type — never skip this lookup.

Call `list_daf_services` with `projectId: kitesports_solutionauthoring` and filter by name
`ProcessPurchaseOrder850ToSAPV2`. Record the `serviceFullName` field, e.g.:

```
project.kitesports_solutionauthoring.integrations:ProcessPurchaseOrder850ToSAPV2
```

This value is passed as `integration_ns_name` in `create_rule`.

---

## Phase 5 — Create the B2B processing rule

Call `create_rule` with the following parameters (all IDs resolved in Phase 2):

| Parameter | Value |
|---|---|
| `name` | `KiteTechPartner_PurchaseOrder_850_ProcessingRulev2` |
| `description` | `Inbound EDI X12 5010 850 Purchase Order from KiteTechPartner — routes to ProcessPurchaseOrder850ToSAPV2 FlowService (SAP S/4HANA + Kafka analytics)` |
| `status` | `Inactive` (activate separately after creation) |
| `sender_ids` | `[{ "id": "<KiteTechPartner partner ID>" }]` |
| `receiver_ids` | `[{ "id": "<IBM Enterprise partner ID>" }]` |
| `document_type_ids` | `[{ "id": "WMX12-5010--850---------" }]` |
| `call_integration_endpoint_type` | `integration` |
| `integration_project_name` | `kitesports_solutionauthoring` |
| `integration_name` | `ProcessPurchaseOrder850ToSAPV2` |
| `integration_type` | `daf` |
| `integration_ns_name` | `project.kitesports_solutionauthoring.integrations:ProcessPurchaseOrder850ToSAPV2` |
| `orchestration_mode` | `reliable` |

### Credential elicitation

> **Pitfall:** `create_rule` with `call_integration_endpoint_type: integration` always returns a
> browser elicitation link — even on a retry. The link expires in **30 minutes**.

The server returns a secure browser link to collect `integration_run_as`. Share the link with the
user exactly as returned. Say:

> "Please open this link, enter your username and password, and paste the `credential_token` value
> shown on the success page back here."

**Do not ask for credentials in chat. Do not proceed without the token.**

Once the user pastes the token, re-invoke `create_rule` with all the same parameters plus
`credential_token: <pasted value>`.

### Verify criteria immediately — do not skip

> **Pitfall:** In the demo run, `create_rule` succeeded but the rule was saved with **empty
> criteria** (no sender, receiver or document type). An empty-criteria rule matches every
> document, so it looks fine until the test. The `create_rule` response does not make this obvious.

As soon as `create_rule` returns a `ruleId`, call `get_rule` and confirm `criteria` contains all
three of:

- sender = `KiteTechPartner` (`m1ajl700k4v7fj3f0000007p`)
- receiver = `IBM Enterprise` (`m1b5fq00k5bp7u6800000070`)
- document type = `WMX12-5010--850---------`

If any is missing, fix it with `update_rule` (do not delete and recreate; that needs a new
credential token):

```
tool: update_rule
  rule_id: <ruleId>
  sender_ids: [{ "id": "m1ajl700k4v7fj3f0000007p" }]
  receiver_ids: [{ "id": "m1b5fq00k5bp7u6800000070" }]
  document_type_ids: [{ "id": "WMX12-5010--850---------" }]
  recognition_errors: ignore
```

Notes:
- The ID arrays must be arrays of `{ "id": "<value>" }` objects, never plain strings.
- `update_rule` replaces the whole criteria group, so always send all three ID lists together plus
  `recognition_errors`. Fields omitted from the criteria group are cleared.
- Do not send `call_integration_endpoint_type` or the other `integration_*` fields in this call.
  That would trigger a new credential elicitation and the action group is replaced as a whole.
  Leaving the action group out keeps the existing endpoint. Confirm with `get_rule` afterwards.

### Reference configuration (v1 rule)

The pre-existing v1 rule `KiteTechPartner_PurchaseOrder_850_ProcessingRule`
(`m1ad1l00k5eqma38000000g6`) is a working example of the target shape. If anything is unclear, call
`get_rule` on it (read-only) instead of guessing. The v2 rule should match it except for the name
and status. Do not modify or activate v1.

Expected final state of the v2 rule (as returned by `get_rule`):

| Field | Expected |
|---|---|
| criteria: sender | `KiteTechPartner` |
| criteria: receiver | `IBM Enterprise` |
| criteria: document type | `X12 5010 850` (`WMX12-5010--850---------`) |
| criteria: recognition errors | `ignore` |
| action: integration | `ProcessPurchaseOrder850ToSAPV2` (type `daf`, project `kitesports_solutionauthoring`) |
| orchestration mode | `reliable` |
| `ordinalIndex` | `0` after activation (highest priority) |

---

## Phase 6 — Activate the rule

After `create_rule` succeeds and returns a `ruleId`, immediately call:

```
tool: set_rule_status
  rule_id: <returned ruleId>
  status: Active
```

Confirm the response shows `status: Active`.

---

## Phase 7 — End-to-end test

> **Pitfall:** Do not construct your own EDI payload. Use the exact payload from [`script.md`](script.md)
> chapter 7. It contains the ISA sender/receiver IDs (`SENDERID` / `RECEIVERID`) that match the
> external EDI identifiers registered on the `KiteTechPartner` and `IBM Enterprise` partner profiles
> in this demo environment. Any other ISA IDs will resolve as `Unknown` and fall through to the
> default rule.

Submit using:

```
tool: submit_document
  type: edi
  content: <exact payload from script.md §7, joined as a single string with ~ delimiters, no line breaks>
```

Expected result:

| Field | Expected value |
|---|---|
| `senderName` | `KiteTechPartner` |
| `receiverName` | `IBM Enterprise` |
| `transactionStatus` | `DONE` |

If `senderName` / `receiverName` come back as `Unknown`, the ISA IDs in the payload do not match
the registered partner external IDs — fix the payload, not the rule.

Verify the activity log with `get_transaction_activity_log`. Look for:
- `Routing rule KiteTechPartner_PurchaseOrder_850_ProcessingRulev2 selected` (not `Default rule`)
- No `Invalid Sender ID` / `Invalid Receiver ID` errors

---

## Phase 8 — Demo cleanup

Removes only the two assets created during the demo run. All pre-existing infrastructure
(partners, project, accounts, document type, channels, Kafka topic) must be left untouched.

### Assets created by this demo

| Asset | Type | Tool to remove |
|---|---|---|
| `KiteTechPartner_PurchaseOrder_850_ProcessingRulev2` | B2B processing rule | `set_rule_status` → Inactive |
| `ProcessPurchaseOrder850ToSAPV2` | webMethods Integration FlowService | `delete_daf` |

> **Note:** There is no delete tool for B2B processing rules — deactivating it is the correct
> cleanup. It will no longer match or route any inbound documents once Inactive.

### Step 1 — Deactivate the processing rule

```
tool: set_rule_status
  rule_id: m1a3f700k5s4ucnk000002m3
  status: Inactive
```

If the rule ID is not known (e.g. a fresh session), resolve it first:

```
tool: get_rules
  filter: KiteTechPartner_PurchaseOrder_850_ProcessingRulev2
```

Use the `ruleId` from the response.

Confirm the response shows `status: Inactive`.

### Step 2 — Delete the FlowService

```
tool: delete_daf
  projectName: kitesports_solutionauthoring
  flowServiceName: ProcessPurchaseOrder850ToSAPV2
```

> **⚠️ Irreversible.** `delete_daf` permanently removes the service definition. The B2B ingress
> created in Phase 3.3 is also rendered inert once the service is gone.

Confirm no error is returned.

### What is NOT cleaned up

The following were pre-existing before the demo and must remain intact:

- Partners `KiteTechPartner` and `IBM Enterprise`
- Project `kitesports_solutionauthoring` and its accounts (`SAP_HANA_KiteSports_Conn`, `Kafka_KiteSports_Account`)
- DAF custom actions `CreatePurchaseOrder` and `TrackCreatedPurchaseOrder`
- Document type `X12 5010 850` (`WMX12-5010--850---------`)
- Kafka topic `orders.created`
- The older Inactive v1 rule `KiteTechPartner_PurchaseOrder_850_ProcessingRule` (pre-existing, leave as-is)

---

## Pitfalls — consolidated reference

| # | Pitfall | Fix |
|---|---|---|
| 1 | **B2B ingress not created** — FlowService deploys but B2B has no ingress; transactions fall to default rule | Call `create_b2b_ingress_for_flow` immediately after deploy |
| 2 | **`serviceFullName` is null from `list_integration_services`** — `create_rule` fails or misbehaves for DAF type without `integration_ns_name` | Always call `list_daf_services` to get the real `serviceFullName`; pass it as `integration_ns_name` |
| 3 | **Credential elicitation link expires** — 30-minute TTL; re-calling `create_rule` without a token issues a new link | Re-invoke `create_rule` (same params, no token) to get a fresh link |
| 4 | **Wrong ISA IDs in test payload** — fabricated sender/receiver IDs resolve as `Unknown` and miss the processing rule | Use the exact payload from `script.md` §7; ISA IDs must match registered partner external IDs |
| 5 | **Line breaks in EDI payload** — ISA segment split across lines causes `Unexpected segment` parse error | Flatten the entire interchange to a single string; use `~` as the only segment delimiter |
| 6 | **Stale v1 rule confusion** — an existing Inactive rule (`v1`) may surface in `get_rules`; its endpoint already points at `ProcessPurchaseOrder850ToSAPV2` | Do not activate or modify it; create the new `v2` rule as specified in POLARIS-2784. It may be read (`get_rule`) as a reference for the target configuration |
| 7 | **`integration_project_name` vs `alias`** — `create_rule` needs the human-readable `name` field (`kitesports_solutionauthoring`), not the `alias` (hex ID) | Use `name` from `list_integration_projects` output, never the `alias` |
| 8 | **`create_rule` saves empty criteria** — the rule matches every document; transactions may appear to route correctly or fall to the Default rule | Right after `create_rule`, run `get_rule` to verify sender, receiver and document type; fix with `update_rule` using `[{ "id": ... }]` arrays plus `recognition_errors: ignore` (see Phase 5) |

---

## Asset reference (demo environment)

| Asset | ID / Value |
|---|---|
| `KiteTechPartner` partner ID | `m1ajl700k4v7fj3f0000007p` |
| `IBM Enterprise` partner ID | `m1b5fq00k5bp7u6800000070` |
| X12 5010 850 document ID | `WMX12-5010--850---------` |
| Project alias | `fl6f5d1ceaf47a4d7eaa283d4d25cfc677` |
| FlowService `serviceFullName` | `project.kitesports_solutionauthoring.integrations:ProcessPurchaseOrder850ToSAPV2` |
| Processing rule ID (current) | `m1a3f700k5s4ucnk000002m3` |
| SAP connection | `SAP_HANA_KiteSports_Conn` |
| Kafka connection | `Kafka_KiteSports_Account` |
| Kafka topic | `orders.created` |
