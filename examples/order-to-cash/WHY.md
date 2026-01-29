# Order-to-Cash: End-to-End Example

## The Business Question

A VP of Operations asks: **"Why are 30% of our orders delivered late?"**

Nobody can answer this because the data lives in 3 systems that don't talk to each other:

```
SAP ERP                Salesforce CRM           Shipping System
(Order fulfillment)    (Sales pipeline)         (Warehouse + Delivery)
───────────────        ──────────────           ─────────────────
VBELN (order #)        SAP_Order__c (ref)       order_ref (ref)
ERDAT (date)           LastModifiedDate         event_time
ERNAM (who)            OwnerId (who)            carrier (who)
FNAME (what changed)   StageName (stage)        event (what)
```

Each system knows its own piece. None knows the full story.

## The 3 Raw Files

### 1. `sap_orders.csv` — What SAP exports look like

Real SAP column names. VBELN is the sales document number. ERDAT is creation
date. ERNAM is who touched it. KUNNR is the customer number. These are the
actual field names from SAP table VBAK.

This file has NO "activity" column. SAP doesn't think in terms of activities.
It thinks in terms of status field changes (LIFSK = delivery block,
FAKSK = billing block, GBSTK = overall status).

### 2. `sap_change_docs.csv` — Where SAP activities actually come from

SAP tracks changes in "change documents." Each row means: "User X changed
field Y from value A to value B at time T on document Z."

THIS is where events come from in SAP. Not from a log. From change records.
The "activity" must be DERIVED: "LIFSK changed from empty to B" means
"Delivery Block Set."

### 3. `salesforce_opportunities.csv` — What Salesforce exports look like

Real Salesforce API field names. StageName is the sales pipeline stage.
The custom field SAP_Order__c links this opportunity back to the SAP order.
This is the JOIN KEY between Salesforce and SAP.

### 4. `shipping_system.csv` — Warehouse management

The shipping system has its own order_ref that matches SAP's VBELN.
The "event" column IS the activity (Picked, Packed, Shipped, Delivered).
This is the only system that actually has a clean activity column.

## Why You Need a Target Schema

Process mining algorithms need 3 things to work:

```
┌─────────────────────────────────────────┐
│         MINIMUM VIABLE EVENT            │
│                                         │
│  1. CASE ID    → "Which process run?"   │
│  2. ACTIVITY   → "What happened?"       │
│  3. TIMESTAMP  → "When?"                │
│                                         │
│  + optional: resource, cost, objects    │
└─────────────────────────────────────────┘
```

Without these 3 fields, you CANNOT:
- Discover the process model (DFG, Petri net, BPMN)
- Find bottlenecks (time between activities)
- Check conformance (does reality match the designed process?)
- Compare processes (drift detection between months/regions)
- Build a PMPT (Process Merkle Patricia Tree for O(1) comparison)

The raw files DON'T have these fields:

| File                | Has Case ID? | Has Activity?          | Has Timestamp? |
|---------------------|-------------|------------------------|---------------|
| sap_orders.csv      | VBELN (yes) | NO — status fields     | ERDAT (yes)   |
| sap_change_docs.csv | OBJECTID    | NO — must be derived   | UDATE+UTIME   |
| salesforce.csv      | SAP_Order__c| StageName (partial)    | LastModified   |
| shipping.csv        | order_ref   | event (yes!)           | event_time    |

## The Mapping: What the User Must Define

The user creates a mapping config. This is NOT automatable — only the user
knows that VBELN is their order number, that FNAME changes represent
activities, and that SAP_Order__c links to VBELN.

```yaml
# .logflow-mapping.yaml (lives in git next to the data)

target_schema:
  case_id: order_id
  timestamp: event_time
  activity: activity
  objects:
    - type: order
    - type: customer
    - type: shipment

sources:
  sap_changes:
    file: sap_change_docs.csv
    mapping:
      OBJECTID: order_id                    # case ID
      UDATE+UTIME: event_time              # timestamp (needs concat)
      USERNAME: resource
    derive_activity:                        # activity must be computed
      from: [FNAME, VALUE_OLD, VALUE_NEW]
      rules:
        - when: {FNAME: AUART, VALUE_OLD: ""}
          activity: "Order Created"
        - when: {FNAME: LIFSK, VALUE_NEW: "B"}
          activity: "Delivery Block Set"
        - when: {FNAME: LIFSK, VALUE_OLD: "B"}
          activity: "Delivery Block Released"
        - when: {FNAME: FAKSK, VALUE_NEW: "A"}
          activity: "Billing Block Set"
        - when: {FNAME: FAKSK, VALUE_OLD: "A"}
          activity: "Billing Block Released"
        - when: {FNAME: FKSTK}
          activity: "Invoice Created"
        - when: {FNAME: GBSTK, VALUE_NEW: "C"}
          activity: "Order Completed"
    objects:
      order_id: {column: OBJECTID, type: order}

  salesforce:
    file: salesforce_opportunities.csv
    mapping:
      SAP_Order__c: order_id               # JOIN KEY to SAP
      LastModifiedDate: event_time
      StageName: activity                  # activity is directly available
      OwnerId: resource
    objects:
      order_id: {column: SAP_Order__c, type: order}
      customer_id: {column: AccountId, type: customer}

  shipping:
    file: shipping_system.csv
    mapping:
      order_ref: order_id                  # JOIN KEY to SAP
      event_time: event_time
      event: activity                      # clean activity column
      carrier: resource
    objects:
      order_id: {column: order_ref, type: order}
      shipment_id: {column: shipment_id, type: shipment}

entity_resolution:
  # These columns in different sources refer to the SAME entity
  order:
    sap_changes: OBJECTID
    salesforce: SAP_Order__c
    shipping: order_ref
```

## What the Unified Output Looks Like

After mapping + joining, you get ONE event log sorted by time:

```
order_id     | activity                | event_time          | resource | source     | objects
─────────────┼─────────────────────────┼─────────────────────┼──────────┼────────────┼────────────────────
4500001000   | Qualification           | 2025-01-01 09:00:00 | OwnerA   | salesforce | order:4500001000, customer:001R34xyz
4500001000   | Order Created           | 2025-01-02 08:14:22 | MUELLER  | sap        | order:4500001000
4500001000   | Proposal                | 2025-01-02 14:30:00 | OwnerA   | salesforce | order:4500001000, customer:001R34xyz
4500001000   | Delivery Block Set      | 2025-01-03 10:30:00 | SCHMIDT  | sap        | order:4500001000
4500001000   | Negotiation             | 2025-01-04 11:00:00 | OwnerA   | salesforce | order:4500001000, customer:001R34xyz
4500001000   | Delivery Block Released  | 2025-01-05 14:22:11 | WEBER    | sap        | order:4500001000
4500001000   | Closed Won              | 2025-01-05 16:00:00 | OwnerA   | salesforce | order:4500001000, customer:001R34xyz
4500001000   | Picked                  | 2025-01-06 08:00:00 | DHL      | shipping   | order:4500001000, shipment:SHP-90001
4500001000   | Packed                  | 2025-01-06 14:00:00 | DHL      | shipping   | order:4500001000, shipment:SHP-90001
4500001000   | Shipped                 | 2025-01-07 06:30:00 | DHL      | shipping   | order:4500001000, shipment:SHP-90001
4500001000   | In Transit              | 2025-01-08 12:00:00 | DHL      | shipping   | order:4500001000, shipment:SHP-90001
4500001000   | Delivered               | 2025-01-10 15:30:00 | DHL      | shipping   | order:4500001000, shipment:SHP-90001
4500001000   | Order Completed         | 2025-01-12 09:00:00 | SYSTEM   | sap        | order:4500001000
```

## What You Can Now Do With This (THE POINT)

### 1. Discover the actual process

Run DFG discovery on this unified log. You get:

```
Qualification → Proposal → Order Created → Delivery Block Set → Negotiation
  → Delivery Block Released → Closed Won → Picked → Packed → Shipped
  → In Transit → Delivered → Order Completed
```

You can SEE that the sales team (Salesforce) and the fulfillment team (SAP)
are working IN PARALLEL. You couldn't see this from any single system.

### 2. Find the bottleneck

Order 4500001001 has a "Delayed" event in shipping. The time from
"Shipped" (Jan 10) to "Delivered" (Jan 14) is 4 days vs the normal 2-3 days.
Process mining shows this as a bottleneck edge in the DFG.

### 3. Answer the VP's question

"Why are 30% of orders late?"

From the unified log, you can compute:
- Orders with "Delivery Block Set" spend 2 extra days on average
- Orders going through WH-Chicago have a "Delayed" event 40% of the time
- FedEx shipments take 1 day longer than DHL on average

NONE of this is visible from any single system. You need the unified view.

### 4. OCEL analysis (object-centric)

Because the output has multiple object types (order, customer, shipment),
you can ask:
- "Show me Customer 001R34xyz's process" → sees ALL their orders
- "Show me Shipment SHP-90002's lifecycle" → Picked → Packed → Shipped → Delayed → Delivered
- "Which customers have the most delivery blocks?" → cross-reference SAP + Salesforce

This is the convergence/divergence-free analysis that OCEL enables.

## Summary: Why the Schema Matters

```
Raw Data (chaos)           Target Schema (structure)      Analysis (insight)
────────────────           ─────────────────────────      ──────────────────
SAP: VBELN, ERDAT,    →   order_id, activity,        →   DFG discovery
     FNAME, VALUE_NEW      event_time, resource,          Bottleneck detection
                           source, objects                 Conformance checking
SF:  SAP_Order__c,    →                              →   Cross-system analysis
     StageName, Date                                      Object-centric mining

Shipping: order_ref,  →                              →   "Why are orders late?"
          event, time
```

The schema is not arbitrary. It's the contract between your raw data and
every process mining algorithm that exists. Without case_id + activity +
timestamp, no algorithm can run. The mapping config is how the user tells
the tool "this is what my columns mean."

## To Delete This Example

```bash
rm -rf examples/order-to-cash/
```
