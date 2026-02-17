# RUM Project — Curation Example

This repository demonstrates a concrete **project using the RUM Framework**. It shows how policies, rules, and actions are applied to manage and process data files in a structured workflow.

The example focuses on the **Check-in policy**, which is the main workflow for ingesting and validating data files.

---

## Project Overview

RUM provides the core engine for policy-driven workflows, but a **project** like this one provides the actual configuration, rules, and input data to process. Without a project, the engine alone does not perform any operations.

This project contains:
- **Policies**: define workflows and link rules
- **Rules**: define conditional logic and trigger actions
- **Actions**: perform actual tasks on files (checks, metadata extraction, copying)


---

## Key Workflow: Check-in Policy

The **Check-in policy** orchestrates the ingestion and validation of new data files.

**Policy file:** `policies/policy-checkin.yaml`

**Associated rules:**
- `rules/rule-filechecks.yaml` — validates file integrity and format
- `rules/rule-extractmetadata.yaml` — extracts/associate metadata from the file
- `rules/rule-cp2archive.yaml` — copies/move the validated file to the archive

### Flow Description

1. **Policy activation**
   - The `policy-checkin` policy is triggered when a new file arrives.
   - The policy defines which rules will be executed and in what order.

2. **Rule evaluation**
   - Each rule is evaluated sequentially by the RUM **sequencer**.
   - Rules check conditions, such as file type, integrity, and presence of required metadata.

3. **Action execution**
   - When a rule condition is satisfied, it triggers its associated **actions**.
   - Actions may have default parameters overridden by the rule configuration.

**Example sequence:**

- **rule-filechecks**:
  - Action: verify file checksum, validate filename pattern
  - Override: none, uses defaults
- **rule-extractmetadata**:
  - Action: extract header info, compute timestamps, mint a PID
  - Override: sets specific parsing options for this rule
- **rule-cp2archive**:
  - Action: copy/move file to archive location
  - Override: sets archive path based on project configuration

4. **Result**
   - The file has been validated, metadata extracted, and stored in the archive.
   - All actions are logged for traceability.

---

## Repository Structure

```
├── actions/         # reusable action implementations
├── config/          # project-specific configurations
├── modules/         # helper modules and utilities
├── policies/        # policy definitions (e.g., policy-checkin.yaml)
├── rules/           # rule definitions (e.g., rule-filechecks.yaml)
├── utils/           # utility scripts
└── README.md
```

---

## How to Use

1. Place the incoming data file in the monitored input directory.
2. Run the RUM sequencer with the `policy-checkin` policy:

```bash
python3 ../rum.py --t --policy policy-checkin --input /path/to/new/file
```

3. The sequencer evaluates the rules and executes actions in order.
4. Check logs or the archive folder to verify that the file has been processed successfully.

---

## Notes

- Actions are **reusable** and may be invoked by multiple rules or policies.
- Rule-specific configuration **overrides** action defaults to adapt behavior.

---

## References

- [RUM Framework](https://github.com/INGV/rum-framework) — Core engine documentation
- *INGV RUM — A Lightweight Rule Manager Framework*, Rapporti Tecnici INGV 508 (2025), DOI: [10.13127/rpt/508](https:doi.org/10.13127/rpt/508)