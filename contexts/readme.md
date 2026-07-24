# Context File

The **Context** file contains the execution information associated with a specific RUM processing.

Unlike **Policies** and **Rules**, which describe **how** data must be processed, the **Context** describes **under which conditions** the current execution is performed.

A Context is loaded once during the RUM bootstrap phase and remains available, read-only, for the entire execution. It provides Actions with additional information that is **not part of the Digital Object itself**, such as the reason for the operation, the requesting organization, software agents involved, or execution options.

Typically, a Context file corresponds to a single operational request (e.g., a GitLab Issue) and can be reused for processing multiple files belonging to the same activity.

---

## Context Fields

### `CONTEXT_SCHEMA_VERSION`

Version of the Context file schema.

This value identifies the structure of the Context document and allows future schema evolution while maintaining backward compatibility.

---

## `ISSUE`

Information about the tracking system associated with the processing request.

| Field | Description |
|-------|-------------|
| `ID` | Identifier of the issue or ticket associated with the operation. |
| `TRACKER` | Name of the issue tracking system (e.g., GitLab, Jira, Redmine). |

---

## `REQUEST`

Administrative information describing why the processing is being performed.

| Field | Description |
|-------|-------------|
| `REQUESTOR` | Person requesting the operation. |
| `ORGANIZATION` | Organization requesting the processing. |
| `REASON` | Short description of the purpose of the operation. |
| `COMMENT` | Additional notes or free-text comments. |

---

## `PROVENANCE`

Information used to populate the provenance records associated with the generated version.

| Field | Description |
|-------|-------------|
| `schema_Organization` | Organization responsible for the update operation. |
| `prov_SoftwareAgent` | List of software applications involved in producing the updated version. |

---

## `OPTIONS`

Execution options that can be used by Actions during the processing.

| Field | Description |
|-------|-------------|
| `VERIFY_CHECKSUM` | If `true`, Actions may verify the consistency of the incoming file before updating the archive. |
| `FORCE_VERSION` | If `true`, allows the update procedure to create a new version even when normal validation checks would prevent it. |

---

## Example

```yaml
CONTEXT_SCHEMA_VERSION: 1.0

ISSUE:
    ID: ISSUE-2456
    TRACKER: GitLab

REQUEST:
    REQUESTOR: Mario Rossi
    ORGANIZATION: INGV-ONT Sez. Palermo
    REASON: Offline archive synchronization
    COMMENT: Recovery after disk replacement

PROVENANCE:
    prov_wasAttributedTo: INGV - Italian EIDA node
    prov_wasGeneratedBy:
        - prov_hadPrimarySource: http://webservices.ingv.it/fdsnws/station/1/query?level=response
        - schema_SoftwareApplication:
            - http://ds.iris.edu/pub/programs/SeedLink/
        - schema_Organization: This Network Operator 
        - dcterms_accrualPeriodicity: continuous

VERSION:
    schema_Organization: ONT - INGV ITALY
    prov_SoftwareAgent:
        - https://github.com/obspy/obspy
        - https://github.com/qt/qttools/tree/5.3

OPTIONS:
    VERIFY_CHECKSUM: true
    FORCE_VERSION: false
```

---

> **Design Note**
>
> A **Context** describes the execution environment of a RUM processing.
> It is **not part of the Digital Object** and does **not modify** the behaviour defined by Policies or Rules.
> Instead, it provides execution-specific information shared by all Actions executed during the current processing.
>
> The Context is loaded once during the bootstrap phase, remains immutable throughout the execution, and is shared by every Action involved in the processing.