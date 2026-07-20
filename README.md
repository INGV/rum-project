# RUM Project — Curation Example

This repository contains a concrete **RUM Project** built on top of the **[RUM Framework](https://github.com/INGV/rum-framework)**.

While the **RUM Framework** provides the generic execution engine, a **Project** defines the actual behaviour of the system by supplying Policies, Rules, Actions, Contexts and project-specific modules.

> **The framework executes. The project defines the behaviour.**

This repository should therefore be considered both:

* a working example of a RUM project;
* a starting point for developing new RUM-based applications.

---

# Relationship with the RUM Framework

RUM intentionally separates the execution engine from the application domain.

The **Framework** is responsible for:

* sequencing;
* configuration management;
* action loading;
* logging;
* session management;
* workflow orchestration.

The **Project** provides:

* Policies;
* Rules;
* Actions;
* Contexts;
* project-specific modules;
* project-specific configurations.

This separation allows the same RUM Framework to support multiple independent projects without modifying the framework itself.

---

# Repository Structure

```
project/
│
├── actions/        # reusable Action implementations
├── config/         # Action configuration files
├── contexts/       # execution Context definitions
├── modules/        # project-specific Python modules
├── policies/       # Policy definitions
├── rules/          # Rule definitions
├── utils/          # project utility functions
└── README.md
```

Each directory represents one aspect of the project-specific logic executed by the RUM Framework.

---

# Core Components

A RUM Project is composed of four main elements.

## Policies

Policies describe the overall workflow.

A Policy specifies:

* which Rules will be executed;
* the execution order;
* the operational objective.

Policies do **not** implement business logic.

---

## Rules

Rules organize the execution flow.

They decide:

* when an Action should be executed;
* which configuration overrides should be applied.

Rules connect Policies with Actions.

---

## Actions

Actions are the fundamental execution units of a RUM Project.

Each Action performs one specific task, for example:

* validating files;
* extracting metadata;
* generating Persistent Identifiers;
* updating provenance;
* copying files;
* invoking external services.

Actions should remain:

* reusable;
* independent;
* self-contained;
* easy to understand.

Whenever possible, complexity should be implemented inside Actions rather than inside the framework.

---

## Contexts

Projects may optionally define one or more **Contexts**.

A Context contains execution-specific information shared by all Actions during a processing session.

Typical information includes:

* issue identifiers;
* requesting organizations;
* provenance metadata;
* execution options;
* project-specific operational information.

The Context is loaded once during the RUM bootstrap phase and remains read-only for the entire execution.

Its purpose is **not** to modify the framework behaviour, but to describe the operational environment in which a Policy is executed.

---

# Developing New Actions

One of the design goals of RUM is that **Actions can be developed independently from the framework**.

Developers should not need to execute the complete framework while writing a new Action.

For this reason, every project includes an **Action Template**.

---

## The Action Template

The provided `action-template.py` implements all conventions required by the framework, including:

* Action class structure;
* constructor interface;
* configuration loading;
* logging support;
* Session access;
* Context access;
* standalone execution for testing.

Developers are encouraged to use the template as the starting point for every new Action.

---

## Recommended Development Workflow

The recommended workflow for implementing a new Action is intentionally simple.

```
Copy action-template.py
          │
          ▼
Implement the Action
          │
          ▼
Execute it as a standalone program
          │
          ▼
Validate the behaviour
          │
          ▼
Add the Action configuration
          │
          ▼
Reference the Action inside a Rule
          │
          ▼
Execute the complete workflow through RUM
```

Developing Actions outside the framework considerably simplifies debugging and testing.

Only after an Action has been validated should it be integrated into a Rule and executed by the RUM sequencer.

---

# Example Workflow

This project demonstrates the **Check-in Policy**, responsible for validating and ingesting new files into the trusted archive.

The workflow is composed of three Rules.

## 1. File Checks

* validate file integrity;
* verify filename format;
* perform sanity checks.

## 2. Metadata Extraction

* extract metadata;
* generate Handle/PID;
* create provenance information.

## 3. Archive Update

* move the validated file into the trusted archive;
* complete the ingestion process.

Each Rule invokes one or more Actions that perform the actual work.

---

# Design Philosophy

Projects are expected to follow the same philosophy as the RUM Framework.

Keep Projects:

* simple;
* modular;
* readable;
* reusable.

Whenever possible:

* add a new Action instead of modifying an existing one;
* add a new Rule instead of introducing complex conditional logic;
* add a new Policy instead of creating special cases inside the framework.

Following the **[Bicycle Principle](https://github.com/INGV/rum-framework#the-bicycle-principle)**, complexity should emerge from Projects—not from the RUM Framework itself.

---

# References

* **RUM Framework** — https://github.com/INGV/rum-framework
* **INGV RUM — A Lightweight Rule Manager Framework**, Rapporti Tecnici INGV 508 (2025), DOI: https://doi.org/10.13127/rpt/508
