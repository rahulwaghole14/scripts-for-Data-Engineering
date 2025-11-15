# Contribution Guide

This guide outlines the development workflow and commit message conventions for this dbt project.

## Table of Contents

1. [Commit Message Convention](#commit-message-convention)
2. [Development Workflow](#development-workflow)
   - [Using dbt CLI](#using-dbt-cli)
   - [dbt Power User in VSCode](#dbt-power-user-in-vscode)

---

## Commit Message Convention

We adhere to the [Conventional Commits Specification](https://www.conventionalcommits.org/en). While this is not a strictly enforced standard, please make an effort to follow it.

- `fix`: For bug fixes (e.g., fixing dbt errors due to bad SQL queries)
- `feat`: For new features (e.g., adding a new model)
- `refactor`: For code refactoring (e.g., rewriting datamart logic)
- `docs`: For documentation changes (e.g., adding usage examples)
- `test`: For adding or updating tests (e.g., adding dbt tests)
- `chore`: For tooling and operational changes (e.g., updating CI configuration)

> **Tip**: If you're using VSCode, the 'Conventional Commits' extension can assist with this.

---

## Development Workflow

### Using dbt CLI

#### Initial Setup

1. Set up your environment using `pyenv` and install the required packages. Refer to `README.md` for details.
2. Verify the dbt installation:

    ```bash
    dbt --version
    ```

3. Place the `profiles.yml` file in the project root (it's currently git-ignored).

#### Commands

1. Test connection and install dependencies:

    ```bash
    $ dbt debug
    $ dbt deps
    ```

2. Run dbt:

    ```bash
    $ dbt run
    ```

3. Execute tests:

    ```bash
    $ dbt test
    ```

4. generate seeds:

    ```bash
    $ dbt seed
    ```

### dbt Power User in VSCode

If you're using VSCode, consider installing the dbt Power User extension to enhance your workflow.

---

## VSCode Settings

You may want to update the `.vscode/settings.json` to make it easier to work on this project. Replace the placeholders as relevant to your machine. Ask another developer if you get stuck.

```json
{
    "dbt.queryLimit": 500,
    "files.associations": {
        "*.sql": "jinja-sql"
    },
    "files.exclude": {
        "**/.git": false
    },
    "terminal.integrated.env.osx": {
      "DBT_PROFILES_DIR": "<Your-Path-Here>"
    },
    "conventionalCommits.scopes": [
        "legacy",
        "smart",
        "sqlfluff",
        "idm",
        "piano"
    ]
}
