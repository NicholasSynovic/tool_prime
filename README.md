# Python Template Repository

> A template repository that Python projects can inherit from to ensure tooling
> consistency

## Table of Contents

- [Python Template Repository](#python-template-repository)
  - [Table of Contents](#table-of-contents)
  - [About](#about)
  - [Supported Tooling](#supported-tooling)
    - [Visual Studio Code](#visual-studio-code)
      - [Development Containers](#development-containers)
    - [Base Template](#base-template)
    - [`.gitignore`](#gitignore)
    - [`requirements.txt`](#requirementstxt)
    - [Poetry](#poetry)
    - [Pre-Commit](#pre-commit)
      - [Hooks](#hooks)
  - [Encouraged Tooling](#encouraged-tooling)
  - [Template Structure](#template-structure)

## About

This is a template repository that is intended to be inherited by other template
repositories *to ensure consistent common tool deployment Python projects*.

This will also support tooling from my
[`template_base`](https://github.com/NicholasSynovic/template_base) repository.

Additionally, while projects can leverage this template or its content,
extending this template is encouraged

## Supported Tooling

### Visual Studio Code

> Modifies and extends original `template_base`

- Website: [https://code.visualstudio.com/](https://code.visualstudio.com/)
- File(s):
  - Development Containers: [.devcontainer/](.devcontainer/)

#### Development Containers

- File: [.devcontainer/devcontainer.json](.devcontainer/devcontainer.json)
- Documentation:
  [https://containers.dev/implementors/json_reference](https://containers.dev/implementors/json_reference)
- Base image: [python:3.10-bookworm](https://hub.docker.com/_/python/)
- Extensions:
  - [autoDocstring - Python Docstring Generator](https://marketplace.visualstudio.com/items?itemName=njpwerner.autodocstring)
  - [Python](https://marketplace.visualstudio.com/items?itemName=ms-python.python)
  - [Python Debugger](https://marketplace.visualstudio.com/items?itemName=ms-python.debugpy)

### Base Template

> All `template_base` tooling is supported

- Documentation:
  [https://github.com/NicholasSynovic/template_base](https://github.com/NicholasSynovic/template_base)

### `.gitignore`

- File: [.gitignore](.gitignore)
- Documentation:
  [https://git-scm.com/docs/gitignore](https://git-scm.com/docs/gitignore)

### `requirements.txt`

- File: [requirements.txt](requirements.txt)
- Documentation:
  [https://pip.pypa.io/en/stable/reference/requirements-file-format/](https://pip.pypa.io/en/stable/reference/requirements-file-format/)
- Packages:
  - [`poetry`](https://python-poetry.org/)

### Poetry

- File: [pyproject.toml](pyproject.toml)
- Documentation: [https://python-poetry.org/](https://python-poetry.org/)

### Pre-Commit

> Modifies and extends original `template_base`

- Website: [https://pre-commit.com/](https://pre-commit.com/)
- File: [.pre-commit-config.yaml](.pre-commit-config.yaml)

#### Hooks

- [pyroma](https://github.com/executablebooks/mdformat)
- [black](https://black.readthedocs.io/en/stable/index.html)
- [flake8](https://flake8.pycqa.org/en/latest/)
- [isort](https://pycqa.github.io/isort/)
  - Config: [.isort.cfg](.isort.cfg)
- [bandit](https://bandit.readthedocs.io/en/latest/)

## Encouraged Tooling

For API docs, please consider using one of the following:

- [Sphinx](https://www.sphinx-doc.org/en/master/)
  - [Built-in themes](https://www.sphinx-doc.org/en/master/usage/theming.html#builtin-themes)
  - [Themes](https://sphinx-themes.readthedocs.io/en/latest/)
  - [Extensions](https://awesomesphinx.useblocks.com/)
- [MkDocs](https://www.mkdocs.org/)
  - [Built-in themes](https://www.mkdocs.org/user-guide/choosing-your-theme/)
  - [Themes](https://github.com/mkdocs/mkdocs/wiki/MkDocs-Themes)
  - [Extensions](https://github.com/mkdocs/catalog)

## Template Structure

Generated with
[File Tree Generator](https://marketplace.visualstudio.com/items?itemName=Shinotatwu-DS.file-tree-generator)

```shell
📦template_python
 ┣ 📂.devcontainer
 ┃ ┗ 📜devcontainer.json
 ┣ 📂.github
 ┃ ┣ 📂DISCUSSION_TEMPLATE
 ┃ ┃ ┣ 📜implementations.yml
 ┃ ┃ ┗ 📜requests.yml
 ┃ ┣ 📂ISSUE_TEMPLATE
 ┃ ┃ ┗ 📜bug-report.yml
 ┃ ┣ 📂workflows
 ┃ ┃ ┣ 📜jekyll-gh-pages.yml
 ┃ ┃ ┗ 📜pre-commit.yml
 ┃ ┣ 📜CODEOWNERS
 ┃ ┣ 📜CODE_OF_CONDUCT.md
 ┃ ┣ 📜CONTRIBUTING.md
 ┃ ┣ 📜FUNDING.yml
 ┃ ┣ 📜GOVERNANCE.md
 ┃ ┣ 📜SECURITY.md
 ┃ ┗ 📜SUPPORT.md
 ┣ 📂docs
 ┃ ┗ 📜.gitkeep
 ┣ 📂src
 ┃ ┣ 📜__init__.py
 ┃ ┗ 📜main.py
 ┣ 📜.gitignore
 ┣ 📜.isort.cfg
 ┣ 📜.markdownlint.json
 ┣ 📜.mdformat.toml
 ┣ 📜.pre-commit-config.yaml
 ┣ 📜.rad.json
 ┣ 📜CITATION.cff
 ┣ 📜Dockerfile
 ┣ 📜LICENSE
 ┣ 📜Makefile
 ┣ 📜README.md
 ┣ 📜README.md.bk
 ┣ 📜poetry.lock
 ┣ 📜pyproject.toml
 ┗ 📜requirements.txt
```
