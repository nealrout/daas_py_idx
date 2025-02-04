# daas_py_idx_asset
## Project

Refrence of DaaS Project - https://github.com/nealrout/daas_docs

## Description

Index manager to pull asset details from database and update SOLR

## Table of Contents

- [Requirements](#requirements)
- [Uninstall-Install](#uninstall-install)
- [Usage](#usage)
- [Package](#package)
- [Features](#features)
- [Miscellaneous](#miscellaneous)
- [Contact](#contact)

## Requirements
__Set .env variables for configuration__  

ENV_FOR_DYNACONF=\<environment\>  
_i.e. development, integration, production_  

DYNACONF_SECRET_KEY=\<secret_key\>

## Uninstall-Install
__Uninstall:__  
python -m pip uninstall daas_py_idx_asset

__Install:__  
python -m pip install .

__Rebuild from source:__  
python -m pip install --no-binary :all: .

## Usage
__Set correct directory:__  


## Package
python setup.py sdist

## Features
- 

## Miscellaneous

### To create new virtual environment  
python -m venv myenv

### To activate the virtual environment for this project
..\.venv\Scripts\activate

## Contact
Neal Routson  
nroutson@gmail.com
