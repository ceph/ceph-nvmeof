#!/bin/bash

pytest unittests | tee pytest.log
rm -rf .pytest_cache
rm -rf test/__pycache__
