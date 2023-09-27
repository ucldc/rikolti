#!/usr/bin/python

import os
import glob
import importlib

get_ipython().run_line_magic("load_ext", "autoreload")  # noqa: F821
get_ipython().run_line_magic("alias_magic", "r %autoreload")  # noqa: F821

# import all modules in the current cwd excluding some
excludes = ["tests", "utilities", "settings", "__init__"]
paths = glob.glob(f"{os.getcwd()}/metadata_mapper/*.py")

for path in paths:
    if any([exclude in path for exclude in excludes]):
        continue

    module = os.path.basename(path)[:-3]

    locals()[module] = importlib.import_module(f"metadata_mapper.{module}")