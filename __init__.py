import pkgutil
import importlib

__all__ = []

def _import_modules():
    import os
    import sys

    # Get the path of the current directory (modules package)
    current_dir = os.path.dirname(__file__)
    for importer, module_name, is_pkg in pkgutil.iter_modules([current_dir]):
        if is_pkg:
            continue
        __all__.append(module_name)
        module = importlib.import_module(f'{__name__}.{module_name}')
        globals()[module_name] = module

_import_modules()
