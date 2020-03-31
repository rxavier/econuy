import logging
import random
import functools
import inspect
from string import ascii_letters
from pathlib import Path
from typing import Optional


def setup(file: Optional[str] = None, null: bool = False):
    """Setup logging."""
    name = ''.join(random.choice(ascii_letters) for i in range(10))
    log_obj = logging.getLogger(name)

    if null is True:
        log_obj.addHandler(logging.NullHandler())
        return log_obj

    log_obj.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(message)s',
                                  datefmt="%d-%b %T")
    ch.setFormatter(formatter)
    log_obj.addHandler(ch)

    if file is not None:
        file_ext = Path(file).with_suffix(".log")
        fh = logging.FileHandler(file_ext)
        fh.setFormatter(formatter)
        log_obj.addHandler(fh)

    return log_obj


def log_getter(func):
    """
    Decorator for :meth:`~econuy.session.Session.get` and
    :meth:`~econuy.session.Session.get_frequent`.

    """
    @functools.wraps(func)
    def wrapped(self, *args, **kwargs):
        name = func.__name__
        self.logger.info(f"Getting '{kwargs['dataset']}' "
                         f"with the '{name}' method.")
        return func(self, *args, **kwargs)
    return wrapped


def log_transformer(func):
    """
    Decorator for transformation methods in
    :class:`~econuy.session.Session`.

    """
    @functools.wraps(func)
    def wrapped(self, *func_args, **func_kwargs):
        name = func.__name__
        inspection = inspect.getfullargspec(func)
        kwarg_defaults = dict(zip(inspection[0][-len(inspection[3]):],
                                  inspection[3]))
        if func_kwargs:
            params_kwargs = [(k, v) for k, v in func_kwargs.items()]
            kwarg_defaults.update(params_kwargs)
        keywords = ", ".join(f"{k}={v}" for k, v in kwarg_defaults.items())
        if func_args:
            keywords = ", ".join(func_args) + ", " + keywords
        self.logger.info(f"Applying '{name}' transformation with the following"
                         f" parameters:\n{keywords}")
        return func(self, *func_args, **func_kwargs)
    return wrapped


def get_called_args(func, keywords):
    """
    Helper function for getting arguments passed to
    :meth:`~econuy.session.Session.get_frequent`

    """
    inspection = inspect.getfullargspec(func)
    kwarg_defaults = dict(zip(inspection[0][-len(inspection[3]):],
                              inspection[3]))
    if keywords:
        params_kwargs = [(k, v) for k, v in keywords.items()]
        kwarg_defaults.update(params_kwargs)
    return ", ".join(f"{k}={v}" for k, v in kwarg_defaults.items())
