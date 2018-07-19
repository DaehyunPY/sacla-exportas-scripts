#!/usr/bin/env python3
"""
Preanalyzing daemon.
"""
# %% import external dependencies
from glob import glob
from os import remove, makedirs
from os.path import dirname, join, basename, splitext, exists, relpath, getmtime
from typing import List, Set, Mapping
from time import sleep
from datetime import datetime, timedelta
from threading import Thread, active_count
from itertools import groupby
from functools import partial
from subprocess import call

from importlib_resources import path


__all__ = ['run']


# %% parameters
maxworkers = 4
startinterval = 30


def workingdir(key: str) -> str:
    """
    Working dir where a preanalyzing process works.
    """
    return f'/path/to/{key}'


def keypatt(lmafilename: str) -> str:
    """
    A rule (pattern) getting keys from lma filenames, where key is unique str indentifying lma file groups.
    In other words, files having the same key belong to the same group. For example, if we define the function as...

    keypatt(lmafilename: str) -> str:
        from os.path import basename
        key, _ = basename(lmafilename).rsplit('__', maxsplit=1)
        return key
    
    then, these two files 'aq001__0000.lma' and 'aq001__0001.lma' have the same key 'aq001'; they will be preanalyzed
    as the same lma group.
    """
    key, _ = splitext(basename(lmafilename))
    return key


def targetlist() -> List[str]:
    """
    Target lma file list.
    """
    return glob(f'/path/to/*.parquet')


# %%
def currentkeys() -> Mapping[str, float]:
    """
    Current keys (lma file groups) have to be preanalyzed and their last modifed timestamp.
    Do not return keys which already have been analyzed.
    """
    return  {k: max(getmtime(f) for f in groupped)
             for k, groupped in groupby(targetlist(), keypatt) if not exists(workingdir(k))}


def todolist() -> Set[str]:
    print(f"[{datetime.now()}] Scanning parquet files...")
    lastkeys = currentkeys()
    lastchecked = datetime.now()
    while True:
        if datetime.now() < lastchecked + timedelta(seconds=startinterval):
            sleep(startinterval)
            continue
        print(f"[{datetime.now()}] Scanning new parquet files...")
        curr = currentkeys()
        lastchecked = datetime.now()
        yield sorted(k for k in curr if k in lastkeys and curr[k] <= lastkeys[k])
        lastkeys = curr


def islocked(key: str) -> bool:
    wdir = workingdir(key)
    locker = f'{wdir}.locked'
    return exists(locker)


def work(key: str) -> None:
    wdir = workingdir(key)
    locker = f'{wdir}.locked'
    print(f"[{datetime.now()}] Working on key '{key}'...")
    with open(locker, 'w'):
        makedirs(wdir)
        with open(join(wdir, 'job.sh'), 'w') as f:

            f.write(dedent(
                f"""\
                #!/bin/bash
                docker run -ti --rm \
                    -v $(pwd):$(pwd) \
                    -v /Users:/Users \
                    daehyunpy/sp8-delayline $(realpath exportas.py) \
                        {' '.join([f for f in targetlist() if keypatt(f)==key])} \
                        -o {join(wdir, f'{key}.root')}
                """
            ))
        call(join(wdir, 'job.sh'), stdout=join(wdir, 'log.out'), stderr=join(wdir, 'log.err'), cwd='')
    remove(locker)


# %% inf loop
def run() -> None:
    for jobs in todolist():
        for key in jobs:
            if not islocked(key) and active_count()-1 < maxworkers:
                job = Thread(target=work, args=[key])
                job.start()
