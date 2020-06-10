#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
common.py

Conventions:

Style
- PEP8 style conventions with tabs vs spaces; 120 char max line length
- PEP257 style conventions
- All code lints clean without warnings or errors
- Debian Filesystem Hierarchy Standard (FHS) folder naming conventions where possible

Encoding
- Text files assumed to be UTF8 with universal newlines unless alternative encoding explicitly specified
- Encoding names should be uppercase without hyphens, eg. UTF8 vs utf-8
- Use text.encode('UTF8') vs bytes(text, 'UTF8')

Logging
- All modules make use of Python’s logging package
- Module loggers are named based on each module’s __name__
- Module loggers use the main script's logger, defaulting to a NullHandler if no global logger active

Paths
- All paths use the forward slash as a folder separator regardless of OS (this works for Win32 environ as well)
- Internally, paths without folder names are coerced to paths with trailing '/' path separators

Venv (virtualenv)
- Use virtualenv environments and pip for installing dependencies
- Virtualenv's ensure your code and dependencies are isolated from the system Python installation

"""


# standard lib
import datetime
import decimal
import fnmatch
import functools
import glob
import hashlib
import html
import json
import logging
import os
import pathlib
import pkg_resources
import platform
import re
import shutil
import socket
import sys
import types
import urllib.parse
import zipfile


# 3rd party lib
import dateutil.parser
import jsonpickle
import psutil
import pytz


# module level logger
logger = logging.getLogger(__name__)


# datetime operations ...


def duration(seconds):
    """Returns human readable time duration value."""
    seconds = int(seconds)
    if seconds > 60 * 60 * 24:
        duration_text = f"{seconds/(60*60*24):.1f} day(s)"
    elif seconds > 60 * 60:
        duration_text = f"{seconds/(60*60):.1f} hour(s)"
    elif seconds > 60:
        duration_text = f"{seconds/60:.1f} min(s)"
    else:
        duration_text = f"{seconds:.1f} sec(s)"
    return duration_text


def datetime_to_iso(timestamp):
    """Return timestamp (date, datetime) as an ISO8601 formatted value."""
    return timestamp.isoformat()


def iso_to_datetime(text):
    """Converts ISO 8601 formatted str to datetime value."""
    # dateutil ships with AWS BOTO3 and AWS CLI
    return dateutil.parser.parse(text)


def datetime_to_seconds(datetime_value):
    """Convert datetime to seconds since epoch."""
    return datetime.datetime.timestamp(datetime_value)


def seconds_to_datetime(seconds):
    """Convert seconds since epoch to datetime."""
    return datetime.datetime.fromtimestamp(seconds)


def to_datetime(text, default=None):
    """Convert text to datetime value. Return default value if conversion fails."""
    try:
        datetime_value = dateutil.parser.parse(text)
    except ValueError:
        datetime_value = default
    return datetime_value


def trim_seconds(datetime_value):
    """
    Trim seconds/microseconds from datetime by settings these attributes to 0.
    Use case: Normalize datetimes to HH:MM values.
    """
    return datetime_value.replace(second=0, microsecond=0)


# datetime with timezone operations ...


def now(timezone=None):
    """
    Return current datetime optionally converted to a timezone aware datetime value.

    Timezones should be specified via official Olson timezone names
    https://en.wikipedia.org/wiki/List_of_tz_database_time_zones

    # programmatic list of timezones using pytz timezone database
    list_of_timezones = pytz.all_timezones

    Use tz database name with Canonical values (vs. Alias or Deprecated values).
    Be specific where timezone nuances exist, eg. AZ, MI, eastern IN, etc.

    Common timezone names (not complete; intended to cover early project use-cases)

    Deprecated    Canonical (common US and CA timezones)
    UTC         > Etc/UTC
    US/Eastern  > America/New_York or America/Toronto
    US/Central  > America/Chicago or America/Winnipeg
    US/Mountain > America/Denver or America/Edmonton
    US/Pacific  > America/Los_Angles or America/Vancouver

    """
    if not timezone:
        return datetime.datetime.now()
    else:
        source_timezone = pytz.timezone(timezone)
        return datetime.datetime.now(source_timezone)


def to_timezone(source_datetime, source_tz, target_tz=None):
    """
    Convert source_datetime from source to target timezone.

    # example
    source_tz = 'America/Denver'
    target_tz = 'America/New_York'
    denver_time = now(source_tz)
    new_york_time = to_timezone(denver_time, source_tz, target_tz)
    print(f'{denver_time} {source_tz} = {new_york_time} {target_tz}')
    """

    source_tz = pytz.timezone(source_tz)
    source_datetime_with_tz = source_datetime.astimezone(source_tz)
    if not target_tz:
        return source_datetime_with_tz
    else:
        target_tz = pytz.timezone(target_tz)
        target_datetime_with_tz = source_datetime_with_tz.astimezone(target_tz)
        return target_datetime_with_tz


# decorators ...


def debug_log_function_return_value(func):
    # preserve information about the function/method being wrapped
    @functools.wraps(func)
    def debug_output(*args, **kwargs):
        value = func(*args, **kwargs)

        # build easy-to-read class.method name
        function_name = func.__name__

        # build easy-to-read description of parameters
        args_str = ""
        if args:
            args_str = str(args).strip("(),")
        kwargs_str = ""
        if kwargs:
            kwargs_str = str(kwargs)

        # parameter combinations
        if args_str and kwargs_str:
            parm_str = f"{args_str}, {kwargs_str}"
        elif args_str:
            parm_str = args_str
        elif kwargs_str:
            parm_str = kwargs_str
        else:
            parm_str = ""

        logger.debug(f"{function_name}({parm_str}) = {value}")
        return value

    return debug_output


def debug_log_method_return_value(func):
    # preserve information about the function/method being wrapped
    @functools.wraps(func)
    def debug_output(self, *args, **kwargs):
        value = func(self, *args, **kwargs)

        # build easy-to-read class.method name
        class_name = self.__class__.__name__
        if func.__name__ == "__call__":
            method_name = ""
        else:
            method_name = f".{func.__name__}"

        # look for an object name for context
        object_name = getattr(self, "name", "")
        if object_name:
            object_name = f"({object_name})"

        # build a call signature
        call_signature = f"{class_name}{object_name}{method_name}"

        # build easy-to-read description of parameters
        args_str = ""
        if args:
            args_str = str(args).strip("(),")
        kwargs_str = ""
        if kwargs:
            kwargs_str = str(kwargs)

        # parameter combinations
        if args_str and kwargs_str:
            parm_str = f"{args_str}, {kwargs_str}"
        elif args_str:
            parm_str = args_str
        elif kwargs_str:
            parm_str = kwargs_str
        else:
            parm_str = ""

        logger.debug(f"{call_signature}({parm_str}) = {value}")
        return value

    return debug_output


# file properties ...


def is_file(path_name):
    """Returns True if path references a file that exists."""
    return pathlib.Path(path_name).is_file()


def is_file_readonly(file_name):
    """Returns True if file exists and is read-only."""
    return is_file(file_name) and not os.access(file_name, os.W_OK)


def file_size(file_name):
    """Returns file's size in bytes."""
    return pathlib.Path(file_name).stat().st_size


def file_create_datetime(file_name):
    """Returns file's create time as a datetime value."""
    return seconds_to_datetime(pathlib.Path(file_name).stat().st_ctime)


def file_modify_datetime(file_name):
    """Returns file's last modified time as a datetime value."""
    return seconds_to_datetime(pathlib.Path(file_name).stat().st_mtime)


# file operations ...


def copy_file_if_exists(source_file_name, target_folder_name_or_file_name):
    """If source file exists, copy it to target; target can be a folder or file name."""
    try:
        # copy2() attempts to preserve file metadata during copy
        shutil.copy2(source_file_name, target_folder_name_or_file_name)
    except OSError:
        # if source file exists and target can't be created, propagate the exception, otherwise ignore it
        if is_file(source_file_name):
            raise
        else:
            pass


def delete_file(file_name, ignore_errors=False):
    """Delete file, optionally ignoring errors."""
    try:
        pathlib.Path(file_name).unlink()
    except OSError:
        if ignore_errors:
            pass
        else:
            raise


def delete_files(glob_pattern, ignore_errors=False):
    """Delete all files that match glob pattern."""
    for file_name in sorted(glob.glob(glob_pattern)):
        delete_file(file_name, ignore_errors)


def move_file(source_file_name, target_folder_name_or_file_name):
    """
    Move source file to target; target can be a folder or file name.
    Note: This is the same code as move_folder().
    """
    shutil.move(source_file_name, target_folder_name_or_file_name)


def rename_file(source_file_name, target_file_name):
    """
    Rename source file name to target file name.
    Note: This is the same code as rename_folder().
    """
    pathlib.Path(source_file_name).rename(target_file_name)


# folder operations ...


def is_folder(path_name):
    """Returns True if path references a folder that exists."""

    # Win32 raises an exception when path_name contains glob_pattern chars
    try:
        is_dir = pathlib.Path(path_name).is_dir()
    except OSError:
        is_dir = False
    return is_dir


def clear_folder(path_name):
    """Clear folder of all files and subdirectories."""

    # delete folder if it exists
    delete_folder(path_name)

    # recreate folder after deleting it
    create_folder(path_name)


def create_folder(path_name):
    """Create folder if it doesn't exist. Creates intermediate folders if passed multi-level path name."""
    path = pathlib.Path(path_name)
    path.mkdir(parents=True, exist_ok=True)


def delete_folder(path_name):
    """Delete folder if exists; deletes all all nested folders and files."""
    try:
        # rmtree removes a folder and all its contents including subfolders
        shutil.rmtree(path_name)
    except OSError:
        # if path exists and can't be deleted, propagate the exception, otherwise ignore it
        if is_folder(path_name):
            raise
        else:
            pass


def move_folder(source_folder_name, target_folder_name):
    """
    Move source folder to target.
    Note: This is the same code as move_file().
    """
    shutil.move(source_folder_name, target_folder_name)


def rename_folder(source_folder_name, target_folder_name):
    """
    Rename source folder name to target folder name.
    Note: This is the same code as rename_file().
    """
    pathlib.Path(source_folder_name).rename(target_folder_name)


# hash operations ...


# TODO: list potential hash_methods and the length of their values as hex strings


def read_chunks(file_handle, chunk_size=8192):
    """Return bytes in chunk sized units. Use for hashing large files."""
    while True:
        data = file_handle.read(chunk_size)
        if not data:
            break
        yield data


def hash_bytes(byte_str="", hash_method_name=None):
    """Return hashed value of bytes as hex str. Default hash method is sha256."""

    # default hash method is sha256
    if hash_method_name not in dir(hashlib):
        hash_method_name = "sha256"

    # get hash method pointer
    hash_method = getattr(hashlib, hash_method_name)

    # hashes only operate on bytestrings; .encode() without an encoding parm converts to bytestring
    return hash_method(byte_str).hexdigest()


def hash_str(text, hash_method_name=None):
    """Return hashed value of str as hex str. Default hash method is sha256."""

    # default hash method is sha256
    if hash_method_name not in dir(hashlib):
        hash_method_name = "sha256"

    # get hash method pointer
    hash_method = getattr(hashlib, hash_method_name)

    # hashes only operate on bytestrings; .encode() without an encoding parm converts to bytestring
    return hash_method(text.encode()).hexdigest()


def hash_file(file_name, hash_method_name=None):
    """Return hashed value of file as hex str. Default hash method is sha256."""

    # default hash method is sha256
    if hash_method_name not in dir(hashlib):
        hash_method_name = "sha256"

    # get hash method pointer
    hash_method = getattr(hashlib, hash_method_name)()

    # open file as binary since hashes only operate on bytestrings
    with open(file_name, "rb") as input_stream:
        for chunk in read_chunks(input_stream):
            hash_method.update(chunk)

    return hash_method.hexdigest()


def hash_files(glob_pattern, hash_method_name=None):
    """Return hashed value of files (ordered by file name) whose file names match a glob pattern."""
    file_hashes = []
    for file_name in sorted(glob.glob(glob_pattern)):
        file_hashes.append(hash_file(file_name, hash_method_name))
    return hash_str("".join(file_hashes), hash_method_name)


# host machine properties ...


def boot_datetime():
    """Returns system boot time as a datetime value."""
    return seconds_to_datetime(psutil.boot_time())


def diskspace_available(path_name="."):
    """Return available diskspace at specified path. Path defaults to space available at current directory."""
    return shutil.disk_usage(path_name)[2]


def diskspace_used(path_name="."):
    """Return diskspace used at specified path. Path defaults to space available at current directory."""
    return shutil.disk_usage(path_name)[1]


def memory_available():
    """Return memory available for use; not necessarily the amount of free memory."""
    return psutil.virtual_memory().available


def memory_total():
    """Return total 'physical' memory."""
    return psutil.virtual_memory().total


# int operations ...


def int_range(start, end, skip=None):
    """Return a range of integers from start to end inclusive, skipping optional skip value."""
    output = [num + start for num in range(end - start + 1)]
    if skip is not None:
        output = [num for num in output if num != skip]
    return output


def is_int(text):
    """Return True if text is a legal integer value."""
    try:
        int(text)
        is_valid = True
    except (ValueError, TypeError):
        is_valid = False
    return is_valid


def to_int(obj, default=None, strict=True):
    """Convert object to int value. Returns default if conversion fails. Raises exception if strict=True."""

    # pass through ints as-is
    if isinstance(obj, int):
        value = obj

    # convert strings to ints
    elif isinstance(obj, str):
        try:
            # convert to float first to handle values with decimal points
            value = int(float(obj))
        except ValueError:
            if strict:
                raise
            else:
                value = default

    # convert floats to ints
    elif isinstance(obj, float):
        value = int(obj)

    # convert dates and datetimes to int seconds since epoch
    elif isinstance(obj, (datetime.date, datetime.datetime)):
        value = int(datetime_to_seconds(obj))

    # convert items in sequences to ints
    elif is_sequence(obj):
        value = [to_int(item, default, strict) for item in obj]

    else:
        raise Exception(f"Unknown type ({type(obj)}")

    return value


# list operations ...


def delete_empty_entries(items):
    """Delete empty (None, '') items from items."""
    return [item for item in to_list(items) if not is_empty(item)]


def is_empty(obj):
    """Returns True if obj is None or empty string ('')."""
    return obj is None or (isinstance(obj, str) and not obj.strip())


def is_sequence(obj):
    """Returns True if object is a non-str/non-dict sequence, iterator, or generator."""
    return isinstance(obj, (list, tuple, set, types.GeneratorType))


def to_list(obj, delimiters=", "):
    """
    Convert object to list:
    - non str/non-dict sequences get converted to list
    - strings get split on delimiters and converted to list
    - atomic values get converted to a single element list
    - None gets converted to empty list
    """

    # ensure sequences are lists
    if is_sequence(obj):
        output = list(obj)

    # split strings into lists based on specified delimiters
    elif isinstance(obj, str):
        output = split(obj, delimiters)

    # place atomic objects into a single element list
    elif obj is not None:
        output = [obj]

    # treat None as an empty list
    else:
        output = []

    # remove empty strings ('') from generated list
    output = [item for item in output if item != ""]
    return output


# logging operations ...


def log_setup(log_file_name=None, log_level=logging.INFO):
    """Setup basic logging."""

    # FUTURE: See Peter Otten pyclbr and setLogRecordFactory technique to create %(className)s context.
    log_timestamp = "%Y-%m-%d %H:%M:%S"
    log_base_format = (
        "%(filename)s:%(lineno)d | %(funcName)s | %(levelname)s | %(message)s"
    )
    if log_file_name:
        log_format = f"%(asctime)s | {log_base_format}"
        logging.basicConfig(
            filename=log_file_name,
            format=log_format,
            datefmt=log_timestamp,
            level=log_level,
        )
    else:
        log_format = f"LOG: {log_base_format}"
        logging.basicConfig(
            stream=sys.stdout, format=log_format, datefmt=log_timestamp, level=log_level
        )


def log_session_info():
    """Output session info to log. Session info is static info about runtime environment."""

    # TODO: A local file that indicates the SDLC environ - set before we parse bootstrap.ini
    # NOTE: This SDLC environ value can't be an environ var because we may be running multiple SDLC
    #       environments on the same server. Unless: Each SDLC runs under its own user (service)
    #       account with its own SDLC specific SDLC set of environment vars ??? !!! The advantage
    #       of this technique: We can set prompt and colors for consoles at the user/environ level ???

    # TODO: Docker info
    docker_version = os.getenv("docker_version", "")
    docker_image_name = os.getenv("docker_image_name", "")
    docker_image_type = os.getenv("docker_type", "")
    docker_instance_id = os.getenv("docker_instance_id", "")

    # remove default python paths to highlight custom paths
    custom_path = [path for path in sys.path if "versions" not in path.lower()]
    logger.info(f"Script_name:  {script_name()}")
    logger.info(f"Script path:  {script_path()}")
    logger.info(f"Current dir:  {full_path(os.curdir)}")
    logger.info(f"Parameters:   {sys.argv[1:]}")
    logger.info(f"Process id:   {os.getpid()}")

    logger.info(f"User name:    {os.getlogin()}")
    logger.info(f"OS platform:  {platform.version()}")
    logger.info(f"Host name:    {platform.node()}")
    logger.info(f"Host IP:      {socket.gethostbyname(socket.gethostname())}")

    # cloud platform
    # config = ConfigSectionKey('conf', 'local')
    # config.load('bootstrap.ini')
    # config.load('init.ini')
    # environment = config('environment')
    # if environment:
    # 	logger.info(f'SDLC type:    {environment.sdlc_type}')
    # 	logger.info(f'SDLC name:    {environment.sdlc_type}')

    logger.info(f"Docker ver:   {docker_version}")
    logger.info(f"Docker name:  {docker_image_name}")
    logger.info(f"Docker id:    {docker_instance_id}")
    logger.info(f"Docker type:  {docker_image_type}")

    logger.info(f"Python ver:   {sys.version.splitlines()[0]}")
    logger.info(f"Python exe:   {sys.executable}")
    logger.info(f"Python path:  {custom_path}")

    logger.info(f"CPU count:    {os.cpu_count()}")
    logger.info(f"Disk used:    {diskspace_used():,} bytes")
    logger.info(f"Disk free:    {diskspace_available():,} bytes")
    logger.info(f"Boot time:    {boot_datetime()}\n")


# object operations ...


def describe(obj, attribute_names):
    """
    Return a description of object based on its class name and specific attributes.
    Note: attribute_names is a space and/or comma delimited string of attribute names to output w/associated values.
    Use case: Diagnostic output via __str__() methods, eg. describe(self, 'attribute, attribute, ...')
    """
    class_name = obj.__class__.__name__
    output = []
    for attribute_name in split(attribute_names):
        value = eval(f"obj.{attribute_name}")
        output.append(f"{attribute_name}={value}")
    return f'{class_name}: {"; ".join(output)}'


# path operations ...


def actual_file_name(file_name):
    """
    Returns the actual file name associated with a logical file name.
    Allows logical file names to be extended with an optional mnemonic @alias.

    Example: project_0001.ini and project_capture_amc_amp_sales.ini are considered logically
    equivalent if <path>/<file_stem>*.<file_ext> yields a single matching file name.

    Example: project_0001.ini will be returned as-is if a file by this name exists.

    Returns file name as '' if the count of matching files is 0 or > 1.

    See also: strip_file_alias().
    """

    # build a glob pattern to search for matching file name(s)
    base_file_name = strip_file_ext(file_name)
    file_ext = just_file_ext(file_name)
    glob_pattern = f"{base_file_name}*.{file_ext}"

    # find files that match a <path>/<file_stem>*.<file_ext> pattern
    file_name_aliases = glob.glob(glob_pattern)

    # no matches
    if len(file_name_aliases) == 0:
        return ""

    # one match - translate file name to its actual file name
    elif len(file_name_aliases) == 1:
        return file_name_aliases[0]

    # more than one match (not good)
    else:
        logger.warning(f"File aliased multiple times: {sorted(file_name_aliases)}")
        return ""


def force_file_ext(path_name, file_ext):
    """Replace path's original file extension with new file_ext."""

    # normalize file_ext to remove optional leading period
    file_ext = file_ext.strip(".")

    # strip off original file extension, then add new file extension
    return strip_file_ext(path_name) + f".{file_ext}"


def force_local_path(path_name):
    """
    Force path to a local path by stripping leading /, ./, ../ navigation.

    Use case: Normalizing target paths for zip archive files.
    Use case: Ensuring emulated blob storage paths don't step outside emulated storage layer.
    """
    char_position = 0
    for char_position, char in enumerate(path_name):
        if char not in "./":
            break
    return path_name[char_position:]


# formerly: path_with_separator(path_name)
def force_trailing_slash(path_name):
    """Returns path with trailing '/' path separator adding path separator if missing."""
    if not path_name.endswith("/"):
        path_name = path_name + "/"
    return path_name


def full_path(path_name):
    """Expand specified path to a full (absolute) path. Use to expand relative path values like '.' or '../...'"""
    return pathlib.Path(path_name).resolve()


def just_path(path_name):
    """Extracts path prefix from a path name with file name."""
    return os.path.dirname(path_name)


def just_file_name(path_name):
    """Extracts file name and extension from a path."""
    return pathlib.Path(path_name).name


def just_file_stem(path_name):
    """Extracts file name without extension from a path."""
    return pathlib.Path(path_name).stem


def just_file_ext(path_name):
    """Extracts file extension (suffix) from a path. Strips leading '.' from extension."""
    return pathlib.Path(path_name).suffix.strip(".")


def normalized_path(path_name):
    """
    Converts path_name to a normalized Linux-style path with forward slash path separators.
    Use case: For unit test scenarios where path strings need to be compared consistently.

    Note: There should be no need to use this function outside of unit test path comparisons.
    """

    # make sure path_name is a str vs pathlib Path object
    path_name = str(path_name)

    # normalize '\/' runs first, then convert backslashes to slashes
    backslash_char = chr(92)
    path_name = path_name.replace("{backslash_char}/", "/")
    path_name = path_name.replace(backslash_char, "/")

    # strip off Windows drive names (drive: format) for consistent paths across environments
    if ":" in path_name:
        # strip from the right-most colon
        path_name = path_name.rpartition(":")[2]

    return path_name


def parent_path(path_name):
    """Return parent path of a path."""
    return pathlib.Path(path_name).parent


def strip_file_ext(path_name):
    """Strips file extension (suffix) from path."""
    file_ext = just_file_ext(path_name)
    if not file_ext:
        return path_name
    else:
        # strip file extension
        return right_trim(path_name, f".{file_ext}")


def strip_file_alias(file_name):
    """
    Strip optional @alias from a file name.
    See also: actual_file_name().
    """
    file_ext = just_file_ext(file_name)
    base_file_name = strip_file_ext(file_name)
    base_file_name = base_file_name.partition("@")[0]
    if file_ext:
        return f"{base_file_name}.{file_ext}"
    else:
        return base_file_name


def strip_trailing_slash(path_name):
    """Strip path delimiters from the end of path name."""
    backslash_char = chr(92)
    return path_name.strip(backslash_char)


# process operations ...


"""
RSS used for process specific memory metrics to maintain compatibility with Task Manager metrics.

RSS: Resident Set Size - non-swapped physical memory used by a process 
- Linux/MacOS: matches tops' RES column
- Win32: matches 'Mem Usage' column of Task Manager (taskmgr.exe)

USS: Unique Set Size - memory unique to a process, which would be freed if the process was immediately terminated 
- USS represents the amount of memory that would be freed if the process was terminated right now
- USS is the most representative metric for determining how much memory is actually being used by a process

Ref: Win32  PROCESS_MEMORY_COUNTERS_EX
http://msdn.microsoft.com/en-us/library/windows/desktop/ms684874(v=vs.85).aspx

Ref: Linux top
http://linux.die.net/man/1/top
"""


def is_process(pid):
    """Returns True if pid corresponds to a running process."""
    return pid and psutil.pid_exists(pid)


def process_memory_used():
    """Return memory used by current process."""
    memory_info = psutil.Process().memory_full_info()

    # use RSS as basis for memory metric
    return memory_info.rss


def process_memory_percentage():
    """Return current process's memory utilization as a percentage; process memory / total 'physical' memory * 100."""

    # use RSS as basis for memory metric
    return psutil.Process().memory_percent(memtype="rss")


# script operations ...


def script_name():
    """Return the name of main executing script minus its path and extension."""
    return just_file_stem(sys.argv[0]).lower()


def script_path():
    """Return the path location of the main executing script."""
    return full_path(just_path(sys.argv[0]))


# serializer operations ...


def _json_serializer(obj):
    """Json serializer for objects not serializable by default json module."""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    elif isinstance(obj, decimal.Decimal):
        return float(obj)
    else:
        raise TypeError(f"Type {type(obj)} is not JSON serializable")


def import_json(file_name):
    """
    Return obj created by importing standard json data.

    Note: Date, datetime, and decimal values need to be decoded by caller.
    """
    with open(file_name) as input_stream:
        obj = json.load(input_stream)
    return obj


def export_json(file_name, obj):
    """
    Exports simple objects in a json format with extended support for datetime and decimal objects.
    Use case: Export data for one-way data exchange with databases, APIs, and Javascript libraries.
    """
    with open(file_name, "w") as output_stream:
        json.dump(obj, output_stream, indent=2, default=_json_serializer)


def load_jsonpickle(file_name):
    """
    Unpickles obj from a json formatted file; alternative to Python binary pickle format.
    Use case: Unpickle Python object from a human readable json formatted file.
    """
    with open(file_name) as input_stream:
        json_str = input_stream.read()

    # jsonpickle.decode() equivalent to pickle.loads()
    return jsonpickle.decode(json_str)


def save_jsonpickle(file_name, obj):
    """
    Pickles obj to file in a jsonpickle format; alternative to Python binary pickle format.
    Use case: Pickle Python object to a human readable json formatted file.

    Warning: jsonpickle sorts attributes; this will break ordered dicts.

    Use export_json() to preserve attribute order.
    """
    with open(file_name, "w") as output_stream:
        # jsonpickle.encode() equivalent to pickle.dumps()
        json_str = jsonpickle.encode(obj)
        output_stream.write(json_str)


def from_jsonpickle(json_str):
    """Unpickles obj from a json formatted str; alternative to Python binary pickle format."""
    return jsonpickle.decode(json_str)


def to_jsonpickle(obj):
    """Pickles obj to str in a jsonpickle format; alternative to Python binary pickle format."""
    # jsonpickle.encode() equivalent to pickle.dumps()
    return jsonpickle.encode(obj)


def load_lines(file_name, default="", line_count=1, encoding="UTF8"):
    """
    Return line_count lines from file as a block of text.
    A negative line_count returns lines from the end of the file.
    """

    # Note: default set to '' vs None so string processing operates as expected
    text = load_text(file_name, default=default, encoding=encoding)

    # return lines from bottom of file
    if line_count < 0:
        return "\n".join(text.splitlines()[line_count:])

    # no lines to return
    elif line_count == 0:
        return ""

    # return lines from top of file
    else:
        return "\n".join(text.splitlines()[:line_count])


def load_text(file_name, default=None, encoding="UTF8"):
    """Return the contents of a text file or default if file can not be opened."""
    try:
        with open(file_name, encoding=encoding) as input_stream:
            text = input_stream.read()

    # return default value if exception during file open
    except OSError as e:
        if is_file(file_name):
            logger.warning(
                f"Unable to open ({file_name}); error: ({e}); returning default ({default})"
            )
        else:
            logger.debug(
                f"File does not exist ({file_name}); returning default ({default})"
            )
        text = default

    return text


def save_text(file_name, text):
    """Save text to specified file."""
    with open(file_name, mode="w") as output_stream:
        output_stream.write(text)


# string operations (encoding/decoding) ....


def decode_uri(text):
    """Decode (unquote) UTF8 encoded bytes escaped with URI quoting. Use for URI's and some JSON values."""

    # https://stackoverflow.com/questions/16566069/url-decode-utf-8-in-python/32451970
    return urllib.parse.unquote(text)


def decode_entities(text):
    """Decode (unescape) &#... entities."""
    return html.unescape(text)


# string operations (cleanup) ...


def compress_char(text, char):
    """Compress runs of multiple chars to a single instance of char."""
    dupe_chars = f"{char}{char}"
    while dupe_chars in text:
        text = text.replace(dupe_chars, char)
    return text


def compress_whitespace(text, preserve_indent=False):
    """
    Strips trailing whitespace and condenses sequences of embedded whitespace to a single space.
    Preserves whitespace indentation if preserve_indent=True; otherwise strips leading whitespace as well.
    Treats tabs as spaces. Condenses consecutive blank lines to a single blank line.
    """

    indentation = ""
    output = list()
    for line in text.splitlines():
        # optionally preserve indentation
        if preserve_indent:
            indentation = get_indentation(line)
        line = " ".join(line.split())

        # skip consecutive blank lines
        if not line.strip() and output and not output[-1].strip():
            pass
        else:
            output.append(f"{indentation}{line}")

    return "\n".join(output)


def delete_blank_lines(text):
    """Delete blank lines from block of text."""
    output = []
    for line in text.splitlines():
        if line.strip():
            output.append(line)
    return "\n".join(output)


def get_indentation(text):
    """Return leading whitespace (space, tab) ("indentation") from text."""
    output = list()
    for char in text:
        if char in " \t":
            output.append(char)
        else:
            break
    return "".join(output)


def all_trim(text, item, case_sensitive=True):
    """Trim item from left side, then right side of text, trimming multiple instances of item if present."""
    text = left_trim(text, item, case_sensitive)
    text = right_trim(text, item, case_sensitive)
    return text


def left_trim(text, item, case_sensitive=True):
    """Trim item from left side of text, trimming multiple instances of item if present."""
    if case_sensitive:
        # case sensitive trim
        while item and text.startswith(item):
            text = text[len(item):]
    else:
        # non-case sensitive trim
        item = item.lower()
        while item and text.lower().startswith(item):
            text = text[len(item):]
    return text


def right_trim(text, item, case_sensitive=True):
    """Trim item from right side of text, trimming multiple instances of item if present."""
    if case_sensitive:
        # case sensitive trim
        while item and text.endswith(item):
            text = text[: -len(item)]
    else:
        # non-case sensitive trim
        item = item.lower()
        while item and text.lower().endswith(item):
            text = text[: -len(item)]
    return text


def strip_c_style_comments(text):
    """Strip C-style // comments and trailing whitespace."""
    output = list()
    for line in text.splitlines():
        line = line.partition("//")[0].rstrip()
        output.append(line)
    return "\n".join(output)


# string operations (expressions/formatting) ...


def expand(expression):
    """Expand non-inline f-string expressions from other sources like conf files, etc."""

    # reach up call stack to get caller's locals() dict

    # noinspection PyProtectedMember
    caller_locals = sys._getframe(1).f_locals
    triple_quote = "'" * 3
    return eval(f"f{triple_quote}{expression}{triple_quote}", None, caller_locals)


def make_fdqn(name):
    """Convert name to a FDQN compatible format, eg. lowercase, hyphen vs underscores."""
    return name.replace("_", "-").lower()


def make_key(*items, delimiter=":"):
    """
    Build a lowercase key from *items, delimited by delimiter.
    Use case: conf file section_type:section_name keys.
    Use case: generating natural keys for database merge operations.
    """
    return delimiter.join(items).lower()


def make_name(name, legal_symbols=""):
    """Convert name to a legal alpha-numeric value with optional punctuation chars."""

    # special case logic that maps hyphens to underscores and vice versa
    if legal_symbols == "-":
        name = name.replace("_", "-")
    if legal_symbols == "_":
        name = name.replace("-", "_")

    # remove all chars that aren't alpha-numeric or optional legal symbols
    return delete_regex_pattern(name, f"[^A-Za-z0-9{legal_symbols}]")


def quote(items):
    """
    Double quote each item in a list of items (strings).
    Use case: Quoting table and/or column names that may be reserved words in SQL snippets.
    """
    return [f'"{item}"' for item in items if not item.startswith('"')]


def spaces(n):
    """Returns a string of n-spaces; used to document intentional indentation in f-strings."""
    return " " * n


# string operations (parsing) ...


def get_lines(text, line_count):
    """Return line_count number of lines from the top or (using a negative line_count) bottom of text."""
    lines = text.splitlines()
    if line_count > 0:
        return lines[0:line_count]
    elif line_count < 0:
        return lines[line_count:]
    else:
        return [""]


def key_value(text):
    """Split text into a key=value pair. Keys are lower-cased and returned as empty strings if no assignment made."""
    key, delimiter, value = text.partition("=")

    # force empty key if no assignment made
    if key and not delimiter:
        key = ""
    else:
        # convert periods and hyphens in key names to underscores
        key = key.replace(".", "_")
        key = key.replace("-", "_")

        # remove all other non alphanumeric chars from key
        key = make_name(key, "_").lower()
        value = value.strip()

    return key, value


def option_value(text):
    """
    Split text into a -option/--option[=value] pair. Value defaults to '1' if no explicit assignment made.
    Options are lower-cased, hyphen-trimmed and returned as empty strings if not prefixed with '-' or '--'.
    """

    # make sure option starts with '--'
    if not text.startswith(("--",)):
        option = ""
        value = ""
    else:
        # strip leading '--'
        text = text.lstrip("-")

        # if no assignment, default value to '1'
        if "=" in text:
            option, value = key_value(text)
        else:
            option = text.lower()
            value = "1"

    return option, value


def split(items, delimiters=", ", strip=True):
    """
    Convert delimited text into a list of items.
    Returns original input if items is not a string.
    Strips whitespace from split items when strip=True.
    """

    # only split string based input; other input passes through as-is
    if isinstance(items, str):
        # normalize delimiters to a single delimiter
        normalized_delimiter = "\0"
        for delimiter in delimiters:
            items = items.replace(delimiter, normalized_delimiter)

        # compress runs of multiple delimiters to avoid creating empty list entries
        items = compress_char(items, normalized_delimiter)

        # strip leading and trailing normalized delimiters before splitting
        items = items.strip(normalized_delimiter)

        # only split items if items is non-empty
        # MMG: 2019-04-16 - make sure we return [] vs ['']
        if not items:
            items = list()
        else:
            # split on the normalized delimiter
            items = items.split(normalized_delimiter)

            # optionally strip whitespace from split items
            if strip:
                items = [item.strip() for item in items]

    return items


# string operations (regular expressions and glob patterns) ...


def case_insensitive_replace(text, old, new):
    """Case insensitive replace; case insensitively replace old with new in text."""
    regex_match = re.compile(re.escape(old), re.IGNORECASE)
    return regex_match.sub(new, text)


def delete_regex_pattern(text, regex_pattern):
    """Remove all instances of regex_pattern from text."""
    matches = re.findall(regex_pattern, text)
    if matches:
        for match in matches:
            text = text.replace(match, "")
    return text


def expand_template(text, key_value_dict, left_delimiter="{%", right_delimiter="%}"):
    """Replace {%key%} in text with non-case sensitive key=value lookups from key_value_dict."""
    for key in extract_matches(text, left_delimiter, right_delimiter):
        key = key.lower()
        key_match = f"{left_delimiter}{key}{right_delimiter}"
        if key in key_value_dict:
            value = str(key_value_dict[key])
        else:
            value = key_match

        text = case_insensitive_replace(text, key_match, value)

    return text


def extract_matches(text, left_delimiter, right_delimiter):
    """
    Return list of all delimiter based substrings in text.

    Note: Delimiters containing regex chars should be escaped if the raw value of the char(s)
    is the delimiter vs a delimiter intentionally specified as a regex expression.
    """
    return re.findall(f"{left_delimiter}(.+?){right_delimiter}", text)


def is_glob_match(glob_pattern, text):
    """Return true if text matches glob pattern."""
    return fnmatch.fnmatch(text.lower(), glob_pattern.lower())


def is_glob_pattern(text):
    """
    Returns True if text contains glob pattern chars.
    https://en.wikipedia.org/wiki/Glob_(programming)
    """

    # intentionally does not include '-' and '!' as glob pattern chars
    glob_pattern_chars = "[]*?"
    for char in text:
        if char in glob_pattern_chars:
            return True

    # no glob pattern chars detected
    return False


# type operations ...


def is_function(obj):
    """Return True if obj is a function or method."""
    # return isinstance(obj, (types.FunctionType, types.MethodType))
    logger.warning("Deprecated: replace is_function() with callable()")
    return callable(obj)


# xml/html operations ...


def get_attrs(element):
    """
    Return a dict of attr="value" assignments or None from a single XML element.
    Values have &#<number>; and &<symbol>; entities decoded.
    """
    pattern = '([a-zA-Z0-9_]+ *= *".*?")'
    matches = re.findall(pattern, element)
    if not matches:
        return None
    else:
        attrs = dict()
        for match in matches:
            name, delimiter, value = match.partition("=")
            name = name.strip()
            value = decode_entities(value.strip(' "'))
            attrs[name] = value
        return attrs


def get_tag(element):
    """Return a tag name or None from a single XML element."""
    backslash_char = chr(92)
    pattern = backslash_char + "<([a-zA-Z0-9_]+)[ />]"
    matches = re.findall(pattern, element)
    if matches:
        return matches[0]
    else:
        return None


# zip file operations ...


"""
Ref: zipfile
https://docs.python.org/3/library/zipfile.html
https://en.wikipedia.org/wiki/Zip_(file_format)

Archive file use cases:
- archive and compress/uncompress files for transmission/storage
- archive/compress code and configuration for application packaging

Tip: Use unzip command line utility to verify archive file contents. 
- MacOS Finder Uncompress option unzips archives to a folder with the name of the archive file.
- MacOS Terminal (console) unzip command line utility unzips archives as-is.  
"""


def load_resource(file_name, default="", encoding="utf8"):
    """
    Load file data from current *.app archive bundle.
    Set encoding=None to return binary values, default=b'<default>'.
    """
    try:
        file_handle = pkg_resources.resource_stream(__name__, file_name)
        data = file_handle.read()
        if encoding:
            data = data.decode(encoding)

    # trap file_name not found as a resource
    except OSError:
        data = default

    return data


class FileList:
    """
    Container for building lists of file names from include/exclude glob pattern rules.
    Registering an archive file at object creation makes include/exclude rules specific to archive's file collection.
    Use for building lists of files to add/extract from archives.

    Note: Include/exclude glob patterns are case sensitive under Linux/macOS.
    """

    def __init__(self, archive_file_name=None):
        """If archive file provided, include searches happen against archive's set of files."""
        self._file_names = set()

        self.archive_file_name = archive_file_name
        self.archive_files_names = list()
        if archive_file_name:
            with zipfile.ZipFile(archive_file_name) as archive_file:
                self.archive_file_names = archive_file.namelist()

    def include(self, glob_pattern):
        """Add files matching glob pattern to set of file names."""
        if not self.archive_file_name:
            # look for files on local file system
            include_file_names = glob.glob(glob_pattern)
        else:
            # look for files in registered archive file
            include_file_names = set()
            for file_name in self.archive_file_names:
                if fnmatch.fnmatch(file_name, glob_pattern):
                    include_file_names.add(file_name)

        self._file_names.update(include_file_names)

    def exclude(self, glob_pattern):
        """Remove files matching glob pattern from current set of file names."""

        # build set of files that match exclude pattern
        exclude_file_names = set()
        for file_name in self._file_names:
            if fnmatch.fnmatch(file_name, glob_pattern):
                exclude_file_names.add(file_name)

        # remove excluded files from current set of file names
        self._file_names = self._file_names - exclude_file_names

    def file_names(self):
        """Return sorted, de-duped list of file names built through multiple include/exclude steps."""
        return sorted(self._file_names)

    def __call__(self):
        """Return file_names() output."""
        return self.file_names()

    def __str__(self):
        """Format state for diagnostic output."""
        return str(self.file_names())


def _archive(archive_file_name, file_names, relative_path="", mode=""):
    """Common code for create/append archive."""

    # insure paths end with '/'
    relative_path = force_trailing_slash(relative_path)

    with zipfile.ZipFile(
        archive_file_name, mode, compression=zipfile.ZIP_DEFLATED
    ) as archive_file:
        for file_name in file_names:
            if relative_path and file_name.startswith(relative_path):
                # if file name starts with relative path, strip off relative path
                archive_file.write(file_name, file_name[len(relative_path):])
            else:
                # store file as its full source name
                archive_file.write(file_name)


def create_archive(archive_file_name, file_names, relative_path=""):
    """
    Create archive and fill with list of files.
    Strips optional relative path from file names when archiving files.
    Consider using FileList() to create file lists using glob patterns.
    """
    _archive(archive_file_name, file_names, relative_path, mode="w")


def append_archive(archive_file_name, file_names, relative_path=""):
    """
    Append existing archive with list of additional files.
    Strips optional relative path from file names when archiving files.
    Consider using FileList() to create file lists using glob patterns.
    """
    _archive(archive_file_name, file_names, relative_path, mode="a")


def extract_archive(archive_file_name, target_folder, file_names):
    """
    Extract list of files from archive to target folder.
    Consider using FileList() to create file lists using glob patterns.
    """

    # insure paths end with '/'
    target_folder = force_trailing_slash(target_folder)

    with zipfile.ZipFile(archive_file_name) as archive_file:
        for file_name in file_names:
            # make sure destination folder present
            folder_name = f"{target_folder}{just_path(file_name)}"
            if not is_folder(folder_name):
                create_folder(folder_name)

            # extract file
            with open(f"{target_folder}{file_name}", "wb") as output_stream:
                output_stream.write(archive_file.read(file_name))


def read_archived_file(archive_file_name, file_name, encoding="UTF8", default=None):
    """
    Return the contents of file from archive. Returns default if file not present in the archive.
    If encoding is None, return contents as bytes, otherwise decode contents based on specified encoding.
    """
    with zipfile.ZipFile(archive_file_name) as archive_file:
        if file_name not in archive_file.namelist():
            return default
        else:
            if encoding is None:
                # return raw bytes
                return archive_file.read(file_name)
            else:
                # decode returned contents
                return archive_file.read(file_name).decode(encoding=encoding)


# temp test harness ...


def test_archive():
    """Test archive code."""

    # test working folder
    test_folder_1 = "test_folder_1"
    test_folder_2 = "test_folder_2"

    file_list = FileList()
    file_list.include("conf/*")
    file_list.exclude("conf/*.sql")
    file_list.exclude("conf/*.tables")
    file_list.exclude("conf/*.cmd")
    file_list.exclude("conf/*.data")
    file_list.include("conf/postgresql.sql")
    file_list.include("app.py")
    file_list.include("missing-file.missing")
    logger.info(file_list)

    file_list = FileList("capture_publish/amc_heroku_amp_01_sales#000000034.zip")
    file_list.include("credit*")
    file_list.include("group*")
    file_list.exclude("*.table")
    logger.info(file_list)

    # create a new archive
    file_list = FileList()
    file_list.include("app.py")
    file_list.include("conf/*.project")
    file_list.exclude("conf/udp_*")
    logger.info(file_list)
    create_archive(f"{test_folder_1}/test_1.zip", file_list())

    # then append to it
    file_list = FileList()
    file_list.include("conf/*.sql")
    archive_file_name = f"{test_folder_1}/test_1.zip"
    append_archive(archive_file_name, file_list())

    # then extract files from it
    file_list = FileList(archive_file_name)
    file_list.include("*.py")
    file_list.include("conf/*.sql")
    extract_archive(archive_file_name, test_folder_2, file_list())

    # then read a file from it
    contents = read_archived_file(archive_file_name, "app.py")
    logger.info(f"\napp.py:\n{get_lines(contents, 10)}")
    contents = read_archived_file(archive_file_name, "missing.file", default="")
    logger.info(f"\nmissing.file:\n{get_lines(contents, 10)}\n")

    # test long paths
    file_list = FileList()
    source_folder_name = "../../malcolmgreene/test2/evercore"
    archive_file_name = f"{test_folder_1}/test_2.zip"
    file_list.include(f"{source_folder_name}/*.csv")
    file_list.include(f"{source_folder_name}/*.txt")
    logger.info(file_list)
    create_archive(archive_file_name, file_list(), source_folder_name)


def test_datetime():
    """Test datetime code."""
    pass


def test_expand_template():
    """Test expand_template() function."""

    key_value_dict = dict()
    key_value_dict["name"] = "Abe"
    key_value_dict["age"] = "99"
    key_value_dict["gender"] = "Male"

    text = "His name was {%Name%}. He was {%AGE%} years old. He was {%gender%}. This will be unmatched: {%bad-key%}."
    output = expand_template(text, key_value_dict)
    logger.info(f"expand_template({text}) = {output}")


def test_file():
    """Test file code."""
    # file_name = 'readonly.file'
    file_name = "test_row_hash.data"
    logger.info(f"\nTest file: {file_name}")
    logger.info(f"is_file(): {is_file(file_name)}")
    logger.info(f"is_file_readonly(): {is_file_readonly(file_name)}")
    logger.info(f"file_size(): {file_size(file_name):,}")
    logger.info(f"file_create_datetime(): {file_create_datetime(file_name)}")
    logger.info(f"file_modify_datetime(): {file_modify_datetime(file_name)}")


def test_file_alias_features():
    """Test file alias features."""

    # test actual file name

    # test_file_name = ''
    # logger.info(f'actual_file_name(conf/project_0001.ini, alias) = {actual_file_name(test_file_name)}')
    # logger.info(f'actual_file_name(conf/project_0002.ini) = {actual_file_name(test_file_name)}')
    # logger.info(f'actual_file_name(conf/project_0003.ini, no alias) = {actual_file_name(test_file_name)}')
    # logger.info(f'actual_file_name(conf/project_9999.ini, missing) = {actual_file_name(test_file_name)}')
    #
    # logger.info(f'strip_file_alias(file.ext) = {strip_file_alias("this_file.ext")}')
    # logger.info(f'strip_file_alias(this_file) = {strip_file_alias("this_file")}')
    # logger.info(f'strip_file_alias(this_file@alias.ext) = {strip_file_alias("this_file@alias.ext")}')


def test_int():
    """Test int functions."""
    text = "1 2 3 4 5.5"
    text_to_list = to_list(text)
    logger.info(f"to_int(text_to_list) = {to_int(text_to_list)}")


def test_make_key():
    """Test make_key()."""
    logger.info(
        f'make_key(A,b,c, delimiter="=") = {make_key("A", "b", "c", delimiter="-")}'
    )
    logger.info(f'make_key(A,b,c) = {make_key("A", "b", "c")}')
    logger.info(f'make_key(A,b) = {make_key("A", "b")}')
    logger.info(f'make_key(A) = {make_key("A")}')
    logger.info(f'make_key("") = {make_key("")}')


def test_string_cleanup():
    """Test string cleanup functions."""
    text = (
        "\nThis   is  a\t line\nThis is  another line\t\n\nBlank lines\n\n\n\nThe end\n"
    )
    logger.info(f"Before:\n{text}")
    logger.info(f"After compress_whitespace():\n{compress_whitespace(text)}#end")
    logger.info(f"After delete_blank_lines():\n{delete_blank_lines(text)}#end")

    text = "CatCatCat-Bat-Bat-CatCat"
    logger.info(f"Before: {text}")
    logger.info(f'After left_trim(): {left_trim(text, "Cat")}')
    logger.info(f'After right_trim(): {right_trim(text, "Cat")}')
    logger.info(f'After all_trim(): {all_trim(text, "Cat")}')
    logger.info(
        f'After left_trim(case_sensitive=False): {left_trim(text, "CAT", case_sensitive=False)}'
    )
    logger.info(
        f'After right_trim(case_sensitive=False): {right_trim(text, "cat", case_sensitive=False)}'
    )
    logger.info(
        f'After all_trim(case_sensitive=False): {all_trim(text, "CaT", case_sensitive=False)}'
    )
    logger.info(f'After all_trim(empty item): {all_trim(text, "")}')
    logger.info(f'After all_trim(empty text): {all_trim("", "Cat")}')


# temp test harness ...


# test code
def main():
    # place temp test code here
    test_archive()
    test_datetime()
    test_expand_template()
    test_file()
    test_file_alias_features()
    test_int()
    test_make_key()
    test_string_cleanup()


# test code
if __name__ == "__main__":
    log_session_info()
    log_setup()
    main()
