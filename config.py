#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
###

TODO: load config files can look into zip files as well as local folder paths

ConfigSectionKey(*paths) - look for config files in one or more paths, load_file() is file without a path
BUT: How do we specify zip files to be searched which may files saved with paths ????
conf.load_file('my_app.app:[path within zip???]', 'local')

When searching a zip file, we look for all instances of the file independent of leading path
or specify a folder prefix for namelist() to go against via a colon separator.

.load_file('my_app.app:conf/', 'local')


TODO: IMPORTANT:

Rename ConfigSectionKey and ConfigSection to better names.
- ConfigSectionKey --> Config() - dynamic config, intended for customization, single-lines w/scalar, dict, list values
- ConfigSection --> Resource() - static resource file supporting multi-line values w/indentation preservation

Then finish unifying the base class they both inherit from.

The only fundamental difference should be how load_file() handles [section] processing.
All other code should be the same.
Both classes should be able to read from zip archive files.
Both classes should be able to use load() to search across multiple paths.

Resource files (ConfigSection) should support optional {%section%} expressions
that allow a [section]\n<value> to be defined and reused.

Resource file parsing should track redefinitions using .is_new_section property.
Resource files should use to soon-to-be-renamed self.warning() and self.debug() methods
for reporting events.

@clear, @clone commands don't make sense because every [section] block
replaces the previous value of the block. These commands in base class
should default to warning('@command not supported')

@stop command makes sense for resource files as well.

Use a generic parser class as base class.
Future generic parser can support @include directives

Config and Resource can be separate libraries.

###

TODO: Cleanup self.warning() and self.debug() method names

These names overlap with logging method names.
We should have a better term for these reporting services.

Idea: parser_info, parser_warning ???

###

TODO: Move the following to generic or common code:

FindFiles should take an optional root path as well if using relative paths

FindFiles('conf', 'local') - with these being folders with source code in parent folder

FindFiles('conf', 'local', root_path='../') - when source moves to src or bin folder and
conf and local folders are peer folders with src or bin vs sub-folders.

FindFiles(path, path, ...)
load() --> find_file(file_name)



FindFiles() registers a list of paths and/or zip files/zip file internal folders
find_file() returns files that match as either:
- file handle - would seem to work for all scenarios
- block of text that caller splitlines()
- list of lines

is_secret() !!!!
- use in options module where secrets may be passed in via env/commandline
- use in config files where where key may be secret
- use in resource files where resource name may be secret
- use in database/api connection diagnostics where key=value may be secret

use: safe_key_value(key, value)
returns {key}={value} where {value} converted to @secret if key is a secret

use: safe_resource_value(resource, value)
Need a version for resource files that output their key/values as
[<resource>]\n<value>

###

Change load() to
- load_file(file_name, default_section_key, is_required=True)
- load_text(text, default_section, virtual_file_name)

Note: virtual_file_name for diagnostics.

load( ..., required=True); raises error if file is missing; set to False to ignore optional files

###

Test ability to read configs across multiple files
Where files may be
- on local file system folder(s)
- within archive (zip) files
- encrypted with *_e extension
- key/value with encrypted value, password = LOCKED:272sj2w8sl2218b

Optional warn if secrets stored in source unencrypted, eg. password, privatekey, etc
This assumes we have a format for values to indicate they're raw/naked secrets or encrypted secrets
password = <my-unencrypted-password> ---> password = PROTECTED$(a22outs9m)
When we encrypt a file, only keys that are secret's have their values protected

Secrets are encrypted/decrypted using the file name so that copying and pasting a PROTECTED$(...) value to
another config file will not work.

ConfigSectionKey.dump() that dumps entire collection of section[section_key] with info on where
OR: Can we get this info from standard log ??? or is doing this at normal log level too noisy ???
- each section first defined
- each key-value last overwritten

DONE:

- ConfigSectionKey (vs ConfigSection)
- ConfigSection (vs. ConfigSectionKey)
- Migrate *.cfg section parser to support SQL text blocks

###

config.py

A config object is a collection of section property blocks from one or more files.

- config objects are containers for one or more section property blocks
- all config access is relative to a section property block
- all sections have a section-type and an optional section-name
- sections with a type, but without a name build a default template for all sections of the specified type
- section-type:section-name sections inherit initial values from any (optional) section-type's that precede them

Inheritance example:

; a section-type (without section-name) that provides default property values for all sections of type table
[table]
cdc = timestamp
first_timestamp = 1900-01-01

; this section will the cdc and first_timestamp key=value
[table:customer]

Sections
- sections are containers for one or more key=value pairs
- sections are defined in sections.py
- sections have their property names (keys) validated by default
- sections can optionally have their _is_validated property set to False to receive any property (key) names

###

Features
- load config files from one or more folder paths allowing additional paths to overwrite defaults
- load config files can look into zip files as well as local folder paths
- load config files without [section]'s by specifying a default [section] name at load time
- section type and optional section name provide a 2-level config structure
- section blocks without name collect default values for inheritance
- section blocks with section type and section name inherit from [section-type] blocks
- section properties (key=value) are validated by default
- sections can receive unvalidated key=value assignments by section's ._is_validated = False, eg. bootstrap.ini
- default section names for ini files without sections, eg. bootstrap.ini
- lists of values via: key| = value
- dicts of values via: key|key_id = value
- diagnostic warnings on syntax problems with file name, line number, description
- diagnostic details on key/value source via @inheritance and @clone tags
- diagnostic details on updated [section] blocks and key/values via @updated tag
- diagnostic details on empty values (@empty) to support filtered logs
- diagnostic details on @not-validated keys (eg. bootstrap)
- secrets (passwords, private keys, etc) replaced in diagnostic output with @secret tags
- line (#, ;) and end-of-line (//) comment styles
- @clear
- @clone <section-type>:<section-name>
- @stop

###

API usage:
- use load() for multi-file inheritance across multiple paths including archive/zip files
- use load_file() to load a specific file with explicit path

###

Using keys that act as lists

Use case: Lists of datapool ids (formerly namespaces)
Use case: Lists of allow/block security rules
Use case: Lists of files (manifests) to include/exclude when building distribution packages (*.app files)

; the '|' appends <value>'s to the key's value
[access]
allow| = customers*
allow| = products*
allow| = transactions*

; value of key allow is:
['customers*', 'products*', 'transactions*']

###

Using keys that act as dicts

Use case: Un-sized PostgreSQL columns that don't follow our column sizing algorithms
Use case: Receiving flat files (from share drive, SFTP, API download, email) that lack table defs
Flat file examples: CSV, XML, JSON, delimited text, XLS, MDB
Note: JSON, XLS may carry type, but - like PostgreSQL - don't carry max char column widths

; the '|<key_id>' adds the value to the key's dict indexed by key_id
[file]
column|customer_name = varchar(24)
column|customer_comment = varchar(max)

; value of key column is:
{'customer_name':'varchar(24)', 'customer_comment':'varchar(max)'}

###

Clear multi-value dict/list values

; start with a multi-value key
[access]
allow| = table 1
allow| = table 2
allow| = table 3

; the current value of access.allow is now ...
access.allow = [table 1, table 2, table 3]

To clear this value:

allow| =

We treat the empty value as a request to clear the sequence vs add an empty item to a dict or list

###

Referencing previously defined section.key references via non-case sensitive {%section.key%} syntax.

Use case: Reference "bootstrap" config files generated by SDLC specific IAC scripts.

; {%<section>.<key>%} references work everywhere
archive_objectstore = {%bootstrap.sqs.archive_queue.name%}
capture_objectstore = aws-udp-s3-capture-amc-{%bootstrap.build.environment%}

###

Future: All config files should have their secrets encrypted and loaded from an encrypted data store.

Config file load order:
- local/bootstrap.ini
- local/init.ini
- conf/_connect.ini
- conf/<project>.ini
- local/<project>.ini

###

ConfigSectionKey log filters

Chain filters to narrow search to specific config files or exclude specific files or values.

# filter out @empty value noise
cat <log> | grep -v '@empty'

# filter out @empty and @inherit(-ed) values
cat <log> | grep -v '@empty' | grep -v '@inherit'

# view all warnings
cat <log> | grep 'WARNING'

# discover secrets (values will be replaced with @secret tags)
cat <log> | grep '@secret'

# trace a specific [section].key value
cat <log> | grep '[application].script'

# view all non-empty key=value settings from a specific config file
cat <log> | grep 'config_test.ini' | grep -v @empty

# trace a specific [section].key value within a specific config file
cat <log> | grep 'config_test.ini' | grep '[application].script'

"""


# standard lib
import copy
import logging


# common lib
from common import compress_whitespace
from common import expand_template
from common import get_indentation
from common import is_file
from common import is_glob_match
from common import key_value
from common import load_text
from common import log_setup
from common import log_session_info
from common import split
from common import strip_c_style_comments
from common import strip_trailing_slash


# section module is referenced indirectly by ConfigSectionKey.new_section()
# noinspection PyUnresolvedReferences
import section


# module level logger
logger = logging.getLogger(__name__)


class Config:

    def __init__(self, *path_names):
        """Initialize a config object; path_names is a prioritized list of paths to follow during load() requests."""
        self.comment_chars = ('#', ';')
        self.secret_substrings = split('cert key password private private_key pwd sas_token secret secure storage_key ssh')

        # strip path delimiters from the end of all paths
        self.path_names = list()
        for path_name in path_names:
            self.path_names.append(strip_trailing_slash(path_name))

        # conf-wide collections
        self.sections = dict()
        self.section_key_value = dict()

        # current section context
        self.current_section = None
        self.current_section_key = None
        self.is_section_new = True

        # current key_name (for multiline values)
        self.current_key_name = None

        # context of file/line being processed
        self.is_stopped = False
        self.file_name = ''
        self.line_number = 0

    def __call__(self, section_key):
        """Return section object based on specified section key or None if section_key undefined."""
        section_key = self.section_key(section_key)
        return self.sections.get(section_key, None)

    @staticmethod
    def is_command(line):
        """Return True if line is an @command [parms]."""
        return line.startswith('@')

    def is_comment(self, line):
        """Return True if line is a comment or a blank line."""
        return line.startswith(self.comment_chars) or not line

    def is_secret(self, key):
        """Return True if key contains a secret."""
        key = self.section_key(key)
        for secret_substring in self.secret_substrings:
            if secret_substring in key:
                return True
        else:
            return False

    @staticmethod
    def is_section(line):
        """Return True if line is a section definition."""
        line = line.strip()
        return line.startswith('[') and line.endswith(']')

    def keys(self, glob_pattern=None):
        """Return keys in config object, optionally filtered by glob_pattern."""
        if not glob_pattern:
            return self.sections.keys()
        else:
            return [key for key in self.sections.keys() if is_glob_match(glob_pattern, key)]

    # TODO: Load files from series of paths (1+) where paths can also be archive (zip) files
    def load(self, file_name, default_section_key=''):
        """Search across registered paths loading all instances of file in path order."""
        logger.info(f'Searching for {file_name} across: {self.path_names}')
        for path_name in self.path_names:
            load_file_name = f'{path_name}/{file_name}'
            if is_file(load_file_name):
                self.load_file(load_file_name, default_section_key)
            else:
                logger.debug(f'{file_name} not present in {path_name}')

    def load_file(self, file_name, default_section_key=''):
        """Override this method."""
        pass

    @staticmethod
    def section_key(section_key):
        """Normalize section_key for consistency."""
        return section_key.strip('[]: \t').lower()

    def warning(self, message):
        """Generate config file warnings with file name and line number references."""
        logger.warning(f'{self.file_name}({self.line_number}): {message}')


class ConfigSectionKey(Config):

    """An INI file is a container of section blocks differentiated by section-type[:section-name]."""

    def clear_section(self):
        """Clear all attributes of current section."""

        # reset section's current key name
        self.current_key_name = None

        # only clear a section if a section object is active
        if not self.current_section:
            logger.warning(f'No section active; @clear command ignored')
        else:
            logger.info(f'[{self.current_section_key}]@clear')

            # context of action
            context = f'@clear'

            # we're clearing at the key vs key_id level
            key_id = ''

            for key in self.current_section.__dict__.keys():
                # only clear public attributes
                if not key.startswith('_'):
                    # we use set_key_value so that dict and list type keys are properly cleared
                    self.set_key_value(key, key_id, '', context)

    def clone_section(self, cloned_section_key, context=''):
        """
        Copy specified section's key=value attributes to current section.
        Use case: Named section inheritance from section-type definition.
        Use case: @clone section-type:section-name.

        Note: Cloning replaces vs merges dict/list based keys.
        """

        # reset section's current key name
        self.current_key_name = None

        cloned_section_key = self.section_key(cloned_section_key)
        cloned_section_type = cloned_section_key.partition(':')[0]
        current_section_type = self.current_section_key.partition(':')[0]

        # cloned_section_key must exist in order to be cloned
        if cloned_section_key not in self.sections:
            self.warning(f'Section not found ({cloned_section_key}); @clone command ignored')

        # section types must match when cloning
        elif cloned_section_type != current_section_type:
            bad_types = f'(current: {current_section_type}, clone: {cloned_section_type})'
            self.warning(f'Section types do not match {bad_types}; @clone command ignored')

        else:
            cloned_section = copy.deepcopy(self.sections[cloned_section_key])
            self.sections[self.current_section_key] = cloned_section
            self.current_section = cloned_section
            self.current_section._section_key = self.current_section_key

            # context of clone
            if context:
                # context provided by inheritance or some other mechanism (future)
                pass
            else:
                context = f'@clone {cloned_section_key}'

            # track all the cloned values
            for key, value in self.current_section.__dict__.items():
                if not key.startswith('_'):
                    self.section_key_value[f'{self.current_section_key}.{key}'] = value
                    self.debug(key, value, context)

    def debug(self, key, value, context=''):
        """Generate diagnostics with file name and line number references."""

        # tag empty values so empty values can be filtered out of logs (less noise)
        if not value:
            value = '@empty'

        # tag and hide secret values like passwords and private keys
        elif self.is_secret(key):
            value = '@secret'

        # tag initial-value vs updated-value
        # TODO: Track set key's in a set by section_key.key (minus key_id)
        if self.is_section_new:
            if not context:
                context = '@initial-value'
        else:
            if context:
                context = f'@updated-value, {context}'
            else:
                context = '@updated-value'

        # tag non-validated keys
        if not self.current_section.is_validated():
            if context:
                context = f'{context}, @not-validated'
            else:
                context = '@not-validated'

        if context:
            logger.debug(f'{self.file_name}({self.line_number}): [{self.current_section_key}].{key} = {value}; {context}')
        else:
            logger.debug(f'{self.file_name}({self.line_number}): [{self.current_section_key}].{key} = {value}')

    def do_command(self, line):
        """Execute @command [parameter]"""

        # parse command line into command and optional parameter
        command, delimiter, parameter = line.partition(' ')
        command = command.lstrip('@').lower()
        parameter = parameter.strip()

        logger.info(f'{self.file_name}({self.line_number}): @{command} {parameter}')

        # route command
        if command == 'clear':
            self.clear_section()
        elif command == 'clone':
            self.clone_section(parameter)
        elif command == 'stop':
            self.stop_command()
        else:
            self.warning(f'Unknown @command ({line})')

    def load_file(self, file_name, default_section_key=''):
        """Load a configuration file. Parse into sections indexed by section_type[:section_name]."""

        # reset parse status variables
        self.current_section = None
        self.current_section_key = None
        self.is_stopped = False
        self.file_name = file_name
        self.line_number = 0

        # provide file name context for debug output
        logger.info(f'ConfigSectionKey.load_file({file_name})')

        # load default section if passed in as default_section_key, eg. for ini files without sections
        if default_section_key:
            logger.info(f'Using default section ({default_section_key})')
            self.load_section(default_section_key)

        lines = load_text(file_name, '').splitlines()
        for self.line_number, line in enumerate(lines, 1):
            # exit if we entered a stop condition
            if self.is_stopped:
                break

            # prep line for parsing
            indentation = get_indentation(line)
            is_indented = len(indentation)
            line = compress_whitespace(line)
            line = strip_c_style_comments(line)

            if is_indented and self.current_section:
                if not self.current_key_name:
                    self.warning('No current key to append indented line to; line ignored')
                else:
                    current_value = getattr(self.current_section, self.current_key_name)
                    new_value = f'{current_value}\n{indentation}{line}'
                    setattr(self.current_section, self.current_key_name, new_value)

            # process section definitions
            elif self.is_section(line):
                self.load_section(line)
                if not self.current_section:
                    self.warning(f'Undefined section ({self.current_section_key})')

            # process comments and blank lines
            elif self.is_comment(line):
                # ignore comment lines and blank lines
                pass

            # process @commands
            elif self.is_command(line):
                self.do_command(line)

            # process key=value assignments
            else:
                key, value = key_value(line)

                # remember current key name
                self.current_key_name = key

                # split keys at '|' to handle keys acting as dicts or lists
                # key|key_id = value (treat key as a dict indexed by key_id)
                # key| = value (treat key as a list)
                key, delimiter, key_id = key.partition('|')

                if not self.current_section:
                    self.warning(f'In undefined section ({self.current_section_key}); line ignored')
                elif not key:
                    self.warning(f'Syntax error; no key-value assignment ({line})')

                # if section is validated and the key isn't present, then warn
                elif self.current_section.is_validated() and not hasattr(self.current_section, key):
                    self.warning(f'Unknown property ({key}) in section {self.current_section_key}')
                else:
                    # update the value
                    self.set_key_value(key, key_id, value)

    def load_section(self, line):
        """Load current_section reference to section object specified in line's [section] value."""
        section_key = self.section_key(line)

        # reset section's current key name
        self.current_key_name = None

        # load an existing section object
        if section_key in self.sections:
            self.is_section_new = False
            logger.debug(f'{self.file_name}({self.line_number}): [{section_key}] @update')
            self.current_section = self.sections[section_key]

        # create a new section object or set to None if section_key is invalid
        else:
            self.is_section_new = True
            logger.debug(f'{self.file_name}({self.line_number}): [{section_key}]')
            self.new_section(section_key)

        # track section name for diagnostics even if new_section fails (.current_section=None)
        self.current_section_key = section_key

    def new_section(self, section_key):
        """Create a new section object based on section type and optional section name."""

        # reset current key name
        self.current_key_name = None

        try:
            self.current_section_key = section_key

            # inherit from section type defaults only if this section is not a default section itself
            section_type = section_key.partition(':')[0]
            if section_type in self.sections and section_type != section_key:
                # inherit from section type default settings if present
                self.clone_section(section_type, context=f'@inherit from {section_type}')
            else:
                # create new section object without inheritance from default settings
                section_class = f'section.Section{section_type.title()}(section_key)'
                section_object = eval(section_class)
                self.sections[section_key] = section_object
                self.current_section = section_object

        # section type not defined
        except (NameError, AttributeError):
            # clear current section object so we ignore key=value statements in this section
            self.current_section = None

    def set_key_value(self, key, key_id, value, context=''):
        """Set key=value in context of current section."""

        # make sure key exists (non-validated sections do not have predefined keys)
        if not hasattr(self.current_section, key):
            setattr(self.current_section, key, '')

        # get key type to determine how we handle value assignment
        key_type = type(getattr(self.current_section, key))

        # expand {%section.key%} references
        left_delimiter = '{%'
        right_delimiter = '%}'
        value = expand_template(value, self.section_key_value, left_delimiter, right_delimiter)

        # standard str based key=value
        if key_type == str:
            setattr(self.current_section, key, value)
            self.debug(key, value, context)
            current_value = value

        # dict based keys (key|key_id = value)
        elif key_type == dict:
            key_attribute = getattr(self.current_section, key)
            if value:
                key_attribute[key_id] = value
            else:
                # clear key's dict when set to an empty value
                key_attribute.clear()

            self.debug(f'{key}|{key_id}', value, context)
            current_value = key_attribute

        # list based keys (key| = value)
        elif key_type == list:
            key_attribute = getattr(self.current_section, key)
            if value:
                key_attribute.append(value)
            else:
                # clear key's list when set to an empty value
                key_attribute.clear()

            self.debug(f'{key}|', value, context)
            current_value = key_attribute

        # unexpected key type (should not happen)
        else:
            raise Exception(f'Unexpected type ({key_type} for key ({key}')

        # update dictionary of section key values with latest value for section.key
        self.section_key_value[f'{self.current_section_key}.{key}'] = current_value

    def stop_command(self):
        self.is_stopped = True
        logger.info(f'{self.file_name}({self.line_number}): @stop; ignoring remainder of file')


class ConfigSection(Config):

    """Capture values at the [section] vs [section].property = value level."""

    # TODO: See other error() method TODO note.
    def error(self, line_number, message):
        print(f'Warning: {self.file_name}[{line_number}]: {message}')

    def load_file(self, file_name, default_section_key=''):
        # reset parse status variables
        self.current_section = None
        self.current_section_key = None
        self.is_stopped = False
        self.file_name = file_name
        self.line_number = 0

        # provide file name context for debug output
        logger.info(f'ConfigSection.load_file({file_name})')

        # load default section if passed in as default_section_key, eg. for ini files without sections
        if default_section_key:
            logger.debug(f'Using default section ({default_section_key})')
            self.current_section_key = self.section_key(default_section_key)
            self.sections[self.current_section_key] = ''

        lines = load_text(file_name, '').splitlines()
        for self.line_number, line in enumerate(lines, 1):
            # exit if we entered a stop condition
            if self.is_stopped:
                break

            # prep line for parsing
            line = compress_whitespace(line, preserve_indent=True)
            line = strip_c_style_comments(line)

            # skip comment lines, but pass blank lines through as data
            if self.is_comment(line) and line:
                continue

            # start a new section
            elif self.is_section(line):
                self.current_section_key = self.section_key(line)
                self.sections[self.current_section_key] = ''

            # add line to section's value
            elif self.current_section_key:
                # TODO: Expand {%expressions%}
                self.sections[self.current_section_key] += '\n' + line

            # non-blank lines without a section name are treated as errors
            elif line:
                self.warning('Unexpected line outside of defined section; line ignored')

        # strip leading and trailing whitespace from values
        # Note: This does not affect indented values.
        for key in self.sections:
            self.sections[key] = self.sections[key].strip()

    def dump(self):
        logger.debug(f'File: {self.file_name}\n')
        for key, value in self.sections.items():
            logger.debug(f'[{key}]\n{value}\n')


# temp test harness ...


# test code
def test_config_section_key():

    config = ConfigSectionKey('conf', 'local')
    config.load('project_archive.ini')
    config('project').dump()
    config('datapool').dump()

    config = ConfigSectionKey('conf', 'local')
    config.load('project_capture_amc_amp_sales.ini')
    config('table:product_catalogs').dump(False)

    config = ConfigSectionKey('conf', 'local')
    config.load('datapool.ini')
    config('datapools').dump()
    logger.debug(f'Value of [datapools].datapool[9001] = {config("datapools").datapool["9001"]}\n')

    config = ConfigSectionKey('conf', 'local')
    config.load('access.ini')
    config('access:finance').dump()
    logger.debug(config('access:marketing').allow)
    logger.debug(config('access:marketing').block)

    config = ConfigSectionKey('conf', 'local')
    config.load('bootstrap.ini', 'bootstrap')
    config.load('init.ini')
    config.load('connect.ini')
    config('cloud:amc_aws_capture_01_etl').dump()


def test_config_section():
    config = ConfigSection('conf', 'local')
    config.load_file('conf/mssql.cfg')
    config.load('mssql.cfg')

    config.dump()
    # logger.info(f'{config("insert_into_table")}')


# test
def main():
    # test_config_section_key()
    test_config_section()


# test code
if __name__ == '__main__':
    log_setup()
    log_session_info()
    main()
