#!/usr/bin/env python

"""
File metadata consistency scanner.

Python requirements: Python 2.7, :mod:`psycopg2`, Enstore modules.
"""

# Python imports
from __future__ import division, print_function, unicode_literals
import atexit
import ConfigParser
import copy
import ctypes
import datetime
import errno
import fcntl
import functools
import grp
import hashlib
import inspect
import locale
import itertools
import json
import math
import multiprocessing
import optparse
import os
import pwd
import Queue
import random
import stat
import sys
import threading
import time

# Chimera and Enstore imports
import chimera
import checksum as enstore_checksum
import configuration_client as enstore_configuration_client
import e_errors as enstore_errors
import enstore_constants
import enstore_functions3
import info_client as enstore_info_client
import namespace as enstore_namespace
import volume_family as enstore_volume_family

# Other imports
import psycopg2.extras  # This imports psycopg2 as well. @UnresolvedImport

# Setup global environment
locale.setlocale(locale.LC_ALL, '')
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

# Specify settings
settings = {
'cache_volume_info': False, # There is no speedup if this is True, and there is
                            # potentially a gradual slow-down.
                            # If True, ensure sys.version_info >= (2, 7).
                            # See MPSubDictCache.__init__
'checkpoint_max_age': 60,  # (days)
'checkpoint_write_interval': 5,  # (seconds)
'fs_root': '/pnfs/fs/usr',
#'fs_root': '/pnfs/fs/usr/astro/fulla',  # for quick test
#'fs_root': '/pnfs/fs/usr/astro/fulla/BACKUP',  # for quicker test
'num_scan_processes_per_cpu': 3,
'scriptname_root': os.path.splitext(os.path.basename(__file__))[0],
'sleep_time_at_exit': 0.01,  # (seconds)
'status_interval': 600,  # (seconds)
}


class Memoize(object):
    """
    Cache the return value of a method.

    This class is meant to be used as a decorator of methods. The return value
    from a given method invocation will be cached on the instance whose method
    was invoked. All arguments passed to a method decorated with memoize must
    be hashable.

    If a memoized method is invoked directly on its class the result will not
    be cached. Instead the method will be invoked like a static method::

        class Obj(object):
            @memoize
            def add_to(self, arg):
                return self + arg

        Obj.add_to(1) # not enough arguments
        Obj.add_to(1, 2) # returns 3, result is not cached

    .. note :: This class is derived from
               http://code.activestate.com/recipes/577452/history/1/.

    .. warning :: This class should not be used directly, as doing so can
        prevent Sphinx from documenting the decorated method correctly. Use the
        :func:`memoize` or :func:`memoize_property` decorator instead.
    """

    def __init__(self, func):
        self.func = func
        #self.__name__ = self.func.__name__  # For Sphinx.
        #self.__doc__ = self.func.__doc__  # For Sphinx.

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self.func
        return functools.partial(self, obj)

    def __call__(self, *args, **kw):
        obj = args[0]
        try:
            cache = obj.__cache
        except AttributeError:
            cache = obj.__cache = {}
        key = (self.func, args[1:], frozenset(kw.items()))
        try:
            res = cache[key]
        except KeyError:
            res = cache[key] = self.func(*args, **kw)
        return res


def memoize(f):
    """
    Return a memoization decorator for methods of classes.

    This wraps the :class:`Memoize` class using :py:func:`functools.wraps`.
    This allows the decorated method to be documented correctly by Sphinx.

    .. note :: This function is derived from
               http://stackoverflow.com/a/6394966/832230.
    """

    memoized = Memoize(f)

    @functools.wraps(f)
    def helper(*args, **kws):
        return memoized(*args, **kws)

    return helper

def memoize_property(f):
    """
    Return a memoization decorator for methods of classes, with it being usable
    as a :obj:`property`.

    This uses the :func:`memoize` function.
    """
    return property(memoize(f))

def do_random_test(negexp=3):
    """
    Return :obj:`True` if a random probability is not greater than a threshold,
    otherwise return :obj:`False`.

    :type negexp: :obj:`int` (non-negative)
    :arg negexp: This is the negative exponent that is used to compute a
        probability threshold. Higher values of ``negexp`` make the threshold
        exponentially smaller. A value of 0 naturally makes the threshold equal
        1, in which case the returned value will be :obj:`True`.
    :rtype: :obj:`bool`
    """
    # Compare random probability with threshold 1e-negexp
    return random.random() <= 10**(-1*negexp)

class PeriodicRunner:
    "Run a callable periodically."

    _concurrency_classes = {'thread': threading.Thread,
                            'process': multiprocessing.Process,}

    def __init__(self, is_active_indicator, target, interval, concurrency,
                 name=None):
        """
        Run the ``target`` callable periodically with the specified idle
        ``interval`` in seconds between each run.

        :type target: :obj:`callable`
        :arg target: This is a callable which is to be run periodically. Only
            one instance of the target is called and run at a time. Even so,
            the target should be thread or process safe, depending upon the
            indicated ``concurrency`` type. The target is also run once upon
            program or thread termination.

        :type is_active_indicator: :obj:`multiprocessing.Value` or
            :obj:`multiprocessing.RawValue`
        :arg is_active_indicator: This must have an attribute ``value`` which
            is evaluated as a :obj:`bool`. The target is called only so long as
            ``is_active_indicator.value`` evaluates to :obj:`True`. If and when
            this indicator evaluates to :obj:`False`, the loop is terminated,
            although the target is then still run one final time.

        :type interval: :obj:`int` or :obj:`float` (positive)
        :arg interval: number of seconds to sleep between each run of the
            target.

        :type concurrency: :obj:`str`
        :arg concurrency: This can be ``thread`` or ``process``, indicating
            whether the target should run in a new thread or a new process.

        :type name: :obj:`str` or :obj:`None`
        :arg name: This is the name assigned to the target thread or process.
            If :obj:`None`, it is determined automatically.
        """

        # Setup variables
        self._is_active = is_active_indicator
        self._target = target
        self._interval = interval
        self._concurrency_type = concurrency
        self._target_name = name

        # Setup and start runner
        self._setup_runner()
        self._runner_instance.start()

    def _setup_runner(self):
        """Setup the target runner."""

        # Setup variables
        self._runner_class = self._concurrency_classes[self._concurrency_type]
        if self._target_name is None:
            self._target_name = '{0}{1}'.format(self._target.__name__.title(),
                                                self._concurrency_type.title())

        # Setup runner
        self._runner_instance = self._runner_class(target=self._target_runner,
                                                   name=self._target_name)
        self._runner_instance.daemon = True

        atexit.register(self._target)

    def _target_runner(self):
        """Run the target periodically."""

        target = self._target
        interval = self._interval

        try:
            while self._is_active.value:
                target()
                time.sleep(interval)
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            target()


class FileParser(ConfigParser.SafeConfigParser):
    """
    Provide a file parser based on :class:`ConfigParser.SafeConfigParser`.

    Stored options are case-sensitive.
    """

    #_filepath_suffixes_lock = multiprocessing.Lock()  # Must be a class attr.
    #_filepath_suffixes_in_use = multiprocessing.Manager().list()  # For Py2.7+
    # Note: multiprocessing.Manager().list() hangs on import in Python 2.6.3.
    # This Python bug is not expected to be present in Python 2.7+.

    def __init__(self, is_active_indicator, filepath_suffix=None):
        """
        Initialize the parser.

        :type is_active_indicator: :obj:`multiprocessing.Value` or
            :obj:`multiprocessing.RawValue`
        :arg is_active_indicator: This must have an attribute ``value`` which
            is evaluated as a :obj:`bool`.

        :type filepath_suffix: :obj:`str` or :obj:`None`
        :arg filepath_suffix: If :obj:`None`, no suffix is used, otherwise the
            provided suffix string is joined to the default file path with an
            underscore.

        Only one instance of this class may be initialized for each unique
        file path suffix.
        """

        ConfigParser.SafeConfigParser.__init__(self)

        # Setup variables
        self._is_active = is_active_indicator
        self._filepath_suffix = filepath_suffix
        self._setup_vars()

        #self._add_filepath_suffix() #For Py2.7+. See _filepath_suffixes_in_use.
        self._makedirs()
        self.read()

    def _setup_vars(self):
        """Setup miscellaneous variables."""

        home_dir = os.getenv('HOME')
        # Note: The environment variable HOME is not available with httpd cgi.
        # Given that the scan is run by the root user, the value of HOME is
        # expected to be "\root".
        filepath_base = os.path.join(home_dir, '.enstore',
                                     settings['scriptname_root'])
        # Note: The value of `filepath_base` is intended to be unique for each
        # module.

        self._filepath = filepath_base
        if self._filepath_suffix:
            self._filepath += '_{0}'.format(self._filepath_suffix)

        self._parser_lock = multiprocessing.Lock()

    def _add_filepath_suffix(self):
        """
        Add the provided filepath suffix to the list of suffixes in use.

        :exc:`ValueError` is raised if the suffix is already in use.
        """

        with self._filepath_suffixes_lock:
            if self._filepath_suffix in self._filepath_suffixes_in_use:
                msg = ('File path suffix "{}" was previously initialized. A '
                       'suffix can be initialized only once.'
                       ).format(self._filepath_suffix)
                raise ValueError(msg)
            else:
                self._filepath_suffixes_in_use.append(self._filepath_suffix)

    def read(self):
        """
        Read the saved values from file if the file exists.

        Note that the retrieved values will be read only into the process from
        which this method is called.
        """

        filepath = self._filepath
        with self._parser_lock:
            if os.path.isfile(filepath):
                ConfigParser.SafeConfigParser.read(self, filepath)

    def _makedirs(self):
        """As necessary, make the directories into which the file will be
        written."""

        path = os.path.dirname(self._filepath)

        try:
            os.makedirs(path)
        except OSError:
            # Note: "OSError: [Errno 17] File exists" exception is raised if
            # the path previously exists, irrespective of the path being a file
            # or a directory, etc.
            if not os.path.isdir(path):
                raise

    def write(self):
        """
        Write the set values to file.

        While this method itself is process safe, the underlying set values are
        not process safe - they are unique to each process.
        """

        try:
            with self._parser_lock:
                with open(self._filepath, 'wb') as file_:
                    ConfigParser.SafeConfigParser.write(self, file_)
                    file_.flush()
                    file_.close()
        except:
            if self._is_active.value: raise

    def optionxform(self, option):
        return option  # prevents conversion to lower case


class Checkpoint(object):
    """
    Provide a checkpoint manager to get and set a checkpoint.

    The :class:`FileParser` class is used internally to read and write the
    checkpoint.
    """

    version = 1  # Version number of checkpointing implementation.
    """Version number of checkpointing implementation."""

    def __init__(self, is_active_indicator, scanner_name):
        """
        Initialize the checkpoint manager with the provided scanner name.

        Each unique notices output file has its own unique checkpoint.

        This class must be initialized only once for a scanner.

        All old or invalid checkpoints are initially deleted.

        :type is_active_indicator: :obj:`multiprocessing.Value` or
            :obj:`multiprocessing.RawValue`
        :arg is_active_indicator: This must have an attribute ``value`` which
            is evaluated as a :obj:`bool`. So long as
            ``is_active_indicator.value`` evaluates to :obj:`True`, the current
            checkpoint is periodically written to file. If and when this
            indicator evaluates to :obj:`False`, the write loop is terminated.
            The checkpoint is then still written one final time.

        :type scanner_name: :obj:`str`
        :arg scanner_name: This represents the name of the current scanner,
            e.g. ``ScannerForward``. It is expected to be the name of the class
            of the current scanner.
        """

        # Setup variables
        self._is_active = is_active_indicator
        self._file_basename = '{0}_checkpoints'.format(scanner_name)
        self._setup_vars()

        self._setup_writer()

    def _setup_vars(self):
        """Setup miscellaneous variables."""

        self._parser = FileParser(self._is_active, self._file_basename)
        self._parser_lock = multiprocessing.Lock()
        self._is_parser_set_enabled = multiprocessing.RawValue(ctypes.c_bool,
                                                               True)

        self._section = 'Version {0}'.format(self.version)
        self._option = settings['output_file']
        self._value = multiprocessing.Manager().Value(unicode, u'')

    def _setup_writer(self):
        """Setup and start the writer thread."""

        self._add_section()
        self._cleanup()
        self.read()
        PeriodicRunner(self._is_active, self.write,
                       settings['checkpoint_write_interval'], 'process',
                       'CheckpointLogger')

    def _is_reliably_active(self):
        """
        Return whether the ``self._is_active`` indicator reliably returns
        :obj:`True`.

        The indicator is checked twice with a time delay between the checks.
        The time delay provides time for the indicator to possibly be set to
        :obj:`False`, such as during an abort.

        This method may be used from any thread or process. It is thread and
        process safe.

        :rtype: :obj:`bool`
        """

        return (self._is_active.value and
                (time.sleep(0.1) or self._is_active.value))
                # Note: "bool(time.sleep(0.1))" is False. Because it is
                # succeeded by "or", it is essentially ignored for boolean
                # considerations. Its only purpose is to introduce a time
                # delay.

    @property
    def value(self):
        """
        For use as a getter, return the locally stored checkpoint.

        :rtype: :obj:`str` (when *getting*)

        For use as a setter, update the locally stored checkpoint with the
        provided value. The checkpoint is updated into the parser
        asynchronously.

        :type value: :obj:`str` (when *setting*)
        :arg value: the current checkpoint.

        The getter or setter may be used from any thread or process. They are
        thread and process safe.
        """

        try: return self._value.value
        except:
            if self._is_reliably_active(): raise
            else: return ''

    @value.setter
    def value(self, value):
        """
        See the documentation for the getter method.

        This method is not documented here because this docstring is ignored by
        Sphinx.
        """

        value = unicode(value)
        try: self._value.value = value
        except:
            if self._is_reliably_active(): raise

    def _add_section(self):
        """Add the pertinent checkpoint section to the parser."""

        with self._parser_lock:
            if not self._parser.has_section(self._section):
                self._parser.add_section(self._section)

    def _cleanup(self):
        """Delete old or invalid checkpoints."""

        max_age = time.time() - 86400 * settings['checkpoint_max_age']

        with self._parser_lock:
            for filepath in self._parser.options(self._section):
                if (((not os.path.isfile(filepath))
                      or (os.stat(filepath).st_mtime < max_age))
                     and (filepath != self._option)):
                    self._parser.remove_option(self._section, filepath)

    def read(self):
        """
        Read and return the checkpoint from the parser.

        The checkpoint defaults to an empty string if it is unavailable in the
        parser.
        """

        try:
            checkpoint = self._parser.get(self._section, self._option)
            # Note: A parser lock is not required or useful for a get
            # operation.
        except ConfigParser.NoOptionError:
            checkpoint = ''
        self.value = checkpoint
        return checkpoint

    def write(self):
        """
        Set and write the locally stored checkpoint, if valid, into the parser.

        The action will happen only if setting it is enabled, otherwise the
        command will be ignored.

        This method is process safe. It is practical to generally call it in
        only one process, however.
        """

        with self._parser_lock:
            if self._is_parser_set_enabled.value:
                checkpoint = self.value
                if checkpoint: # Note: checkpoint is initially an empty string.
                    self._parser.set(self._section, self._option, checkpoint)
                    self._parser.write()

    def remove_permanently(self):
        """
        Remove and also disable the checkpoint altogether from the parser,
        effectively preventing it from being set into the parser again.

        This method is process safe, but it is expected to be called only once.
        """

        self._is_parser_set_enabled.value = False
        with self._parser_lock:
            self._parser.remove_option(self._section, self._option)
            self._parser.write()


class PrintableList(list):
    """Provide a list object which has a valid English string
    representation."""

    def __init__(self, plist=None, separator=', ', separate_last=False):
        """
        Initialize the object.

        :type plist: :obj:`list` or :obj:`None`
        :arg plist: This is the initial :obj:`list` with which to initialize
            the object. It is optional, with a default value of :obj:`None`, in
            which case an empty :obj:`list` is initialized.

        :type separator: :obj:`str`
        :arg separator: the string which is used to delimit consecutive items
            in the list.

        :type separate_last: :obj:`bool`
        :arg separate_last: indicates whether to separate the last two items in
            the list with the specified ``separator``.
        """

        self.separator = separator
        self.separate_last = separate_last

        if plist is None: plist = []
        list.__init__(self, plist)

    def __str__(self):
        """
        Return a valid English string representation.

        :rtype: :obj:`str`

        Example::

            >>> for i in range(5):
            ...     str(PrintableList(range(i)))
            ...
            ''
            '0'
            '0 and 1'
            '0, 1 and 2'
            '0, 1, 2 and 3'
        """

        separator = self.separator
        separator_last = separator if self.separate_last else ' '
        separator_last = '{0}and '.format(separator_last)

        s = (str(i) for i in self)
        s = separator.join(s)
        s = separator_last.join(s.rsplit(separator, 1))
        return s


class ReversibleDict(dict):
    """
    Provide a reversible :obj:`dict`.

    Initialize the object with a :obj:`dict`.
    """

    def reversed(self, sort_values=True):
        """
        Return a reversed :obj:`dict`, with keys corresponding to non-unique
        values in the original :obj:`dict` grouped into a :obj:`list` in the
        returned :obj:`dict`.

        :type sort_values: :obj:`bool`
        :arg sort_values: sort the items in each :obj:`list` in each value of
            the returned :obj:`dict`.
        :rtype: :obj:`dict`

        Example::

            >>> d = ReversibleDict({'a':3, 'c':2, 'b':2, 'e':3, 'd':1, 'f':2})
            >>> d.reversed()
            {1: ['d'], 2: ['b', 'c', 'f'], 3: ['a', 'e']}
        """

        revdict = {}
        for k, v in self.iteritems():
            revdict.setdefault(v, []).append(k)
        if sort_values:
            revdict = dict((k, sorted(v)) for k, v in revdict.items())
        return revdict

    def _reversed_tuple_revlensorted(self):
        """
        Return a :obj:`tuple` created from the reversed dict's items.

        The items in the :obj:`tuple` are reverse-sorted by the length of the
        reversed dict's values.

        :rtype: :obj:`tuple`

        Example::

            >>> d = ReversibleDict({'a':3, 'c':2, 'b':2, 'e':3, 'd':1, 'f':2})
            >>> d._reversed_tuple_revlensorted()
            ((2, ['b', 'c', 'f']), (3, ['a', 'e']), (1, ['d']))
        """

        revitems = self.reversed().items()
        sortkey = lambda i: (len(i[1]), i[0])
        revtuple = tuple(sorted(revitems, key=sortkey, reverse=True))
        return revtuple

    def __str__(self):
        """
        Return a string representation of the reversed dict's items using the
        :class:`PrintableList` class.

        The items in the returned string are reverse-sorted by the length of
        the reversed dict's values.

        :rtype: :obj:`str`

        Example::

            >>> print(ReversibleDict({'a':3, 'c':2, 'b':2, 'e':3, 'd':1, 'f':2}))
            b, c and f (2); a and e (3); and d (1)
            >>> print(ReversibleDict({'a': 3, 'c': 2}))
            a (3) and c (2)
        """
        revtuple = self._reversed_tuple_revlensorted()

        revstrs = ('{0} ({1})'.format(PrintableList(values), key)
                   for key, values in revtuple)
        pl_args = ('; ', True) if (max(len(i[1]) for i in revtuple) > 1) else ()
        revstrs = PrintableList(revstrs, *pl_args)
        revstr = str(revstrs)
        return revstr


class MPSubDictCache:
    """
    Provide a memory-efficient and :mod:`multiprocessing`-safe subset of a
    :obj:`dict` for use when the values in the :obj:`dict` are themselves
    dicts.

    Memory efficiency is derived from sharing the keys used in the level 2
    dicts.
    """

    def __init__(self):
        """Initialize the object."""

        self._manager = multiprocessing.Manager()
        # Note: If the above line is executed at import-time, it makes the
        # program hang on exit in Python 2.6.3. In addition, the dict creation
        # lines below, if executed at import-time, make the program hang at
        # import-time in Python 2.6.3. With Python 2.7+, it may better to
        # declare `_manager` as a class variable instead of an instance
        # variable.

        self._cache = self._manager.dict()
        self._subkeys = self._manager.dict() # e.g. {'kA': 1, 'kB': 2}
        self._subkeys_reverse = self._manager.dict() # e.g. {1: 'kA', 2: 'kB'}

        self._index = multiprocessing.Value(ctypes.c_ushort)
        self._setitem_lock = multiprocessing.Lock()

    def __contains__(self, k):
        """``D.__contains__(k)`` returns :obj:`True` if ``D`` has a key ``k``,
        else returns :obj:`False`."""
        return (k in self._cache)

    def __getitem__(self, k):
        """``x.__getitem__(y) <==> x[y]``"""
        subdict = self._cache[k] # can raise KeyError
        subdict = self._decompress_subkeys(subdict) # should not raise KeyError
        return subdict

    def __setitem__(self, k, subdict):
        """``x.__setitem__(i, y) <==> x[i]=y``"""
        with self._setitem_lock:
            subdict = self._compress_subkeys(subdict)
            self._cache[k] = subdict

    @property
    def _next_index(self):
        index = self._index.value
        self._index.value += 1
        return index

    def _compress_subkeys(self, subdict):
        """
        Compress the keys in the ``subdict`` using an internal index.

        :type subdict: :obj:`dict`
        :arg subdict:
        :rtype: :obj:`dict`
        """

        subdict_compressed = {}
        for k,v in subdict.items():
            try: k_compressed = self._subkeys[k]
            except KeyError:
                k_compressed = self._next_index
                self._subkeys[k] = k_compressed
                self._subkeys_reverse[k_compressed] = k
            subdict_compressed[k_compressed] = v
        return subdict_compressed

    def _decompress_subkeys(self, subdict):
        """
        Decompress the keys in the ``subdict`` using the internal index.

        :type subdict: :obj:`dict`
        :arg subdict:
        :rtype: :obj:`dict`
        """

        return dict((self._subkeys_reverse[k],v) for k,v in subdict.items())


class CommandLineOptionsParser(optparse.OptionParser):
    """
    Parse program options specified on the command line.

    These are made available using instance attributes ``options`` and
    ``args``, as described below.

    ``options`` is an attribute providing values for all options. For example,
    if ``--file`` takes a single string argument, then ``options.file`` will be
    the filename supplied by the user. Alternatively, its value will be
    :obj:`None` if the user did not supply this option.

    ``args`` is the list of positional arguments leftover after parsing
    ``options``.

    The :meth:`add_option` method can be used to add an option.
    """

    # In newer versions of Python, i.e. 2.7 or higher, the usage of the
    # optparse module should be replaced by the newer argparse module.

    _options_seq = []

    def __init__(self):
        """Parse options."""

        usage = '%prog -t SCAN_TYPE [OPTIONS]'
        optparse.OptionParser.__init__(self, usage=usage)

        self._add_options()
        (self.options, self.args) = self.parse_args()
        self._check_option_values()
        self._process_options()

    def add_option(self, *args, **kwargs):

        # Refer to the docstring of the overridden method.

        optparse.OptionParser.add_option(self, *args, **kwargs)
        if kwargs.get('dest'): self._options_seq.append(kwargs['dest'])

    @staticmethod
    def output_filename():
        """Return the name of the output file for notices."""

        datetime_str = datetime.datetime.now().strftime('%Y%m%dT%H%M%S')
        filen = '{0}_{1}.log'.format(settings['scriptname_root'], datetime_str)
        pathn = os.path.abspath(filen)
        return pathn

    def _add_options(self):
        """Add various options."""

        # Add scan type option
        self.add_option('-t', '--type', dest='scan_type',
                        choices=('forward', 'reverse'),
                        help='(forward, reverse) scan type',
                        )

        # Add scan directory option
        directory = settings['fs_root']
        self.add_option('-d', '--directory', dest='fs_root',
                        default=directory,
                        help=('(for forward scan only) absolute path of '
                              'directory to scan recursively '
                              '(recommended default is {0}) (not recommended '
                              'to be specified for large nested directories)'
                              ).format(directory))

        # Add notices output file option
        filename = self.output_filename()
        self.add_option('-o', '--output_file', dest='output_file',
                        default=filename,
                        help=('absolute path to output file for notices '
                              '(default is dynamic, e.g. {0}) (appended if '
                              'exists)'
                              ).format(filename))

        # Add printing option
        self.add_option('-p', '--print', dest='print',
                        choices=('checks', 'notices'),
                        help=('(checks, notices) for the specified scan type, '
                              'print all runnable checks and their overviews, '
                              'or all notice templates, and exit'),
                        )

        # Add resume option
        self.add_option('-r', '--resume', dest='resume_scan', default=False,
                        action='store_true',
                        help=('for specified output file (per -o), resume scan '
                              'where aborted (default is to restart scan) (use '
                              'with same database only)'))

        # Add status interval option
        status_interval = settings['status_interval']
        self.add_option('-s', '--status_interval', dest='status_interval',
                        type=float,
                        default=status_interval,
                        help=('max status output interval in seconds (default '
                              'approaches {0})').format(status_interval),
                        )

    def _check_option_values(self):
        """Check whether options have been specified correctly, and exit
        otherwise."""

        # Check required options
        if not all((self.options.scan_type, self.options.output_file)):
            self.print_help()
            self.exit(2)

    def _process_options(self):
        """Process and convert options as relevant."""

        # Process options as relevant
        self.options.fs_root = os.path.abspath(self.options.fs_root)
        self.options.output_file = os.path.abspath(self.options.output_file)

        # Convert options from attributes to keys
        self.options = dict((k, getattr(self.options, k)) for k in
                            self._options_seq)


class Enstore:
    """
    Provide an interface to various Enstore modules.

    A unique instance of this class must be created in each process in which
    the interface is used.
    """

    def __init__(self):
        """Initialize the interface."""

        self.config_client = \
            enstore_configuration_client.ConfigurationClient()
        self.config_dict = self.config_client.dump_and_save()
        self.library_managers = [k[:-16] for k in self.config_dict
                                 if k.endswith('.library_manager')]

        info_client_flags = (enstore_constants.NO_LOG |
                             enstore_constants.NO_ALARM)
        self.info_client = \
            enstore_info_client.infoClient(self.config_client,
                                           flags=info_client_flags)

        #self.storagefs = enstore_namespace.StorageFS()

class Chimera:
    """
    Provide an interface to the Chimera database.

    A unique database connection is internally used for each process and thread
    combination from which the class instance is used.
    """

    _connections = {}
    _cursors = {}

    @staticmethod
    def confirm_psycopg2_version():
        """Exit the calling process with an error if the available
        :mod:`psycopg2` version is less than the minimally required version."""

        ver_str = psycopg2.__version__
        ver = [int(i) for i in ver_str.partition(' ')[0].split('.')]
        min_ver = [2, 4, 5]
        min_ver_str = '.'.join(str(v) for v in min_ver)

        if ver < min_ver:
            msg = ('The installed psycopg2 version ({0}) is older than the'
                   ' minimally required version ({1}). Its path is {2}.'
                   ).format(ver_str, min_ver_str, psycopg2.__path__)
            exit(msg)  # Return code is 1.

    @staticmethod
    def confirm_fs():
        """Exit the calling process with an error if the filesystem root is not
        a Chimera filesystem."""

        # Note: This method helps confirm that PNFS is not being used.

        fs_root = str(settings['fs_root']) # StorageFS requires str, not unicode
        fs_type_reqd = chimera.ChimeraFS
        #import pnfs; fs_type_reqd = pnfs.Pnfs
        fs_type_seen = enstore_namespace.StorageFS(fs_root).__class__

        if fs_type_reqd != fs_type_seen:
            msg = ('The filesystem root ({0}) is required to be of type {1} '
                   'but is of type {2}.'
                   ).format(fs_root,
                            fs_type_reqd.__name__, fs_type_seen.__name__)
            exit(msg)  # Return code is 1.

    @property
    def _cursor(self):
        """Return the cursor for the current process and thread combination."""

        key = (multiprocessing.current_process().ident,
               threading.current_thread().ident)

        if key in self._cursors:
            return self._cursors[key]
        else:
            # Create connection
            conn = psycopg2.connect(database='chimera', user='enstore')
            #conn.set_session(readonly=True, autocommit=True)
            self._connections[key] = conn
            # Create cursor
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            self._cursors[key] = cursor
            return cursor

    def fetchone(self, *args):
        """Fetch and return a row for the provided query arguments."""

        self._cursor.execute(*args)
        return self._cursor.fetchone()


class Scanner:
    """
    This is the base class for derived scanner classes.

    A derived class must:

    - Define the :meth:`validate_scan_location` method to validate the scan
      location root.
    - Define the :meth:`queue_items` method which puts items into the
      :attr:`items_q` queue.
    - Define *check methods* as relevant, which raise the appropriate
      :class:`Notice` exception.
    - Define the :attr:`checks` variable which is a
      :obj:`~collections.Sequence` of *check method* names, indicating the
      order in which to execute the methods.
    - Define the :attr:`notices` variable, the requirements for which are
      specified by the :meth:`Notice.update_notices` method.
    - Define the :meth:`get_num_items` method returning a non-negative integer
      which is the total number of items to be processed.

    To run the scan, initialize the derived class, and call the :meth:`run`
    method.
    """

    num_scan_processes = (multiprocessing.cpu_count() *
                          settings['num_scan_processes_per_cpu'])

    # Estimate the max number of pending items that may remain queued
    est_max_scan_speed = 120 * 5  # (per second) (Guesstimate with SSD.)
    est_max_dir_list_time = 15  # Est. max seconds to walk a single large dir.
    queue_max_len = est_max_scan_speed * est_max_dir_list_time
    # Note: The goals here are twofold:
    #  - limiting memory usage
    #  - ensuring a process does not run out of pending items to scan
    # This will need to be moved to the derived class if different scanners have
    # very different scan speeds.

    def __init__(self):
        """Set up the scanner."""

        self._check_prereqs_prevars()
        self._setup_vars()
        self._check_prereqs_postvars()
        self._setup_workers()

    def _check_prereqs_prevars(self):
        """Check various prerequisites before setting up instance variables."""

        self._check_ugid()
        self._check_exclusive()

        #Chimera.confirm_psycopg2_version()
        Chimera.confirm_fs()

    def _setup_vars(self):
        """Setup miscellaneous variables."""

        settings['output_file_dict'] = '{0}.dict'.format(
                                                  settings['output_file'])
        Notice.update_notices(self.notices)
        self.is_active = multiprocessing.RawValue(ctypes.c_bool, True)
        self.checkpoint = Checkpoint(self.is_active, self.__class__.__name__)
        self.num_items_total = multiprocessing.RawValue(ctypes.c_ulonglong,
                                                        self.get_num_items())
        self._start_time = time.time() # Intentionally defined now.

    def _check_prereqs_postvars(self):
        """Check various prerequisites after setting up instance variables."""

        self.validate_scan_location()  # Implemented in derived class.
        self._validate_output_files_paths()
        self._validate_checkpoint()

    def _setup_workers(self):
        """Setup and start worker processes and threads."""

        self._setup_mp()
        self._start_workers()
        self._start_ScannerWorker_monitor()

    def run(self):
        """
        Perform scan.

        This method blocks for the duration of the scan.
        """

        try:
            self.queue_items()  # Defined by derived class  # is blocking
        except (KeyboardInterrupt, SystemExit) as e:
            self.is_active.value = False
            if isinstance(e, KeyboardInterrupt):
                print()  # Prints a line break after "^C"
            if str(e):
                print(e, file=sys.stderr)
        except:
            self.is_active.value = False
            raise
        finally:
            # Note: self.is_active.value must *not* be set to False here,
            # independent of whether it was previously set to False. It should
            # essentially remain True if the work ends normally.
            self._stop_workers()
            self._do_postreqs()
            if not self.is_active.value:
                # Note: This condition will here be True if an abort was
                # instructed, such as using KeyboardInterrupt or SystemExit.
                # This can cause an unknown non-daemonic process to hang on
                # exit. To get past the hang, the exit is forced. The hang is
                # not observed to occur during a normal scan completion.
                os._exit(1)

    def _do_postreqs(self):
        """Perform post-requisites after completion of the scan."""

        # Finalize checkpoint
        if self.is_active.value:
            self.checkpoint.remove_permanently()
        else:
            self.checkpoint.write()

        # Log status
        try: self._log_status()  # Warning: Not executed if using `tee`.
        except IOError: pass

    def _check_ugid(self):
        """Check whether process UID and GID are 0, exiting the process with an
        error otherwise."""

        id_reqd = 0

        if (os.getuid() != id_reqd) or (os.getgid() != id_reqd):
            msg = ('Error: UID and GID must be {0}, but are {1} and {2} '
                   'instead.').format(id_reqd, os.getuid(), os.getgid())
            exit(msg)
        else:
            # Set "effective" IDs in case they are not equal to "real" IDs
            os.setegid(id_reqd)
            os.seteuid(id_reqd)
            # Note: EGID is intentionally set before EUID. This is because
            # setting EUID first (to a non-root value) can result in a loss of
            # power, causing the EGID to then become unchangeable.

    def _check_exclusive(self):
        """
        Check if the scanner is running exclusively.

        Requirements:

        - Root permissions.
        - The ``enstore`` user and group must exist.

        This check is required to be performed only once. For safety, this must
        be done before the initialization of file-writing objects such as
        :attr:`self.checkpoint`, etc.

        This check is performed to prevent accidentally overwriting any of the
        following:

        - Notices output files if they have the same name.
        - The common checkpoint file.
        """

        # Establish lock dir settings
        ld_name =  '/var/run/enstore'  # Parent dir is later assumed to exist.
        # Note: Writing in "/var/run" requires root permissions.
        ld_mode = stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP
        #       = 0o660 = 432
        try:
            ld_uid = pwd.getpwnam('enstore').pw_uid
        except KeyError:  # User 'enstore' does not exist.
            ld_uid = -1  # -1 keeps the value unchanged.
        try:
            ld_gid = grp.getgrnam('enstore').gr_gid
        except KeyError:  # Group 'enstore' does not exist.
            ld_gid = -1  # -1 keeps the value unchanged.

        # Create lock directory
        umask_original = os.umask(0)
        try:
            os.mkdir(ld_name, ld_mode)  # This assumes parent dir exists.
        except OSError:
            if not os.path.isdir(ld_name): raise
        else:
            os.chown(ld_name, ld_uid, ld_gid)
        finally:
            os.umask(umask_original)

        # Establish lock file settings
        lf_name = '{0}.lock'.format(settings['scriptname_root'])
        lf_path = os.path.join(ld_name, lf_name)
        lf_flags = os.O_WRONLY | os.O_CREAT
        lf_mode = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH  # = 0o222 = 146

        # Create lock file
        umask_original = os.umask(0)
        try:
            lf_fd = os.open(lf_path, lf_flags, lf_mode)
        finally:
            os.umask(umask_original)
        # Note: It is not necessary to use "os.fdopen(lf_fd, 'w')" to open a
        # file handle, or to keep it open for the duration of the process.

        # Try locking the file
        try:
            fcntl.lockf(lf_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            msg = ('Error: {0} may already be running. Only one instance of it '
                   'can run at a time.'
                   ).format(settings['scriptname_root'].title())
            exit(msg)

        # Note: Because fcntl is used, it is not necessary for the locked file
        # to be deleted at the end of the scan.

    def _validate_output_files_paths(self):
        """Validate the paths of the scan notices output files."""

        # Determine file properties
        of_map = {'main': 'output_file', 'dict': 'output_file_dict'}
        of_all = {}
        for of_name, of_key in of_map.items():
            of = {}
            of['path'] = settings[of_key]
            of['path_basename'] = os.path.basename(of['path'])
            of['path_exists'] = os.path.isfile(of['path'])
            of_all[of_name] = of

        # Add file paths to message
        ofall_paths = ['"{0}"'.format(of['path']) for of in of_all.values()]
        ofall_paths = sorted(ofall_paths)
        ofall_paths_str = str(PrintableList(ofall_paths))
        msg = 'The scan notices output files are {0}.'.format(ofall_paths_str)

        # Add an "appended" or "created" status to message
        if of_all['main']['path_exists'] == of_all['dict']['path_exists']:
            if of_all['main']['path_exists']:
                msg += ' These files exist and will be appended.\n'
            elif not settings['resume_scan']:
                msg += ' These files do not exist and will be created.\n'
            else:
                msg += '\n'
            print(msg)
        else:
            msg = ('Error: {0} These must be in a consistent state. Both must '
                   'either exist (so they can be appended), or both must not '
                   'exist (so they can be created). If one of these files is '
                   'missing, it is recommended the other be deleted.'
                   ).format(msg)
            exit(msg)

    def _validate_checkpoint(self):
        """
        Validate usability of checkpoint.

        This is performed only when a scan resumption is requested.
        """

        # Note: It is possible that this method belongs in the derived class
        # instead.

        if not settings['resume_scan']: return

        output_file = settings['output_file']
        output_file_dict = settings['output_file_dict']
        checkpoint = self.checkpoint.value

        # Caution: Checks specific to the current scanner musn't be done here.

        if ((not os.path.isfile(output_file)) or
            (not os.path.isfile(output_file_dict))):
            msg = ('Error: A request to resume the scan for the current set of '
                   'output files ("{0}" and "{1}") was received, but one or '
                   'more of these output files do not already exist. A scan '
                   'can be resumed only for an existing set of incomplete '
                   'output files.'
                   ).format(output_file, output_file_dict)
            exit(msg)
        elif not checkpoint:
            msg = ('Error: A request to resume the scan was received, but a '
                   'checkpoint is unavailable for the current primary output '
                   'file ({0}). If an output file was previously specified, '
                   'the currently specified path must match the previous path '
                   'exactly.'
                   ).format(output_file)
            exit(msg)
        else:
            msg = ('Scan will be resumed at the approximate checkpoint "{0}". '
                   'Items preceding this checkpoint will still be traversed by '
                   'the scanner but will not be rescanned. Because the '
                   'checkpoint is an approximate one, a few duplicate entries '
                   'may exist in the notices output file near the checkpoint.\n'
                   ).format(checkpoint)
            # Note: For more info on setting a checkpoint, see
            # self._item_handler.
            print(msg)

    def _setup_mp(self):
        """Setup :mod:`multiprocessing` environment."""

        # Setup queues
        self.items_q = multiprocessing.Queue(self.queue_max_len)
        self.noticegrps_q = multiprocessing.Queue(self.queue_max_len)

        # Setup counter
        self.num_ScannerWorker_alive = multiprocessing.Value(ctypes.c_ubyte)
        self.num_items_processed = multiprocessing.Value(ctypes.c_ulonglong,
                                                         lock=True)
        self._num_items_processed_lock = multiprocessing.Lock()

        # Define processes
        # Note defaults: {'num': 1, 'join': True, 'queue': None}
        self._processes = ({'name': 'ScannerWorker',
                            'num': self.num_scan_processes,
                            'target': self._item_handler,
                            'queue': self.items_q,},
                           {'name': 'NotificationLogger',
                            'target': self._noticegrp_handler,
                            'queue': self.noticegrps_q,},
                           {'name': 'StatusLogger',
                            'target': self._status_logger, 'join': False,},
                           )
        # Note that processes will be started in the order specified in the
        # above sequence. They will also later be terminated in the same order.
        # It is important to terminate them in the same order.

    def _increment_num_items_processed(self, amount=1):
        """
        Increment the number of items processed by the specified amount.

        This method is process safe.

        :type amount: :obj:`int` (positive)
        :arg amount: number by which to increment
        """

        with self._num_items_processed_lock:  # required
            self.num_items_processed.value += amount

    def _start_workers(self):
        """Start workers."""

        # Start processes in order
        for pgrp in self._processes:
            pgrp['processes'] = []
            num_processes = pgrp.get('num', 1)
            for i in range(num_processes):
                name = pgrp['name']
                if num_processes > 1: name += str(i)
                process = multiprocessing.Process(target=pgrp['target'],
                                                  name=name)
                process.daemon = True
                pgrp['processes'].append(process)
                process.start()

    def _start_ScannerWorker_monitor(self):
        """Start a monitor to track the number of running worker processes."""

        def monitor():
            interval = settings['status_interval']/4
            processes = [pgrp for pgrp in self._processes if
                       pgrp['name']=='ScannerWorker']
            processes = processes[0]['processes']
            try:
                while self.is_active.value:
                    try:
                        num_alive = sum(p.is_alive() for p in processes)
                        # Note: The "is_alive" Process method used above can be
                        # used only in the parent process. As such, this
                        # function must be run in the main process and not in a
                        # child process.
                    except OSError:
                        if self.is_active.value: raise
                    else:
                        self.num_ScannerWorker_alive.value = num_alive
                        time.sleep(interval)
            except (KeyboardInterrupt, SystemExit):
                pass

        monitor_thread = threading.Thread(target=monitor)
        monitor_thread.daemon = True
        monitor_thread.start()

    def _stop_workers(self):
        """Stop workers cleanly."""

        # Stop processes in order
        for pgrp in self._processes:
            pgrp_queue = pgrp.get('queue')
            if pgrp_queue is not None:
                pgrp_num = pgrp.get('num', 1)
                for _ in range(pgrp_num):
                    if self.is_active.value:
                        pgrp_queue.put(None)  # is blocking
                    else:
                        try: pgrp_queue.put_nowait(None)
                        except Queue.Full: pass
            if pgrp.get('join', True):
                for p in pgrp['processes']:
                    p.join()

        time.sleep(settings['sleep_time_at_exit'])

    def _item_handler(self):
        """
        Handle queued item paths.

        Multiple instances of this method are expected to run simultaneously.
        """

        # Create process-specific interfaces
        Item.enstore = Enstore()

        # Setup local variables
        local_count = 0
        # Note: A separate local count (specific to the current process) and
        # global count are maintained. This allows for infrequent updates to
        # the global count. The local count is the number of items that have
        # been processed locally in the current process, but have not yet been
        # incremented in the global count.
        update_time = time.time()  # Most recent global state update time.
        update_thresholds = {'count': self.num_scan_processes * 2,
                             'time': settings['status_interval'] / 2,}
        # Note: The thresholds are used to reduce possible contention from
        # multiple scan processes for updating the shared global values. A
        # global update is executed when any of the thresholds are reached.

        # Setup required external variables locally
        item_get = self.items_q.get
        noticegrp_put = self.noticegrps_q.put
        gc_updater = self._increment_num_items_processed  # For global count.

        def process_item(item):
            noticegrp = self._scan_item(item)
            if noticegrp:
                noticegrp = noticegrp.to_dict()
                # Note: The object is transmitted over multiprocessing as a
                # dict. This is because a dict can be easily pickled and
                # unpickled, whereas the object itself is more difficult to
                # pickle and unpickle.
                noticegrp_put(noticegrp)

        def update_state(item, local_count, update_time, force_update=False):

            if (item is not None) and item.is_scanned:
                local_count += 1

            update_age = time.time() - update_time
            update_thresholds_met = {
                'count': local_count == update_thresholds['count'],
                'time': update_age > update_thresholds['time'],}
            update_threshold_met = any(update_thresholds_met.values())

            if (local_count > 0) and (update_threshold_met or force_update):
                gc_updater(local_count)
                local_count = 0
                update_time = time.time()
                if (item is not None) and item.is_scanned and item.is_file():
                    # Note: The checkpoint is set only if the item is a
                    # file, as opposed to a directory. This is because
                    # the resumption code can currently only use a file
                    # as a checkpoint.
                    self.checkpoint.value = item
                    # Note: Given that multiple workers work on the
                    # work queue concurrently, this setting of the
                    # checkpoint is an approximate one, as coordinated
                    # checkpointing is not used. A blocking version of
                    # coordinated checkpointing can possibly be
                    # implemented with multiprocessing.Barrier, which
                    # was only introduced in Python 3.3.

            return (local_count, update_time)

        # Process items
        item = None  # Prevents possible NameError exception in finally-block.
        try:
            while self.is_active.value:
                item = item_get()
                if item is None:  # Put by self._stop_workers()
                    break
                else:
                    process_item(item)
                    local_count, update_time = \
                        update_state(item, local_count, update_time)
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            update_state(item, local_count, update_time, force_update=True)
            # Note: If an exception is raised in the try-block, due to
            # implementation limitations, it is possible that a duplicate
            # update is performed on the global count.
            time.sleep(settings['sleep_time_at_exit'])

    def _noticegrp_handler(self):
        """
        Handle queued :class:`NoticeGrp` objects.

        Only a single instance of this method is expected to run.
        """

        noticegrp_get = self.noticegrps_q.get

        # Obtain handles to output files
        output_file = open(settings['output_file'], 'a')
        output_file_dict = open(settings['output_file_dict'], 'a')

        # Write to output files
        try:
            while self.is_active.value:
                noticegrp = noticegrp_get()
                if noticegrp is None:  # Put by self._stop_workers()
                    break
                else:
                    noticegrp = NoticeGrp.from_dict(noticegrp)
                    # Note: The object is transmitted over multiprocessing
                    # as a dict. This is because a dict can be easily
                    # pickled and unpickled, whereas the object itself is
                    # more difficult to pickle or unpickle.
                    if noticegrp:

                        str_out = '{0}\n\n'.format(noticegrp)
                        output_file.write(str_out)
                        str_out = '{0}\n'.format(noticegrp.to_exportable_dict())
                        output_file_dict.write(str_out)

                        output_file.flush()
                        output_file_dict.flush()

        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            time.sleep(settings['sleep_time_at_exit'])
            output_file.flush()
            output_file_dict.flush()
            output_file.close()
            output_file_dict.close()

    def _status_logger(self):
        """
        Log the current status after each interval of time.

        Only a single instance of this method is expected to run.
        """

        # Setup environment
        interval = settings['status_interval']
        interval_cur = 1
        num_items_processed_previously = secs_elapsed_previously = 0

        # Log status
        try:
            while self.is_active.value:
                (num_items_processed_previously, secs_elapsed_previously) = \
                    self._log_status(num_items_processed_previously,
                                     secs_elapsed_previously,
                                     interval_cur)
                time.sleep(interval_cur)
                interval_cur = min(interval, interval_cur*math.e)
                # Note: `interval_cur` converges to `interval`.
                # `interval_cur*math.e` is equivalent to `math.e**x` with
                # incrementing x.
        except (KeyboardInterrupt, SystemExit):
            pass
        # Note: Execution of "self._log_status()" inside a "finally" block here
        # does not happen. It is done in the main process instead.

    def _log_status(self, num_items_processed_previously=None,
                          secs_elapsed_previously=None,
                          interval_next=None):
        """
        Log the current status once.

        :type num_items_processed_previously: :obj:`int` (non-negative) or
            :obj:`None`
        :arg num_items_processed_previously: Number of items processed
            cumulatively, as returned previously.
        :type secs_elapsed_previously: :obj:`int` (non-negative) or :obj:`None`
        :arg secs_elapsed_previously: Number of seconds elapsed cumulatively,
            as returned previously.
        :type interval_next: :obj:`int` (non-negative) or :obj:`None`
        :arg interval_next: Number of seconds to next update.

        .. todo:: The `num_items_processed_previously` and
            `secs_elapsed_previously` arguments are to be removed and replaced
            using class instance variables.
        """

        dttd = lambda s: datetime.timedelta(seconds=round(s))
        intstr = lambda i: locale.format('%i', i, grouping=True)
#       fmtstrint = lambda s, i: '{0} {1}'.format(s, intstr(i))
        items = []

        # Prepare basic items for status
        datetime_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        num_items_total = int(self.num_items_total.value)
        num_items_processed_cum = int(self.num_items_processed.value)
        secs_elapsed_cum = time.time() - self._start_time
        speed_cum = num_items_processed_cum / secs_elapsed_cum  #  items/second

        # Add basic stats
        items += [('Active workers',
                   intstr(self.num_ScannerWorker_alive.value)),  #approx
                  ('Time elapsed',
                   dttd(secs_elapsed_cum)),
                  ('Items scanned (cumulative)',
                   intstr(num_items_processed_cum)),  #approx
                  ('Speed (cumulative) (items/s)',
                   intstr(speed_cum)),  #approx
                  ]

        # Conditionally add current iteration status
        if ((num_items_processed_previously is not None) and
            (secs_elapsed_previously is not None)):
            # Above is explicit check to disambiguate from 0

            num_items_processed_cur = (num_items_processed_cum -
                                       num_items_processed_previously)
            secs_elapsed_cur = secs_elapsed_cum - secs_elapsed_previously
            speed_cur = num_items_processed_cur / secs_elapsed_cur

            items += [
#                      ('Items processed (current)',
#                       intstr(num_items_processed_cur)),  #approx
                      ('Speed (current) (items/s)',
                       intstr(speed_cur)),  #approx
                      ]

        # Conditionally add remaining time
        remaining_secs = float('inf')
        if num_items_total is not None:
            num_items_remaining = num_items_total - num_items_processed_cum
            items.append(('Items remaining',
                          intstr(num_items_remaining)))  #approx
            if speed_cum != 0:
                remaining_secs = num_items_remaining / speed_cum
                total_secs = secs_elapsed_cum + remaining_secs
                items += [('Time remaining', dttd(remaining_secs)),  #approx
                          ('Time total', dttd(total_secs)),  #approx
                          ]

        # Conditionally add time to next update
        if interval_next is not None:
            interval_next = min(interval_next, remaining_secs)
            items += [('Time to next update', dttd(interval_next))]  #approx

        # Prepare status string
        max_item_key_len = max(len(i[0]) for i in items)
        items = ['{0:{1}}: {2}'.format(i[0], max_item_key_len, i[1]) for i in
                 items]
        items.insert(0, datetime_now)
        status = '\n'.join(items)

        print(status, end='\n\n')
        return (num_items_processed_cum, secs_elapsed_cum)

    def _scan_item(self, item):
        """
        Scan provided item as per the *check method* names and dependencies
        which are specified in the :attr:`self.checks` attribute, returning the
        updated :class:`NoticeGrp` instance for the item.

        Multiple instances of this method are expected to run on different
        items simultaneously.
        """

        # Note: self.checks should not be modified.

        checks_pending = list(self.checks)
        # Note: Using `list(some_dict)` creates a copy of the dict's keys in
        # both Python 2.x and 3.x. A copy is created here because the list is
        # mutated later in the method.

        #random.shuffle(checks_pending)
        # Note: The list of pending checks can be randomized to reduce the
        # possibility of multiple worker processes trying to read the same type
        # of file metadata at the same time. By explicitly randomizing the
        # order in which various checks may run, there may be slightly less
        # contention. It is unclear if this randomization actually affects the
        # scan speed.

        ccs = {}  # This refers to checks completion statuses.

        def pop_pending_check_name():
            for check in copy.copy(checks_pending):
                # Note: A copy is used above because checks_pending is modified
                # inside the loop. Not using a copy may cause a problem with
                # iteration.
                prereqs = self.checks[check]
                is_prereqs_passed = all(ccs.get(pr)==True  for pr in prereqs)
                is_prereqs_failed = any(ccs.get(pr)==False for pr in prereqs)
                if is_prereqs_passed or is_prereqs_failed:
                    checks_pending.remove(check)
                    if is_prereqs_passed: return check
            raise IndexError('no pending check')

        # Run checks
        while True:

            # Get check
            try: check_name = pop_pending_check_name()
            except IndexError: break
            check = getattr(self, check_name)

            # Run check
            run_status = None
            try:
                check(item)
            except Notice as notice:
                item.add_notice(notice)
                run_status = not (isinstance(notice, CriticalNotice)
                                  or isinstance(notice, ErrorNotice))
                if isinstance(notice, CriticalNotice): break
            else:
                run_status = True
            finally:
                if run_status is not None:
                    ccs[check_name] = run_status

        item.is_scanned = True
        return item.noticegrp

    @classmethod
    def print_checks(cls):
        """
        Print names and docstrings of all runnable *check methods*.

        A check is runnable if all its prerequisites, if any, are runnable.
        """

        def is_runnable(check):
            return ((check in cls.checks) and
                    all(is_runnable(prereq) for prereq in cls.checks[check]))
            # Note: This check is performed to help with debugging in the
            # event some checks are manually disabled.

        def describe(check):
            method = getattr(cls, check)
            desc = inspect.getdoc(method)
            desc = desc.partition('\n\n')[0]  # Truncate at first empty line.
            desc = desc.replace('\n', ' ')  # Joins multiple lines.
            desc = desc.replace('`', '')  # Removes what may be Sphinx syntax.
            desc = '{}: {}'.format(check, desc)
            return desc

        checks = (describe(c) for c in sorted(cls.checks) if is_runnable(c))
        for c in checks: print(c)


class ScannerForward(Scanner):
    """Perform forward scan."""

    checks = {
    # Each dict key is the name of a check method which accepts an Item object.
    # The method can optionally raise a Notice (or an inherited) exception. If
    # the ErrorNotice or CriticalNotice exception is raised, the check is
    # considered failed, otherwise succeeded. If a CriticalNotice exception is
    # raised, all remaining checks for the Item are skipped.
    #
    # Each dict value is a sequence of names of dependencies of check methods.
    #
    # The noted check methods can be run in a pseudo-random order as long as
    # the respective dependencies have succeeded.
    'check_path_attr':           (),  # Test check; never fails.
    'check_is_storage_path':     (),
    'check_lstat':               ('check_is_storage_path',),
    'check_link_target':         ('check_is_storage_path',),
    'check_file_nlink':          ('check_lstat',),
    'check_file_temp':           ('check_is_storage_path',),
    'check_file_bad':            ('check_is_storage_path',),
    'check_fsid':                ('check_is_storage_path',),
#    'check_parentfsid':          # Never fails
#                                 ('check_fsid',
#                                  'check_lstat',),

    'check_fslayer2lines':       ('check_fsid',
                                  'check_lstat',),
    'check_fslayer2':            ('check_fslayer2lines',),
    'check_fslayer2_crc':        ('check_fslayer2',),
    'check_fslayer2_size':       ('check_fslayer2',),
    'check_fslayer2_size_match': ('check_fslayer2',
                                  'check_fslayer2_size',),
    'check_fslayer1lines':       ('check_fsid',
                                  'check_lstat',
                                  'check_fslayer2lines',
                                  'check_fslayer2',),
    'check_fslayer1':            ('check_fslayer1lines',
                                  'check_lstat',),
    'check_fslayer4lines':       ('check_fsid',
                                  'check_lstat',
                                  'check_fslayer2lines',
                                  'check_fslayer2',),
    'check_fslayer4':            ('check_fslayer4lines',
                                  'check_lstat',),

    'check_enstore_file_info':   ('check_lstat',
                                  'check_fslayer1lines',), # L1 BFID is used.
    'check_enstore_volume_info': ('check_lstat',
                                  'check_enstore_file_info',),

    'check_bfid_match':          ('check_fslayer1',
                                  'check_fslayer4',
                                  'check_enstore_file_info',),

    'check_file_deleted':        ('check_enstore_file_info',),
    'check_recent':              ('check_lstat',),
    'check_empty':               ('check_lstat',
                                  'check_fslayer2_size_match',
                                  'check_enstore_file_info',),

    'check_sfs_path':            ('check_is_storage_path',),

    'check_volume_label':        ('check_fslayer4',
                                  'check_enstore_file_info',),
    'check_location_cookie':     ('check_fslayer4',
                                  'check_enstore_file_info',),
    'check_size':                ('check_lstat',
                                  'check_fslayer2',
                                  'check_fslayer4',
                                  'check_enstore_file_info',),
    'check_file_family':         ('check_fslayer4',
                                  'check_enstore_volume_info',),
#     'check_library':              # Not necessary
#                                  ('check_enstore_volume_info',),
    'check_drive':               ('check_fslayer4',
                                  'check_enstore_file_info',),
    'check_crc':                 (#'check_fslayer2',
                                  'check_fslayer4',
                                  #'check_enstore_volume_info',
                                  'check_enstore_file_info',),
    'check_path':                ('check_fslayer4',
                                  'check_enstore_file_info',),
    'check_pnfsid':              ('check_fsid',
                                  'check_fslayer4',
                                  'check_enstore_file_info',),

    'check_copy':                ('check_enstore_file_info',),
    }

    notices = {
    'NoPath': "Cannot access object's path.",
    'NotStorage': 'Not an Enstore file or directory.',
    'NoStat': "Cannot access object's filesystem stat. {exception}",
    'NlinkGT1': ("lstat nlink (number of hard links) count is >1. It is "
                 "{nlink}."),
    'LinkBroken': ('Symbolic link is broken. It points to "{target}" which '
                   'does not exist.'),
    'TempFile': ('File is a likely temporary file because its name '
                 'or extension ends with "{ending}".'),
    'MarkedBad': 'File is marked bad.',
    'NoID': 'Cannot read filesystem ID file "{filename}". {exception}',
    'NoL2File': 'Cannot read layer 2 metadata file "{filename}". {exception}',
    'L2MultVal': 'Multiple layer 2 "{property_}" property values exist.',
    'L2RepVal': 'Repetition of layer 2 "{property_}" property values.',
    'L2Extra': 'Layer 2 has these {num_extra_lines} extra lines: '
               '{extra_lines}',
#    'NoParentID': ('Cannot read parent\'s filesystem ID file "{filename}". '
#                   '{exception}'),
#    'ParentIDMismatch': ('Parent ID mismatch. The parent IDs provided by files'
#                         ' "{filename1}" ({parentid1}) and "{filename2}" '
#                         '({parentid2}) are not the same.'),
    'L2CRCNone': 'Layer 2 CRC is unavailable.',
    'L2SizeNone': 'Layer 2 size is missing.',
    'L2SizeMismatch': ("File size mismatch. Layer 2 size ({size_layer2})"
                       " doesn't match lstat size ({size_lstat})."),
    'NoL1File': 'Cannot read layer 1 metadata file "{filename}". {exception}',
    'L1Empty': 'Layer 1 metadata file "{filename}" is empty.',
    'L1Extra': 'Extra layer 1 lines detected.',
#    'L1Mismatch': ('Layer 1 mismatch. Layer 1 provided by files "{filename1}" '
#                   'and "{filename2}" are not the same.'),
    'L1BFIDBad': 'Layer 1 BFID ({bfid}) is invalid.',
    'L1BFIDNone': 'Layer 1 BFID is missing.',
    'NoL4File': ('Cannot read layer 4 metadata file "{filename}". '
                 '{exception}'),
    'L4Empty': 'Layer 4 metadata file "{filename}" is empty.',
    'L4Extra': 'Extra layer 4 lines detected.',
#    'L4Mismatch': ('Layer 4 mismatch. Layer 4 provided by files "{filename1}" '
#                   'and "{filename2}" are not the same.'),
    'L4BFIDNone': 'Layer 4 BFID is missing.',
    'FileInfoBadType': ('Enstore file info is not a dict. It is of type '
                        '"{type_}" and has value "{value}".'),
    'NoFileInfo': 'File is not in Enstore database.',
    'FileInfoBad': ('File status in file info provided by Enstore is not ok. '
                    'It is "{status}".'),
    'FileInfoPathNone': 'File path in Enstore file info is missing.',
    'FileInfoPNFSIDNone': 'PNFS ID in Enstore file info is missing.',
    'FileInfoPNFSIDBad': ('PNFS ID in file info provided by Enstore database '
                          'is invalid. It is "{pnfsid}".'),
    'VolInfoBadType': ('Enstore volume info is not a dict. It is of type '
                       '"{type_}" and has value "{value}".'),
    'NoVolInfo': 'Volume is not in Enstore database.',
    'VolInfoBad': ('Volume status in volume info provided by Enstore database '
                   'is not ok. It is "{status}".'),
    'BFIDMismatch': ('BFID mismatch. The BFIDs provided by layer 1 '
                     '({bfid_layer1}) and layer 4 ({bfid_layer4}) are not the '
                     'same.'),
    'MarkedDel': ('File is marked deleted by Enstore file info, but its entry '
                  'still unexpectedly exists in the filesystem.'),
    'TooRecent': ('Object was modified in the past one day since the scan '
                  'began.'),
    'Size0FileInfoOk': ('File is empty. Its lstat size is 0 and its layer 2 '
                        'size is {layer2_size}. Its info in Enstore is ok.'),
    'Size0FileInfoNo': ('File is empty. Its lstat size is 0 and its layer 2 '
                        'size is {layer2_size}. Its info in Enstore is not '
                        'ok. It presumably has no info in Enstore.'),
    'MultSFSPaths': ('Multiple paths were returned for PNFS ID {pnfsid}, '
                     'namely:  {paths}'),
    'L4VolLabelNone': 'Layer 4 volume label is missing.',
    'FileInfoVolLabelNone': 'Volume label in Enstore file info is missing.',
    'VolLabelMismatch': ("Volume label mismatch. File's layer 4 volume label "
                         "({volume_layer4}) doesn't match its Enstore file "
                         "info volume label ({volume_enstore})."),
    'L4LCNone': 'Layer 4 location cookie is missing.',
    'FileInfoLCNone': 'Location cookie in Enstore file info is missing.',
    'L4LCBad': 'Layer 4 location cookie ({lc_layer4}) is invalid.',
    'FileInfoLCBad': ('Location cookie in Enstore file info is invalid. It is '
                      '"{lc_enstore}".'),
    'LCMismatch': ("Current location cookie mismatch. File's current layer 4 "
                   "location cookie ({current_lc_layer4}) doesn't match its "
                   "current Enstore file info location cookie "
                   "({current_lc_enstore})."),
    'SizeNone': 'lstat size is missing.',
    'L4SizeNone': 'Layer 4 size is missing.',
    'FileInfoSizeNone': 'File size in Enstore file info is missing.',
    'SizeMismatch': ("File size mismatch. File sizes for file with layer 1 "
                     "BFID \"{bfid_layer1}\" provided by {size} don't all "
                     "match."),
    'L4FFNone': 'Layer 4 file family is missing.',
    'VolInfoFFNone': 'File family is missing in Enstore volume info.',
    'FFMismatch': ("File family mismatch. File's layer 4 file family "
                   "({ff_layer4}) doesn't match its Enstore volume info "
                   "file family ({ff_enstore})."),
    'VolInfoLibNone': 'Library is missing in Enstore volume info.',
    'VolInfoLibBad': ('Library ({library}) in Enstore volume info is not '
                      'recognized.'),
    'L4DriveNone': 'Layer 4 drive is missing.',
    'FileInfoDriveNone': 'Drive is missing in Enstore file info.',
    'DriveMismatch': ("Drive mismatch. File's layer 4 drive "
                      "({drive_layer4}) doesn't match its Enstore file info "
                      "drive ({drive_enstore})."),
    'CRCNone': 'CRC is missing in both layer 4 and Enstore file info.',
    'L4CRCNone': 'Layer 4 CRC is missing.',
    'FileInfoCRCNone': 'CRC is missing in Enstore file info.',
    'L4CRCMismatch': ("CRC mismatch. File's layer 4 CRC ({crc_layer4}) doesn't"
                      " match its Enstore file info CRC ({crc_enstore})."),
    'L2CRCMismatch': ("CRC mismatch. File's layer 2 CRC ({crc_layer2}) doesn't"
                      " match its Enstore file info 0-seeded CRC "
                      "({crc_enstore_0seeded}) or its Enstore file info "
                      "1-seeded CRC ({crc_enstore_1seeded})."),
    'L4PathNone': 'Layer 4 file path is missing.',
    'PathMismatch': ("File path mismatch. Normalized file paths for file with "
                     "layer 1 BFID \"{bfid_layer1}\" provided by {path} don't "
                     "all match. File may have been moved."),
    'L4PNFSIDNone': 'Layer 4 PNFS ID is missing.',
    'PNFSIDMismatch': ("PNFS ID mismatch. PNFS IDs for file with layer 1 BFID "
                       "\"{bfid_layer1}\" provided by {pnfsid} don't all "
                       "match. File may have been moved."),
    'FileInfoDelNone': 'The "deleted" field is missing in Enstore file info.',
    'FileInfoDelBad': ('The value of the "deleted" field ({deleted}) in '
                       'Enstore file info is not recognized.'),
    'MarkedCopy': 'File is marked as {copy_types} by Enstore file info.',
    }
    # Note: The type of each notice, i.e. warning, error, etc. is not noted in
    # the above dict because it can sometimes be dynamic.

    @memoize
    def get_num_items(self):
        """
        Return the total number of items to be scanned.

        This method is thread and process safe.

        .. note:: As implemented, the :class:`memoize` decorator will rerun the
            target method if and when the target is called upon program exit.
            This is one reason why this method is not implemented as a reused
            *property*.
        """

        # Note: settings['fs_root'] is known to be an absolute path (and not
        # have a trailing slash).

        if settings['fs_root'] == '/pnfs/fs/usr':

            # Use database

            # Note: This can take a few seconds.

            operation = ('select count(*) from t_inodes where itype in '
                         '(16384, 32768)')
            # itype=16384 indicates a directory.
            # itype=32768 indicates a file.
            return int(Chimera().fetchone(operation)['count'])

        else:

            self.validate_scan_location()

            # Use filesystem

            # Note: This can be slow, depending upon the number of directories.
            # This state is not normally expected.

            count = -1  # This allows exclusion of fs_root itself, as done in
                        # self.queue_items. Resultant minimum will still be 0.
            for _root, _dirs, files in os.walk(settings['fs_root']):
                count += (1 + len(files))  # 1 is for _root
            return count

    def queue_items(self):
        """Queue items for scanning."""

        # Provide methods locally
        fs_root = settings['fs_root']
        os_walker = os.walk(fs_root)
        os_path_join = os.path.join
        items_q_put = self.items_q.put
        put_item = lambda item: items_q_put(Item(item))
        put_file = lambda root, file_: put_item(os_path_join(root, file_))
        def put_files(root, files):
            for file_ in files:
                put_file(root, file_)

        # Provide checkpointing related variables
        resume_flag = settings['resume_scan']
        checkpoint = self.checkpoint.value  # Guaranteed to be a file path.

        def process_pre_checkpoint_dirs():

            if resume_flag and checkpoint:

                checkpoint_dir, checkpoint_file = os.path.split(checkpoint)

                generic_failure_msg =  ('The scan cannot be resumed for the '
                                        'specified output file.\n')

                # Perform some checks that are specific to the current Scanner.

                # Ensure checkpoint is in fs_root
                if not checkpoint.startswith(fs_root):
                    msg = ('Error: Checkpoint file "{0}" is outside of '
                           'scanning directory "{1}". {2}'
                           ).format(checkpoint, fs_root, generic_failure_msg)
                    exit(msg)

                # Ensure checkpoint file exists
                if not os.path.isfile(checkpoint):
                    msg = ('Error: Checkpoint file "{0}" does not exist. {1}'
                           ).format(checkpoint, generic_failure_msg)
                    exit(msg)

                # Skip items preceding checkpoint
                checkpoint_crossed = False
                for root, _dirs, files in os_walker:
                    if root != checkpoint_dir:
                        num_items_less = 1 + len(files)  # 1 is for root
                        self.num_items_total.value -= num_items_less
                    else:
                        # Note: Checkpoint is now a file in the current dir.
                        # As such, this state happens only once.
                        self.num_items_total.value -= 1  # for root
                        for file_ in files:
                            if checkpoint_crossed:
                                # Note: self.num_items_total.value should not
                                # be reduced here.
                                put_file(root, file_)
                            else:
                                self.num_items_total.value -= 1  # for file_
                                if file_ == checkpoint_file:
                                    checkpoint_crossed = True
                                    print('Checkpoint crossed.\n')
                        else:
                            if not checkpoint_crossed:
                                msg = ('Error: Checkpoint directory "{0}" was'
                                       ' not found to contain checkpoint file '
                                       '"{1}". {2}'
                                       ).format(checkpoint_dir,
                                                checkpoint_file,
                                                generic_failure_msg)
                                exit(msg)
                        break
                else:
                    if not checkpoint_crossed:
                        msg = ('Error: Checkpoint directory "{0}" was not '
                               'found. {1}'
                               ).format(checkpoint_dir, generic_failure_msg)
                        exit(msg)

        def process_post_checkpoint_dirs():

            if (not resume_flag) or (not checkpoint):

                # Queue only the files from only the initial root directory
                root, _dirs, files = next(os_walker)
                # put_item(root) is intentionally skipped, to not test fs_root.
                # Doing this here allows avoidance of a persistent "if"
                # statement.
                put_files(root, files)

            # Queue each remaining directory and file for scanning
            for root, _dirs, files in os_walker:
                put_item(root)
                put_files(root, files)

        # Process items
        process_pre_checkpoint_dirs()
        process_post_checkpoint_dirs()

    def validate_scan_location(self):
        """Validate the scan location root."""

        loc = settings['fs_root']

        if not os.path.isdir(loc):
            msg = 'Error: Scan root "{0}" is not a directory.'.format(loc)
            exit(msg)
        # Note: There is no reason to print loc if it is valid, as it is
        # already evident.

    def check_path_attr(self, item):
        """
        Check whether ``item`` has a path.

        :type item: :class:`Item`
        :arg item: object to check
        """

        try:
            item.path
        except AttributeError:
            raise CriticalNotice('NoPath')

    def check_is_storage_path(self, item):
        """
        Check whether ``item`` is an Enstore ``item``.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if not item.is_storage_path():
            raise CriticalNotice('NotStorage')

    def check_lstat(self, item):
        """
        Check whether ``item``'s stats are accessible.

        :type item: :class:`Item`
        :arg item: object to check
        """

        try:
            item.lstat
        except OSError as e:
            # If this occurs, refer to the get_stat method of the previous
            # implementation of this module for a possible modification to this
            # section.
            raise CriticalNotice('NoStat', exception=e.strerror or '')

    def check_file_nlink(self, item):
        """
        Check whether file has more than 1 hard links.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if item.is_file() and (item.lstat.st_nlink > 1):
            raise InfoNotice('NlinkGT1', nlink=item.lstat.st_nlink)
        # There is no usual reason for the link count to be greater than 1.
        # There have been cases where a move was aborted early and two
        # directory entries were left pointing to one i-node, but the i-node
        # only had a link count of 1 and not 2.  Since there may be legitimate
        # reasons for multiple hard links, it is not considered an error or a
        # warning.

    def check_link_target(self, item):
        """
        Check whether symbolic link is broken.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if item.is_link() and (not os.path.exists(item.path)):
            # Note: os.path.exists returns False for broken symbolic links.
            raise WarningNotice('LinkBroken',
                                target=os.path.realpath(item.path))

    def check_file_temp(self, item):
        """
        Check whether ``item`` is a temporary file.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if item.is_file():
            path = item.path
            endings = ('.nfs', '.lock', '_lock')
            for ending in endings:
                if path.endswith(ending):
                    raise InfoNotice('TempFile', ending=ending)

        # Note: If the item is a temporary file, this method reports the
        # corresponding ending string. This is why an "is_temp_file" method in
        # the Item class is not implemented or used insted.

    def check_file_bad(self, item):
        """
        Check whether ``item`` is a file that is marked bad.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if item.is_file_bad:
            raise InfoNotice('MarkedBad')

    def check_fsid(self, item):
        """
        Check whether ``item`` has an accessible filesystem ID.

        :type item: :class:`Item`
        :arg item: object to check
        """
        try:
            _fsid = item.fsid
        except (IOError, OSError) as e:
            raise CriticalNotice('NoID', filename=item.fsidname,
                                 exception=e.strerror or '')

#     def check_parentfsid(self, item):
#         """
#         Check file's parent's filesystem ID for its presence.
#
#         :type item: :class:`Item`
#         :arg item: object to check
#         """
#
#         if item.is_file():
#
#             # Check value for availability
#             #sources = 'parent_file', 'parent_dir'
#             sources = ('parent_file',)
#             parentfsids = {}
#             for source in sources:
#                 try:
#                     parentfsids[source] = item.parentfsid(source)
#                 except (IOError, OSError) as e:
#                     raise ErrorNotice('NoParentID',
#                                       filename=item.parentfsidname(source),
#                                       exception=e.strerror or '')
#
#             # Check values for consistency (never observed to fail)
#             source1, source2 = 'parent_file', 'parent_dir'
#             if ((source1 in parentfsids) and (source2 in parentfsids) and
#                 (parentfsids[source1] != parentfsids[source2])):
#                 raise ErrorNotice('ParentIDMismatch',
#                                   filename1=item.parentfsidname(source1),
#                                   parentid1=parentfsids[source1],
#                                   filename2=item.parentfsidname(source2),
#                                   parentid2=parentfsids[source2])

    def check_fslayer2lines(self, item):
        """
        Check whether a file's filesystem provided layer 2 is corrupted.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if item.is_file():
            try:
                _layer_lines = item.fslayerlines(2, 'filename')
            except (OSError, IOError) as e:
                if not (e.errno == errno.ENOENT):  # confirmed
                    raise ErrorNotice('NoL2File', filename=item.fslayername(2),
                                      exception=e.strerror or '')

    def check_fslayer2(self, item):
        """
        Check for inconsistencies in a file's filesystem provided layer 2.

        The check is performed only if the ``item`` is a file and if its layer
        2 is present. If its layer 2 is missing, this is not an error
        condition.

        :type item: :class:`Item`
        :arg item: object to check
        """

        def check_properties(properties):
            for property_, values in properties.items():
                if len(values) > 1:
                    notice = ErrorNotice('L2MultVal', property_=property_)
                    item.add_notice(notice)
                    if len(values) != len(set(values)):
                        notice = WarningNotice('L2RepVal', property_=property_)
                        item.add_notice(notice)

        # Do checks
        if item.is_file() and item.has_fslayer(2):

            layer_dict = item.fslayer2('filename')

            # Check properties
            try: properties = layer_dict['properties']
            except KeyError: pass
            else: check_properties(properties)

            # Check pools
            if not item.is_file_empty:
                try: pools = layer_dict['pools']
                except KeyError: pass
                else: InfoNotice('L2Extra', num_extra_lines=len(pools),
                                 extra_lines=pools)

    def check_fslayer2_crc(self, item):
        """
        Check whether a file's layer 2 CRC is available.

        The check is performed only if the ``item`` is a file and a HSM is not
        used.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if (item.is_file() and item.has_fslayer(2)
            and (item.fslayer2_property('hsm') == 'no')
            and (item.fslayer2_property('crc') is None)):

            raise WarningNotice('L2CRCNone')

    def check_fslayer2_size(self, item):
        """
        Check whether a file's layer 2 size is available.

        The check is performed only if the ``item`` is a file and a HSM is not
        used.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if (item.is_file() and item.has_fslayer(2)
            and (item.fslayer2_property('hsm') == 'no')
            and (item.fslayer2_property('length') is None)):
            raise WarningNotice('L2SizeNone')

    def check_fslayer2_size_match(self, item):
        """
        Conditionally check whether ``item``'s layer 2 and filesystem sizes
        match.

        The check is performed only if the ``item`` is a file and a HSM is not
        used.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if item.is_file() and (item.fslayer2_property('hsm') == 'no'):
            size_lstat = item.lstat.st_size  # int
            size_layer2 = item.fslayer2_property('length')  # int or None
            if ((size_layer2 is not None) and (size_lstat != size_layer2) and
                not (size_lstat==1 and size_layer2>2147483647)):
                # Not sure why the check below was done:
                # "not (size_lstat==1 and size_layer2>2147483647)"
                # Note that 2147483647 is 2GiB-1.
                raise ErrorNotice('L2SizeMismatch', size_layer2=size_layer2,
                                  size_lstat=size_lstat)

    def check_fslayer1lines(self, item):
        """
        Check whether file's filesystem provided layer 1 is corrupted.

        The check is performed only if the ``item`` is a non-recent file and if
        a HSM may be used.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if item.is_nonrecent_file and (item.fslayer2_property('hsm') != 'no'):
            try:
                layer_lines = item.fslayerlines(1, 'filename')
            except (OSError, IOError) as e:
                raise ErrorNotice('NoL1File', filename=item.fslayername(1),
                                  exception=e.strerror or '')
            else:
                if not layer_lines:
                    raise ErrorNotice('L1Empty', filename=item.fslayername(1))

    def check_fslayer1(self, item):
        """
        Check for inconsistencies in file's filesystem provided layer 1.

        The check is performed only if the ``item`` is a non-recent file and if
        its layer 1 is present. If its layer 1 is missing, an appropriate
        notice is raised by the :meth:`check_fslayer1lines` method as
        applicable.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if item.is_nonrecent_file and item.has_fslayer(1):

            layer1 = item.fslayer1('filename')

            # Check for extra lines
            if 'pools' in layer1:
                item.add_notice(WarningNotice('L1Extra'))

#                # Check for mismatch (never observed to fail)
#                if item.fslayer1('filename') != item.fslayer1('fsid'):
#                    raise ErrorNotice('L1Mismatch',
#                                      filename1=item.fslayername(1,'filename'),
#                                      filename2=item.fslayername(1,'fsid'))

            # Check BFID
            if not item.is_file_empty:
                bfid = item.fslayer1_bfid() or ''
                if not bfid:
                    raise ErrorNotice('L1BFIDNone')
                elif len(bfid) < 8:
                    raise ErrorNotice('L1BFIDBad', bfid=bfid)

    def check_fslayer4lines(self, item):
        """
        Check whether a file's filesystem provided layer 4 is corrupted.

        The check is performed only if the ``item`` is a non-recent file and if
        a HSM may be used.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if item.is_nonrecent_file and (item.fslayer2_property('hsm') != 'no'):
            try:
                layer_lines = item.fslayerlines(4, 'filename')
            except (OSError, IOError) as e:
                raise ErrorNotice('NoL4File', filename=item.fslayername(4),
                                  exception=e.strerror or '')
            else:
                if not layer_lines:
                    raise ErrorNotice('L4Empty', filename=item.fslayername(4))

    def check_fslayer4(self, item):
        """
        Check for inconsistencies in ``item``'s filesystem provided layer 4.

        The check is performed only if the ``item`` is a non-recent file and if
        its layer 4 is present. If its layer 4 is missing, an appropriate
        notice is raised by the :meth:`check_fslayer4lines` method as
        applicable.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if item.is_nonrecent_file and item.has_fslayer(4):

            layer4 = item.fslayer4('filename')

            # Check for extra lines
            if 'pools' in layer4:
                item.add_notice(WarningNotice('L4Extra'))

#                # Check for mismatch (never observed to fail)
#                if item.fslayer4('filename') != item.fslayer4('fsid'):
#                    raise ErrorNotice('L4Mismatch',
#                                      filename1=item.fslayername(4,'filename'),
#                                      filename2=item.fslayername(4,'fsid'))

            # Check BFID
            if (not item.is_file_empty) and (not item.fslayer4_bfid()):
                raise ErrorNotice('L4BFIDNone')

    def check_enstore_file_info(self, item):
        """
        Check for inconsistencies in ``item``'s Enstore provided file info.

        This check is performed only if the ``item`` is a non-recent file and
        its BFID is obtained.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if item.is_nonrecent_file:

            efi = item.enstore_file_info
            bfid_layer1 = item.fslayer1_bfid()  # Links Chimera with Enstore.

            if not isinstance(efi, dict):
                raise ErrorNotice('FileInfoBadType',
                                  bfid_layer1=bfid_layer1,
                                  type_=type(efi).__name__,
                                  value=efi)

            if efi.get('bfid'):  # Unsure if this line is necessary.

                if not item.is_enstore_file_info_ok:

                    if efi['status'][0] == enstore_errors.NO_FILE:
                        # Note: enstore_errors.NO_FILE == 'NO SUCH FILE/BFID'
                        raise ErrorNotice('NoFileInfo',
                                          bfid_layer1=bfid_layer1,)
                    else:
                        raise ErrorNotice('FileInfoBad',
                                          bfid_layer1=bfid_layer1,
                                          status=efi['status'])

                elif not item.is_file_deleted:
                    # This state is normal.

                    empty_values = ('', None, 'None')

                    if efi.get('pnfs_name0') in empty_values:
                        raise ErrorNotice('FileInfoPathNone',
                                          bfid_layer1=bfid_layer1)

                    efi_pnfsid = efi.get('pnfsid')
                    if efi_pnfsid in empty_values:
                        raise ErrorNotice('FileInfoPNFSIDNone',
                                          bfid_layer1=bfid_layer1)
                    elif not enstore_namespace.is_id(efi_pnfsid):
                        # Note: enstore_namespace.is_id expects a str.
                        raise ErrorNotice('FileInfoPNFSIDBad',
                                          bfid_layer1=bfid_layer1,
                                          pnfsid=efi_pnfsid)

    def check_enstore_volume_info(self, item):
        """
        Check for inconsistencies in ``item``'s Enstore provided volume info.

        This check is performed only if the ``item`` is a non-recent file and
        its volume name is obtained.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if item.is_nonrecent_file:

            evi = item.enstore_volume_info
            bfid_layer1 = item.fslayer1_bfid()  # Links Chimera with Enstore.

            if not isinstance(evi, dict):
                raise ErrorNotice('VolInfoBadType',
                                  bfid_layer1=bfid_layer1,
                                  type_=type(evi).__name__,
                                  value=evi)

            if evi.get('external_label'):  # Unsure if this line is necessary.

                if not item.is_enstore_volume_info_ok:

                    if evi['status'][0] == enstore_errors.NOVOLUME:
                        # enstore_errors.NOVOLUME = 'NOVOLUME'
                        raise ErrorNotice('NoVolInfo', bfid_layer1=bfid_layer1)
                    else:
                        raise ErrorNotice('VolInfoBad',
                                          bfid_layer1=bfid_layer1,
                                          status=evi['status'])

    def check_bfid_match(self, item):
        """
        Check for a mismatch in the file's layer 1 and layer 4 BFIDs.

        This check is performed only if the ``item`` is a file, is not a copy,
        is not marked deleted, and its layer 1 and 4 exist.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if (item.is_file() and (not item.is_file_a_copy) and
            (not item.is_file_deleted) and item.has_fslayer(1) and
            item.has_fslayer(4)):

            fslayer1_bfid = item.fslayer1_bfid()
            fslayer4_bfid = item.fslayer4_bfid()

            if fslayer1_bfid != fslayer4_bfid:
                raise ErrorNotice('BFIDMismatch',
                                  bfid_layer1=fslayer1_bfid,
                                  bfid_layer4=fslayer4_bfid)

    def check_file_deleted(self, item):
        """
        Check if file is marked deleted.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if item.is_file():

            deleted = item.enstore_file_info.get('deleted')

            if deleted is None:
                raise ErrorNotice('FileInfoDelNone')
            else:
                if (not item.is_file_a_copy) and (deleted=='yes'):
                    raise ErrorNotice('MarkedDel')
                if deleted not in ('yes', 'no'):
                    raise ErrorNotice('FileInfoDelBad', deleted=deleted)

    def check_recent(self, item):
        """
        Check if the file is recent.

        This check can help in the assessment of other notices.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if item.is_recent:
            raise InfoNotice('TooRecent')

    def check_empty(self, item):
        """
        Check if the file has :obj:`~os.lstat` size zero.

        This check is performed only if the ``item`` is a non-recent file.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if item.is_nonrecent_file and item.is_file_empty:
            fslayer2_size = item.fslayer2_property('length')
            # fslayer2_size can be 0, None, etc.
            if fslayer2_size is None: fslayer2_size = 'not present'
            NoticeType = InfoNotice if fslayer2_size==0 else ErrorNotice
            notice_key_suffix = 'Ok' if item.is_enstore_file_info_ok else 'No'
            notice_key = 'Size0FileInfo{0}'.format(notice_key_suffix)
            raise NoticeType(notice_key, layer2_size=fslayer2_size)

    def check_sfs_path(self, item):
        """
        Check if any of the PNFS IDs in Enstore for the current file has
        more than one path.

        :type item: :class:`Item`
        :arg item: object to check
        """

        # Note: This is very slow with PNFS, but is not too slow with Chimera.

        if item.is_file():
            for file_info in item.enstore_files_list_by_path:

                pnfs_name = file_info.get('pnfs_name0')
                pnfs_id = file_info.get('pnfsid')
                if (not pnfs_name) or (pnfs_id in ('', None, 'None')):
                    continue

                sfs_paths = item.sfs_paths(pnfs_name, pnfs_id)
                if len(sfs_paths) > 1:
                    item.add_notice(ErrorNotice('MultSFSPaths',
                                                pnfsid=pnfs_id,
                                                paths=', '.join(sfs_paths)))

    def check_volume_label(self, item):
        """
        Check file's layer 4 and Enstore volume labels for their presence and
        also for a mismatch.

        This check is performed only if the ``item`` is a file and is not a
        copy.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if item.is_file() and (not item.is_file_a_copy):

            # Retrieve values
            volume_layer4 = item.fslayer4('filename').get('volume')
            volume_enstore = item.enstore_file_info.get('external_label')

            # Check values for availability
            if item.has_fslayer(4) and (not volume_layer4):
                item.add_notice(ErrorNotice('L4VolLabelNone'))
            if item.is_enstore_file_info_ok and (not volume_enstore):
                item.add_notice(ErrorNotice('FileInfoVolLabelNone'))

            # Check values for consistency
            if (volume_layer4 and volume_enstore and
                (volume_layer4 != volume_enstore)):
                raise ErrorNotice('VolLabelMismatch',
                                  volume_layer4=volume_layer4,
                                  volume_enstore=volume_enstore)

    def check_location_cookie(self, item):
        """
        Check file's layer 4 and Enstore location cookies for their presence
        and also for a mismatch.

        This check is performed only if the ``item`` is a file and is not a
        copy.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if item.is_file() and (not item.is_file_a_copy):

            # Retrieve values

            layer4 = item.fslayer4('filename')
            efi = item.enstore_file_info

            lc_layer4 = layer4.get('location_cookie')
            lc_enstore = efi.get('location_cookie')

            lc_cur_layer4 = layer4.get('location_cookie_current')
            lc_cur_enstore = efi.get('location_cookie_current')

            is_lc = enstore_functions3.is_location_cookie

            # Check values for availability

            if item.has_fslayer(4):
                if not lc_layer4:
                    item.add_notice(ErrorNotice('L4LCNone'))
                elif not is_lc(lc_layer4):
                    item.add_notice(ErrorNotice('L4LCBad',
                                                lc_layer4=lc_layer4))

            if item.is_enstore_file_info_ok:
                if not lc_enstore:
                    item.add_notice(ErrorNotice('FileInfoLCNone'))
                elif not is_lc(lc_enstore):
                    item.add_notice(ErrorNotice('FileInfoLCBad',
                                                lc_enstore=lc_enstore))

            # Check values for consistency
            if (lc_cur_layer4 and lc_cur_enstore and
                (lc_cur_layer4 != lc_cur_enstore)):
                raise ErrorNotice('LCMismatch',
                                  current_lc_layer4=lc_cur_layer4,
                                  current_lc_enstore=lc_cur_enstore)

    def check_size(self, item):
        """
        Check file's lstat, layer 4 and Enstore file sizes for their presence
        and also for a mismatch.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if item.is_file():

            # Retrieve values
            sizes = item.sizes

            # Check values for availability
            if sizes['lstat'] is None:
                item.add_notice(ErrorNotice('SizeNone'))
            # Note: Layer 2 size is checked in the check_fslayer2_size and
            # check_fslayer2_size_match methods.
            if item.has_fslayer(4) and (sizes['layer 4'] is None):
                item.add_notice(ErrorNotice('L4SizeNone'))
            if item.is_enstore_file_info_ok and (sizes['Enstore'] is None):
                item.add_notice(ErrorNotice('FileInfoSizeNone'))

            # Check values for consistency
            num_unique_sizes = len(set(s for s in sizes.values() if
                                       (s is not None))) # Disambiguates from 0
            if num_unique_sizes > 1:
#                sizes = dict((b'size_{0}'.format(k),v) for k,v in
#                              sizes.items())
#                raise ErrorNotice('SizeMismatch', **sizes)
                raise ErrorNotice('SizeMismatch',
                                  bfid_layer1=item.fslayer1_bfid(),
                                  size=ReversibleDict(sizes))
                # Note: The `size` arg name above must remain singular because
                # this name is joined to the key names in its value. Refer to
                # the `Notice.to_exportable_dict.flattened_dict` function.

    def check_file_family(self, item):
        """
        Check file's layer 4 and Enstore file family for their presence and
        also for a mismatch.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if item.is_file():

            # Retrieve values
            ff_layer4 = item.fslayer4('filename').get('file_family')
            ff_enstore = item.enstore_volume_info.get('file_family')

            # Check values for availability
            if item.has_fslayer(4) and (not ff_layer4):
                item.add_notice(ErrorNotice('L4FFNone'))
            if item.is_enstore_volume_info_ok and (not ff_enstore):
                item.add_notice(ErrorNotice('VolInfoFFNone'))

            # Check values for consistency
            if (ff_layer4 and ff_enstore and
                (ff_enstore not in (ff_layer4,
                                    '{0}-MIGRATION'.format(ff_layer4),
                                    ff_layer4.partition('-MIGRATION')[0],
                                    ff_layer4.partition('_copy_')[0],))):
                raise ErrorNotice('FFMismatch', ff_layer4=ff_layer4,
                                  ff_enstore=ff_enstore)

#     def check_library(self, item):
#         """
#         Check file's Enstore library name for its presence and validity.
#
#         This check is performed only if the ``item`` is a file and its Enstore
#         volume info is ok.
#
#         :type item: :class:`Item`
#         :arg item: object to check
#         """
#
#         if item.is_file() and item.is_enstore_volume_info_ok:
#
#             try: library = item.enstore_volume_info['library']
#             except KeyError: raise ErrorNotice('VolInfoLibNone')
#             else:
#                 if (library and
#                     (library not in item.enstore.library_managers) and
#                     ('shelf' not in library)):
#                     raise ErrorNotice('VolInfoLibBad', library=library)

    def check_drive(self, item):
        """
        Check file's layer 4 and Enstore drives for their presence and also for
        a mismatch.

        This check is performed only if the ``item`` is a file and is not a
        copy.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if item.is_file() and (not item.is_file_a_copy):

            # Retrieve values
            drive_layer4 = item.fslayer4('filename').get('drive')
            drive_enstore = item.enstore_file_info.get('drive')

            # Check values for availability
            if item.has_fslayer(4) and (not drive_layer4):
                item.add_notice(WarningNotice('L4DriveNone'))
            if item.is_enstore_file_info_ok and (not drive_enstore):
                item.add_notice(WarningNotice('FileInfoDriveNone'))

            # Check values for consistency
            drive_enstore_excludes = (drive_layer4, 'imported', 'missing',
                                      'unknown:unknown')
            if (drive_layer4 and drive_enstore and
                (drive_enstore not in drive_enstore_excludes)):
                raise ErrorNotice('DriveMismatch', drive_layer4=drive_layer4,
                                  drive_enstore=drive_enstore)

    def check_crc(self, item):
        """
        Check file's layer 4 and Enstore CRCs for their presence and also for
        a mismatch.

        This check is performed only if the ``item`` is not an empty file.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if item.is_file() and (not item.is_file_empty):

            # Retrieve values
            crc_layer2 = item.fslayer2_property('crc')
            crc_layer4 = item.fslayer4('filename').get('crc')
            efi = item.enstore_file_info
            crc_enstore = crc_enstore_0seeded = efi.get('complete_crc')
            crc_enstore_1seeded = efi.get('complete_crc_1seeded')
            media_type = item.enstore_volume_info.get('media_type')

            # Check values for availability
            # Note: It is ok for crc_layer2 to be unavailable.
            if (item.has_fslayer(4) and (not crc_layer4) and
                item.is_enstore_file_info_ok and (not crc_enstore)):
                # Note: When crc_layer4 or crc_enstore are missing, they are
                # often missing together.
                item.add_notice(WarningNotice('CRCNone'))
            else:
                if item.has_fslayer(4) and (not crc_layer4):
                    item.add_notice(WarningNotice('L4CRCNone'))
                if item.is_enstore_file_info_ok and (not crc_enstore):
                    item.add_notice(WarningNotice('FileInfoCRCNone'))

            # Check values for consistency
            if (crc_layer2 and (crc_enstore_0seeded or crc_enstore_1seeded) and
                media_type and (media_type != 'null') and
                (crc_layer2 not in (crc_enstore_0seeded, crc_enstore_1seeded))):
                item.add_notice(ErrorNotice('L2CRCMismatch',
                                crc_layer2=crc_layer2,
                                crc_enstore_0seeded=crc_enstore_0seeded,
                                crc_enstore_1seeded=crc_enstore_1seeded))
            if crc_layer4 and crc_enstore and (crc_layer4 != crc_enstore):
                raise ErrorNotice('L4CRCMismatch', crc_layer4=crc_layer4,
                                  crc_enstore=crc_enstore)

    def check_path(self, item):
        """
        Check file's layer 4, Enstore, and filesystem provided paths for
        their presence and also for a mismatch.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if item.is_file() and (not item.is_file_a_copy):

            # Retrieve values
            paths = item.norm_paths

            # Check values for availability
            # Note: 'filesystem' and 'Enstore' paths are checked previously.
            if item.has_fslayer(4) and (not paths['layer 4']):
                item.add_notice(ErrorNotice('L4PathNone'))

            # Check values for consistency
            num_unique_paths = len(set(p for p in paths.values() if p))
            if num_unique_paths > 1:
                raise ErrorNotice('PathMismatch',
                                  bfid_layer1=item.fslayer1_bfid(),
                                  path=ReversibleDict(paths))
                # Note: The `path` arg name above must remain singular because
                # this name is joined to the key names in its value. Refer to
                # the `Notice.to_exportable_dict.flattened_dict` function.

    def check_pnfsid(self, item):
        """
        Check file's filesystem ID file, layer 4 and Enstore PNFS IDs for
        their presence and also for a mismatch.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if item.is_file():

            # Retrieve values
            pnfsids = {'ID-file': item.fsid,
                       'layer 4': item.fslayer4('filename').get('pnfsid'),
                       'Enstore': item.enstore_file_info.get('pnfsid'),
                       }

            # Check values for availability
            # Note: 'ID-file' and 'Enstore' PNFS IDs are checked previously.
            if item.has_fslayer(4) and (not pnfsids['layer 4']):
                item.add_notice(ErrorNotice('L4PNFSIDNone'))

            # Check values for consistency
            num_unique_paths = len(set(p for p in pnfsids.values() if p))
            if num_unique_paths > 1:
                raise ErrorNotice('PNFSIDMismatch',
                                  bfid_layer1=item.fslayer1_bfid(),
                                  pnfsid=ReversibleDict(pnfsids))
                # Note: The `pnfsid` arg name above must remain singular
                # because this name is joined to the key names in its value.
                # Refer to the `Notice.to_exportable_dict.flattened_dict`
                # function.

    def check_copy(self, item):
        """
        Check if file has any of the copy attributes set.

        This check is performed only if the ``item`` is a file and its Enstore
        file info is ok.

        :type item: :class:`Item`
        :arg item: object to check
        """

        if item.is_file() and item.is_enstore_file_info_ok:

            # Retrieve values
            efi = item.enstore_file_info

            # Identify positive values
            possible_copy_types = ('multiple', 'primary', 'migrated',
                                   'migrated_to')
            is_true = lambda v: v in ('yes', 'Yes', '1', 1, True)
            detected_copy_types = []
            for copy_type in possible_copy_types:
                key = 'is_{0}_copy'.format(copy_type)
                val = efi.get(key)
                if val and is_true(val):
                    copy_type = copy_type.replace('_', ' ')
                    detected_copy_types.append(copy_type)

            # Report positive values
            if detected_copy_types:
                detected_copy_types = [c.replace('_', ' ') for c in
                                       detected_copy_types]
                detected_copy_types = ['{0} copy'.format(c) for c in
                                       detected_copy_types]
                detected_copy_types = str(PrintableList(detected_copy_types))
                raise InfoNotice('MarkedCopy', copy_types=detected_copy_types)


class Item:
    """
    Return an object corresponding to a file or a directory.

    Before an instance is created:

    - The :attr:`enstore` class attribute must be set to an :class:`Enstore`
      class instance. This must be done individually in each process in which
      this :class:`Item` class is to be used.
    """

    start_time = time.time()
    enstore = None  # To be set individually in each process.
    chimera = Chimera()
    _cache_volume_info = settings['cache_volume_info']
    if _cache_volume_info:
        #volume_info_cache = multiprocessing.Manager().dict()
        volume_info_cache = MPSubDictCache()

    def __init__(self, item_path):
        """Initialize the ``item``.

        :type item_path: :obj:`str`
        :arg item_path: absolute filesystem path of the item.
        """

        self.path = item_path
        self.noticegrp = NoticeGrp(self.path)
        self.is_scanned = False

        self._cached_exceptions = {}

    def __repr__(self):
        """
        Return a string representation.

        This string allows the class instance to be reconstructed in another
        process.
        """

        return '{0}({1})'.format(self.__class__.__name__, repr(self.path))

    def __eq__(self, other):
        """
        Perform an equality comparison.

        :type other: :class:`Item`
        :arg other: object to compare.
        :rtype: :obj:`bool`
        """

        return (self.path==other.path)

    def __str__(self):
        """Return a string representation."""

        return self.path

    @staticmethod
    def _readfile(filename):
        """Return the stripped contents of the file corresponding to the
        specified filename."""
        return open(filename).read().strip()

    def add_notice(self, notice):
        """
        Add the provided ``notice`` to the group of notices associated with
        the ``item``.

        :type notice: :class:`Notice`
        :arg notice: object to add.
        """

        self.noticegrp.add_notice(notice)

    @memoize_property
    def dirname(self):
        """
        Return the directory name of the ``item``.

        :rtype: :obj:`str`
        """
        return os.path.dirname(self.path)

    @memoize_property
    def basename(self):
        """
        Return the base name of the ``item``.

        :rtype: :obj:`str`
        """
        return os.path.basename(self.path)

    @memoize_property
    def fsidname(self):
        """
        Return the name of the ID file.

        :rtype: :obj:`str`
        """
        return os.path.join(self.dirname, '.(id)({0})'.format(self.basename))

    @memoize
    def parentfsidname(self, source='parent_file'):
        """
        Return the name of the parent ID file.

        :type source: :obj:`str`
        :arg source: ``parent_file`` or ``parent_dir``.
        :rtype: :obj:`str`
        """
        if source == 'parent_file':
            return os.path.join(self.dirname,
                                '.(parent)({0})'.format(self.fsid))
        elif source == 'parent_dir':
            return self.__class__(self.dirname).fsidname
        else:
            msg = 'Invalid value for source: {0}'.format(source)
            raise ValueError(msg)

    @memoize
    def fslayername(self, layer_num, source='filename'):
        """
        Return the name of layer information file for the specified layer
        number.

        :type layer_num: :obj:`int`
        :arg layer_num: valid layer number
        :type source: :obj:`str`
        :arg source: ``filename`` or ``fsid``.
        :rtype: :obj:`str`
        """
        if source == 'filename':
            return os.path.join(self.dirname,
                                '.(use)({0})({1})'.format(layer_num,
                                                          self.basename))
        elif source == 'fsid':
            return os.path.join(self.dirname,
                                '.(access)({0})({1})'.format(self.fsid,
                                                             layer_num))
        else:
            msg = 'Invalid value for source: {0}'.format(source)
            raise ValueError(msg)

    @memoize
    def fslayerlines(self, layer_num, source):
        """
        Return a :obj:`list` containing the lines contained in the layer
        information file for the specified layer number.

        :type layer_num: :obj:`int`
        :arg layer_num: valid layer number
        :type source: :obj:`str`
        :arg source: ``filename`` or ``fsid``. It does not have a default value
            to allow memoization to work correctly.
        :rtype: :obj:`list`

        A sample returned :obj:`list` for layer 1 is::

            ['CDMS124711171800000']

        A sample returned :obj:`list` for layer 2 is::

            ['2,0,0,0.0,0.0', ':c=1:6d0f3ab9;h=yes;l=8192000000;']

        A sample returned :obj:`list` for layer 4 is::

            ['VON077', '0000_000000000_0001544', '635567027', 'volatile',
            '/pnfs/ilc4c/LOI/uu/uu_Runs_130.FNAL2.tar.gz', '',
            '003A0000000000000005E2E0', '', 'CDMS124655315900000',
            'stkenmvr204a:/dev/rmt/tps0d0n:1310050819', '954405925']

        Layers are unavailable for directories.

        The following exception will be raised if the layer information file is
        unavailable::

            IOError: [Errno 2] No such file or directory

        Error 2 is ENOENT.
        """

        # Re-raise cached exception if it exists
        key = ('fslayerlines', layer_num, source)
        try:
            raise self._cached_exceptions[key]
        except KeyError: pass

        # Read file
        fslayername = self.fslayername(layer_num, source)
        try:
            lines = open(fslayername).readlines()
        except Exception as e:
            self._cached_exceptions[key] = e
            raise

        lines = [ln.strip() for ln in lines]
        return lines

    @memoize
    def fslayer1(self, source):
        """
        Return a :obj:`dict` containing layer 1 information for a file.

        :type source: :obj:`str`
        :arg source: ``filename`` or ``fsid``. It does not have a default value
            to allow memoization to work correctly.
        :rtype: :obj:`dict`

        A sample returned :obj:`dict` is::

            {u'bfid': 'CDMS124711171800000'}

        An empty :obj:`dict` is returned in the event of an OS or IO exception.

        Layer 1 information is unavailable for directories.
        """
        layer = {}

        # Get lines
        try: fslayerlines = self.fslayerlines(1, source)
        except (OSError, IOError): return layer

        # Parse lines
        if fslayerlines:

            # Save BFID
            try: layer['bfid'] = fslayerlines[0]
            except IndexError: pass

            # Save anything found in any remaining lines
            pools = fslayerlines[1:]
            if pools: layer['pools'] = pools

        return layer

    @memoize
    def fslayer1_bfid(self, source='filename'):
        """
        Return the layer 1 value for BFID, or return :obj:`None` if
        unavailable.

        :type source: :obj:`str`
        :arg source: ``filename`` or ``fsid``
        :rtype: :obj:`str` or :obj:`None`
        """
        return self.fslayer1(source).get('bfid')

    @memoize
    def fslayer2(self, source):
        """
        Return a :obj:`dict` containing layer 2 information for a file.

        :type source: :obj:`str`
        :arg source: ``filename`` or ``fsid``. It does not have a default value
            to allow memoization to work correctly.
        :rtype: :obj:`dict`

        A sample returned dict is::

            {u'numbers': [u'2', u'0', u'0', u'0.0', u'0.0'],
             u'properties': {u'crc': [4000420189], u'length': [8192000000],
                             u'hsm': [u'yes']}}

        Each value in the ``properties`` :obj:`dict` is a :obj:`list`. This
        :obj:`list` can have more than one item.

        An empty :obj:`dict` is returned in the event of an OS or IO exception.

        Layer 2 information is unavailable for directories.
        """

        layer = {}

        # Get lines
        try: fslayerlines = self.fslayerlines(2, source)
        except (OSError, IOError): return layer

        # Parse lines
        if fslayerlines:

            # Save numbers
            try: numbers = fslayerlines[0]
            except IndexError: pass
            else:
                layer['numbers'] = numbers.split(',')  # (line 1)

            # Save properties
            try: properties = fslayerlines[1]  # (line 2)
            except IndexError: pass
            else:
                pkm = { # Mapped names for Property Keys.
                       'c': 'crc', 'h': 'hsm', 'l': 'length'}
                pvt = { # Transforms applied to Property Values.
                       'crc': lambda s: int(s.split(':')[1], 16),
                       'length': int,
#                       'hsm': lambda h: {'yes': True,
#                                         'no': False}.get(h),  #ignore
                       }

                # Sample: ':c=1:ee71915d;h=yes;l=8192;'
                if properties[0] == ':': properties = properties[1:]
                # Sample: 'c=1:ee71915d;h=yes;l=8192;'
                properties = sorted(p.split('=',1) for p in
                                    properties.split(';') if p)
                # Sample: [['c', '1:ee71915d'], ['h', 'yes'], ['l', '8192']]
                properties = dict((k,[v[1] for v in v])
                                  for k,v in
                                  itertools.groupby(properties, lambda p: p[0]))
                # This transforms the list into a dict in a way that if
                # multiple values exist for a key, they are all noted in the
                # list. Duplicate values are noted as well. Missing items are
                # possible.
                # Sample: {'h': ['yes'], 'c': ['1:ee71915d'], 'l': ['8192']}
                properties = dict((pkm.get(k,k), v) for k,v in
                                  properties.items())
                # Sample: {'hsm': ['yes'], 'crc': ['1:ee71915d'],
                #          'length': ['8192']}
                properties = dict((k, [(pvt[k](v) if pvt.get(k) else v) for v
                                       in vlist])
                                  for k,vlist in properties.items())
                # Sample: {'hsm': ['yes'], 'crc': [4000420189],
                #          'length': [8192]}
                layer['properties'] = properties

            # Save anything found in any remaining lines
            pools = fslayerlines[2:]
            if pools: layer['pools'] = pools

        return layer

    @memoize
    def fslayer2_property(self, l2property, source='filename'):
        """
        Return the value for the specified layer 2 property, or return
        :obj:`None` if unavailable.

        :type l2property: :obj:`str`
        :arg l2property: typically ``crc``, ``hsm``, or ``length``.
        :type source: :obj:`str`
        :arg source: ``filename`` or ``fsid``
        """
        return self.fslayer2(source).get('properties', {}).get(l2property,
                                                               [None])[0]

    @memoize
    def fslayer4(self, source):
        """
        Return a :obj:`dict` containing layer 4 information for a file.

        :type source: :obj:`str`
        :arg source: ``filename`` or ``fsid``. It does not have a default value
            to allow memoization to work correctly.
        :rtype: :obj:`dict`

        A sample returned dict is::

            {u'original_name': '/pnfs/BDMS/tariq/p2_004/p2_004_struc.LN22',
             u'drive': 'stkenmvr214a:/dev/rmt/tps0d0n:1310051193',
             u'volume': 'VOO007', 'crc': 1253462682, u'file_family': 'BDMS',
             u'location_cookie': '0000_000000000_0000454',
             u'pnfsid': '000E00000000000000013248',
             u'bfid': 'CDMS123258695100001',
             'size': 294912,
             'location_cookie_list': ['0000', '000000000', '0000454'],
             'location_cookie_current': '0000454',}

        An empty :obj:`dict` is returned in the event of an OS or IO exception.

        Layer 4 information is unavailable for directories.
        """

        layer = {}

        # Get lines
        try: fslayerlines = self.fslayerlines(4, source)
        except (OSError, IOError): return layer

        # Parse lines
        if fslayerlines:

            keys = ('volume', 'location_cookie', 'size', 'file_family',
                    'original_name', None, 'pnfsid', None, 'bfid', 'drive',
                    'crc',)
            transforms = \
                {'crc': lambda s: int(long(s)),
                 'size': lambda s: int(long(s)),
                 # Note that "int(long(s))" covers strings such as '123L' also.
                 }

            # Parse known lines
            for i, k in enumerate(keys):
                if k is not None:
                    try: layer[k] = fslayerlines[i]
                    except IndexError: break

            # Transform as applicable
            for k, tr_func in transforms.items():
                if k in layer:
                    layer[k] = tr_func(layer[k])

            # Parse extra lines
            pools = fslayerlines[len(keys):]
            if pools: layer['pools'] = pools

            # Add calculated fields
            # Also see similar section in `file_info` method.
            try:
                layer['location_cookie_list'] = \
                    layer['location_cookie'].split('_')
            except KeyError: pass
            else:
                try:
                    layer['location_cookie_current'] = \
                          layer['location_cookie_list'][-1]
                except IndexError: pass

        return layer

    @memoize
    def fslayer4_bfid(self, source='filename'):
        """
        Return the layer 4 value for BFID, or return :obj:`None` if
        unavailable.

        :type source: :obj:`str`
        :arg source: ``filename`` or ``fsid``
        :rtype: :obj:`str` or :obj:`None`
        """
        return self.fslayer4(source).get('bfid')

    @memoize
    def has_fslayer(self, layer_num, source='filename'):
        """
        Return a :obj:`bool` indicating whether the specified layer number is
        available.

        :type source: :obj:`str`
        :arg source: ``filename`` or ``fsid``
        :rtype: :obj:`bool`
        """

        attr = 'fslayer{0}'.format(layer_num)
        getter = getattr(self, attr, {})
        return bool(getter(source))

    @memoize_property
    def fsid(self):
        """
        Return the filesystem ID.

        This is available for both files and directories.

        :rtype: :obj:`str`

        Example: ``00320000000000000001D6B0``
        """

        # Re-raise cached exception if it exists
        key = 'fsid'
        try: raise self._cached_exceptions[key]
        except KeyError: pass

        # Read file
        try:
            return self._readfile(self.fsidname)
        except Exception as e:
            self._cached_exceptions[key] = e
            raise

    @memoize
    def parentfsid(self, source):
        """
        Return the parent's filesystem ID.

        :type source: :obj:`str`
        :arg source: ``filename`` or ``fsid``. It does not have a default value
            to allow memoization to work correctly.
        :rtype: :obj:`str`

        This is available for both files and directories.
        """

        # Re-raise cached exception if it exists
        key = ('parentfsid', source)
        try: raise self._cached_exceptions[key]
        except KeyError: pass

        # Read file
        filename = self.parentfsidname(source)
        try:
            return self._readfile(filename)
        except Exception as e:
            self._cached_exceptions[key] = e
            raise

    @memoize_property
    def lstat(self):
        """
        Return :obj:`~os.lstat` info.

        Sample::

            posix.stat_result(st_mode=33188, st_ino=1275083688, st_dev=24L,
                              st_nlink=1, st_uid=13194, st_gid=1623,
                              st_size=8192000000, st_atime=1248570004,
                              st_mtime=1153410176, st_ctime=1153407484)
        """
        return os.lstat(self.path)

    @memoize_property
    def is_recent(self):
        """Return a :obj:`bool` indicating whether the ``item`` was modified
        within the past one day since the scan began."""
        return (self.start_time - self.lstat.st_mtime) < 86400

    @memoize_property
    def is_nonrecent(self):
        """Return a :obj:`bool` indicating whether the ``item`` was modified
        before the past one day since the scan began."""
        return (not self.is_recent)

    @memoize_property
    def st_mode(self):
        """
        Return the protection mode.

        :rtype: :obj:`int`
        """
        return self.lstat.st_mode

    @memoize
    def is_file(self):
        """
        Return a :obj:`bool` indicating whether the ``item`` is a regular file.

        Given that :obj:`~os.lstat` is used, this returns :obj:`False` for
        symbolic links.
        """
        return stat.S_ISREG(self.st_mode)

    @memoize_property
    def is_recent_file(self):
        """Return a :obj:`bool` indicating whether the ``item`` is a file that
        was modified within the past one day since the scan began."""
        return (self.is_file() and self.is_recent)

    @memoize_property
    def is_nonrecent_file(self):
        """Return a :obj:`bool` indicating whether the ``item`` is a file that
        was modified before the past one day since the scan began."""
        return (self.is_file() and self.is_nonrecent)

    @memoize_property
    def is_file_a_copy(self):
        """Return a :obj:`bool` indicating whether the ``item`` is a file that
        is a copy."""
        is_true = lambda v: v in ('yes', 'Yes', '1', 1, True)
        is_true2 = lambda k: is_true(self.enstore_file_info.get(k))
        return (self.is_file() and
                (is_true2('is_multiple_copy') or is_true2('is_migrated_copy')))

    @memoize_property
    def is_file_bad(self):
        """Return a :obj:`bool` indicating whether the ``item`` is a file that
        is marked bad."""
        return (self.is_file() and
                (self.path.startswith('.bad') or self.path.endswith('.bad')))

    @memoize_property
    def is_file_deleted(self):
        """Return a :obj:`bool` indicating whether the ``item`` is a deleted
        file."""
        return (self.is_file() and
                (self.enstore_file_info.get('deleted')=='yes'))

    @memoize_property
    def is_file_empty(self):
        """Return a :obj:`bool` indicating whether the ``item`` is a file with
        size 0."""
        return (self.is_file() and (self.lstat.st_size==0))

    @memoize
    def is_dir(self):
        """
        Return a :obj:`bool` indicating whether the ``item`` is a directory.

        Given that :obj:`~os.lstat` is used, this returns :obj:`False` for
        symbolic links.
        """
        return stat.S_ISDIR(self.st_mode)

    @memoize
    def is_link(self):
        """
        Return a :obj:`bool` indicating whether the ``item`` is a symbolic
        link.

        .. Given that :obj:`~os.lstat` is used, this returns :obj:`True` for
            symbolic links.
        """
        return stat.S_ISLNK(self.st_mode)

    @memoize
    def is_storage_path(self):
        """Return a :obj:`bool` indicating whether the ``item`` is a path in
        the Enstore namespace."""
        return bool(enstore_namespace.is_storage_path(self.path,
                                                      check_name_only=1))

#    is_access_name_re = re.compile("\.\(access\)\([0-9A-Fa-f]+\)")
#    @memoize
#    def is_access_name(self):
#        return bool(re.search(self.is_access_name_re, self.path_basename))

    @memoize_property
    def enstore_file_info(self):
        """
        Return the available file info from the Enstore info client.

        This is returned for the layer 1 BFID.

        :rtype: :obj:`dict`

        Sample::

            {'storage_group': 'astro', 'uid': 0,
             'pnfs_name0': '/pnfs/fs/usr/astro/fulla/fulla.gnedin.026.tar',
             'library': 'CD-LTO4F1', 'package_id': None,
             'complete_crc': 3741678891L, 'size': 8192000000L,
             'external_label': 'VOO732', 'wrapper': 'cpio_odc',
             'package_files_count': None, 'active_package_files_count': None,
             'gid': 0, 'pnfsid': '00320000000000000001DF20',
             'archive_mod_time': None, 'file_family_width': None,
             'status': ('ok', None), 'deleted': 'no', 'archive_status': None,
             'cache_mod_time': None, 'update': '2009-07-25 23:57:23.222394',
             'file_family': 'astro',
             'location_cookie': '0000_000000000_0000150',
             'cache_location': None, 'original_library': None,
             'bfid': 'CDMS124858424200000', 'tape_label': 'VOO732',
             'sanity_cookie': (65536L, 1288913660L), 'cache_status': None,
             'drive': 'stkenmvr211a:/dev/rmt/tps0d0n:1310051081',
             'location_cookie_list': ['0000', '000000000', '0000150'],
             'location_cookie_current': '0000150',
             'complete_crc_1seeded': 2096135468L,}

        Sample if BFID is :obj:`None`::

            {'status': ('KEYERROR', 'info_server: key bfid is None'),
             'bfid': None}

        Sample if BFID is invalid::

            {'status': ('WRONG FORMAT', 'info_server: bfid 12345 not valid'),
             'bfid': '12345'}
        """

        bfid = self.fslayer1_bfid()
        file_info = self.enstore.info_client.bfid_info(bfid)

        # Update status field as necessary
        if isinstance(file_info, dict) and ('status' not in file_info):
            file_info['status'] = (enstore_errors.OK, None)
            # Note: enstore_errors.OK == 'ok'

        # Add calculated fields
        # Also see similar section in `fslayer4` method.

        try:
            file_info['location_cookie_list'] = \
                file_info['location_cookie'].split('_')
        except KeyError: pass
        else:
            try:
                file_info['location_cookie_current'] = \
                      file_info['location_cookie_list'][-1]
            except IndexError: pass

        try:
            crc, size = file_info['complete_crc'], file_info['size']
        except KeyError: pass
        else:
            #file_info['complete_crc_0seeded'] = crc  # not necessary
            if (crc is not None) and (size is not None):
                file_info['complete_crc_1seeded'] = \
                    enstore_checksum.convert_0_adler32_to_1_adler32(crc, size)
            else:
                file_info['complete_crc_1seeded'] = None

        return file_info

    @memoize_property
    def is_enstore_file_info_ok(self):
        """Return a :obj:`bool` indicating whether Enstore file info is ok or
        not."""
        return bool(enstore_errors.is_ok(self.enstore_file_info))

    @memoize_property
    def enstore_volume_info(self):
        """
        Return the available volume info from the Enstore info client.

        If volume caching is enabled, volume info may be cached and may be
        returned from cache if possible.

        :rtype: :obj:`dict`

        Sample (with no keys deleted)::

            {'comment': ' ', 'declared': 1217546233.0, 'blocksize': 131072,
             'sum_rd_access': 4, 'library': 'CD-LTO4F1',
             'si_time': [1317155514.0, 1246553166.0], 'wrapper': 'cpio_odc',
             'deleted_bytes': 77020524269L, 'user_inhibit': ['none', 'none'],
             u'storage_group': 'ilc4c', 'system_inhibit': ['none', 'full'],
             'external_label': 'VON077', 'deleted_files': 214,
             'remaining_bytes': 598819328L, 'sum_mounts': 149,
             'capacity_bytes': 858993459200L, 'media_type': 'LTO4',
             'last_access': 1331879563.0, 'status': ('ok', None),
             'eod_cookie': '0000_000000000_0001545', 'non_del_files': 1556,
             'sum_wr_err': 0, 'unknown_files': 10, 'sum_wr_access': 1544,
             'active_bytes': 746796833291L,
             'volume_family': 'ilc4c.volatile.cpio_odc',
             'unknown_bytes': 7112706371L, 'modification_time': 1246553159.0,
             u'file_family': 'volatile', 'write_protected': 'y',
             'sum_rd_err': 0, 'active_files': 1330,
             'first_access': 1234948627.0}

        Sample (with unneeded keys deleted)::

            {'library': 'CD-LTO4F1', 'wrapper': 'cpio_odc',
             'deleted_bytes': 0L, 'user_inhibit': ['none', 'none'],
             u'storage_group': 'ilc4c', 'system_inhibit': ['none', 'full'],
             'external_label': 'VON778', 'deleted_files': 0,
             'media_type': 'LTO4', 'status': ('ok', None), 'unknown_files': 0,
             'active_bytes': 818291877632L,
             'volume_family': 'ilc4c.static.cpio_odc', 'unknown_bytes': 0L,
             u'file_family': 'static', 'active_files': 411}

        Sample if volume is :obj:`None`::

            {'status': ('KEYERROR', 'info_server: key external_label is None'),
             'work': 'inquire_vol', 'external_label': None}

        Sample if volume is invalid::

            {'status': ('WRONG FORMAT', 'info_server: bfid 12345 not valid'),
             'bfid': '12345'}
        """

        # Get volume name
        volume = self.enstore_file_info.get('external_label')
        if volume == '':
            # This is done because self.enstore.info_client.inquire_vol('')
            # hangs.
            volume = None

        # Conditionally try returning cached volume info
        if self._cache_volume_info:
            try: return self.volume_info_cache[volume]
            except KeyError: volume_is_cacheable = True
        else: volume_is_cacheable = False

        # Get volume info from Enstore info client
        volume_info = self.enstore.info_client.inquire_vol(volume)
        if isinstance(volume_info, dict) and ('status' not in volume_info):
            volume_info['status'] = (enstore_errors.OK, None)
            # Note: enstore_errors.OK == 'ok'

        # Add calculated volume info keys
        try: volume_family = volume_info['volume_family']
        except KeyError: pass
        else:
            calculated_keys = ('storage_group', 'file_family', 'wrapper')
            # Note: 'wrapper' may very well already exist.
            for k in calculated_keys:
                if k not in volume_info:
                    getter = getattr(enstore_volume_family,
                                     'extract_{0}'.format(k))
                    volume_info[k] = getter(volume_family)

        # Conditionally process and cache volume info
        if volume_is_cacheable and (volume not in self.volume_info_cache):

            # Remove unneeded volume info keys, in order to reduce its memory
            # usage. An alternate approach, possibly even a better one, is to
            # use a whitelist instead of a blacklist.
            unneeded_keys = (
                             'blocksize',
                             'capacity_bytes',
                             'comment',
                             'declared',
                             'eod_cookie',
                             'first_access',
                             'last_access',
                             'modification_time',
                             'non_del_files',
                             'remaining_bytes',
                             'si_time',
                             'sum_mounts',
                             'sum_rd_access',
                             'sum_rd_err',
                             'sum_wr_access',
                             'sum_wr_err',
                             'write_protected',
                             )
            for k in unneeded_keys:
                try: del volume_info[k]
                except KeyError: pass

            # Cache volume info
            self.volume_info_cache[volume] = volume_info

        return volume_info

    @memoize_property
    def is_enstore_volume_info_ok(self):
        """Return a :obj:`bool` indicating whether Enstore volume info is ok or
        not."""
        return bool(enstore_errors.is_ok(self.enstore_volume_info))

    @memoize_property
    def enstore_files_by_path(self):
        """
        Return a :obj:`dict` containing information about the list of known
        files in Enstore for the current path.

        :rtype: :obj:`dict`

        Sample::

            {'status': ('ok', None),
             'r_a': (('131.225.13.10', 59329), 21L,
                     '131.225.13.10-59329-1347478604.303243-2022-47708927252656'),
             'pnfs_name0': '/pnfs/fs/usr/astro/idunn/rei256bin.015.tar',
             'file_list':
             [{'storage_group': 'astro', 'uid': 0,
               'pnfs_name0': '/pnfs/fs/usr/astro/idunn/rei256bin.015.tar',
               'library': 'shelf-CD-9940B', 'package_id': None,
               'complete_crc': 2995796126L, 'size': 8192000000L,
               'external_label': 'VO9502', 'wrapper': 'cpio_odc',
               'package_files_count': None, 'active_package_files_count': None,
               'gid': 0, 'pnfsid': '00320000000000000001CF38',
               'archive_status': None, 'file_family_width': None,
               'deleted': 'yes', 'archive_mod_time': None,
               'cache_mod_time': None, 'update': '2009-09-27 15:57:10.011141',
               'file_family': 'astro',
               'location_cookie': '0000_000000000_0000020',
               'cache_location': None,
               'original_library': None, 'bfid': 'CDMS113926889300000',
               'tape_label': 'VO9502', 'sanity_cookie': (65536L, 2312288512L),
               'cache_status': None,
               'drive': 'stkenmvr36a:/dev/rmt/tps0d0n:479000032467'},
              {'storage_group': 'astro', 'uid': 0,
               'pnfs_name0': '/pnfs/fs/usr/astro/idunn/rei256bin.015.tar',
               'library': 'CD-LTO4F1', 'package_id': None,
               'complete_crc': 2995796126L, 'size': 8192000000L,
               'external_label': 'VOO732', 'wrapper': 'cpio_odc',
               'package_files_count': None, 'active_package_files_count': None,
               'gid': 0, 'pnfsid': '00320000000000000001CF38',
               'archive_status': None, 'file_family_width': None,
               'deleted': 'no', 'archive_mod_time': None,
               'cache_mod_time': None, 'update': '2009-07-25 23:53:46.278724',
               'file_family': 'astro',
               'location_cookie': '0000_000000000_0000149',
               'cache_location': None, 'original_library': None,
               'bfid': 'CDMS124858402500000', 'tape_label': 'VOO732',
               'sanity_cookie': (65536L, 2312288512L), 'cache_status': None,
               'drive': 'stkenmvr211a:/dev/rmt/tps0d0n:1310051081'}]}
        """
        return self.enstore.info_client.find_file_by_path(self.path)

    @memoize_property
    def is_enstore_files_by_path_ok(self):
        """Return a :obj:`bool` indicating whether the list of Enstore provided
        files for the current path is ok or not."""
        return bool(enstore_errors.is_ok(self.enstore_files_by_path))

    @memoize_property
    def enstore_files_list_by_path(self):
        """
        If the :obj:`list` of Enstore provided files for the current path is
        ok, return this :obj:`list`, otherwise an empty :obj:`list`.

        :rtype: :obj:`list`

        For a sample, see the value of the ``file_list`` key in the sample
        noted for :attr:`enstore_files_by_path`.
        """
        if self.is_enstore_files_by_path_ok:
            enstore_files_by_path = self.enstore_files_by_path
            try:
                return enstore_files_by_path['file_list']
            except KeyError:
                return [enstore_files_by_path]  # Unsure of correctness.
        return []

    @memoize_property
    def _sfs(self):
        """Return the :class:`~enstore_namespace.StorageFS` instance for the
        current path."""
        return enstore_namespace.StorageFS(self.path)

    @memoize_property
    def sfs_pnfsid(self):
        """
        Return the PNFS ID for the current file as provided by
        :class:`~enstore_namespace.StorageFS`.

        :rtype: :obj:`str`
        """
        return self._sfs.get_id(self.path)

    @memoize
    def sfs_paths(self, filepath, pnfsid):
        """
        Return the paths for the indicated file path and PNFS ID, as
        provided by :class:`~enstore_namespace.StorageFS`.

        :type filepath: :obj:`str`
        :arg filepath: Absolute path of file, as provided by
            :class:`~enstore_namespace.StorageFS`.
        :type pnfsid: :obj:`str`
        :arg pnfsid: PNFS ID of file, as provided by
            :class:`~enstore_namespace.StorageFS`.
        """
        try:
            sfs_paths = self._sfs.get_path(id=pnfsid,
                                           directory=os.path.dirname(filepath))
        # Note: This was observed to _not_ work with a common StorageFS
        # instance, which is why a file-specific instance is used. The `dir`
        # argument was also observed to be required.
        except (OSError, ValueError):  # observed exceptions
            sfs_paths = []
        else:
            sfs_paths = sorted(set(sfs_paths))
        return sfs_paths

    @memoize_property
    def sizes(self):
        """
        Return a :obj:`dict` containing all available file sizes for the
        current file.

        :rtype: :obj:`dict`

        The available keys in the returned :obj:`dict` are ``lstat``,
        ``layer 2``, ``layer 4``, and ``Enstore``. Each value in the
        :obj:`dict` is an :obj:`int` or is :obj:`None` if unavailable.
        """

        return {'lstat': self.lstat.st_size,
                'layer 2': self.fslayer2_property('length'),
                'layer 4': self.fslayer4('filename').get('size'),
                'Enstore': self.enstore_file_info.get('size'),
                }

    @memoize_property
    def norm_paths(self):
        """
        Return a :obj:`dict` containing all available paths for the current
        file.

        The paths are normalized to remove common mount locations such as
        ``/pnfs/fs/usr``, etc.

        The available keys in the returned :obj:`dict` are ``filesystem``,
        ``layer 4``, and ``Enstore``. A value in the :obj:`dict` may be
        :obj:`None` if unavailable.
        """

        # Retrieve values
        paths = {'filesystem': self.path,
                 'layer 4': self.fslayer4('filename').get('original_name'),
                 'Enstore': self.enstore_file_info.get('pnfs_name0'),
                 }

        # Normalize values
        variations = ('/pnfs/fnal.gov/usr/',
                      '/pnfs/fs/usr/',
                      '/pnfs/',
                      '/chimera/',
                      )
        for path_key, path_val in paths.items():
            if path_val:
                for variation in variations:
                    if variation in path_val:
                        paths[path_key] = path_val.replace(variation,
                                                           '/<pnfs>/', 1)
                        # Note: The variation in question can occur anywhere
                        # in the path, and not just at the start of the path.
                        # str.startswith is not used for this reason.
                        break

        return paths


class Notice(Exception):
    """Provide an :obj:`~exceptions.Exception` representing a single notice."""

    _notices = {'Test': 'Testing "{test}".',}  # Updated externally, specific
    #-->                                       # to current scanner.

    @classmethod
    def update_notices(cls, notices):
        """
        Register the provided notices.

        :type notices: :obj:`dict`
        :arg notices: This is a :obj:`dict` containing notices to register. Its
            keys are compact string identifiers. Its values are corresponding
            string message templates. For example::

                {'NoL1File': ('Cannot read layer 1 metadata file "{filename}".'
                              '{exception}'),}

            In the above example, ``filename`` and ``exception`` represent
            keyword arguments whose values will be used to :obj:`~str.format`
            the message template.
        """

        cls._notices.update(notices)

    def __init__(self, key, **kwargs):
        """
        Return an :obj:`~exceptions.Exception` containing a single notice for
        the provided message arguments.

        Before any instance is created, the :meth:`update_notices` classmethod
        must have been called at least once.

        :type key: :obj:`str`
        :arg key: This refers to the identifying-type of the notice. It must
            have been previously registered using the :meth:`update_notices`
            classmethod.
        :rtype: :class:`Notice`

        Additionally provided keyword arguments are used to :obj:`~str.format`
        the corresponding message template. This template must have been
        previously provided together with the ``key`` using the
        :meth:`update_notices` classmethod.

        All arguments are also included in a :obj:`dict` representation of the
        notice.
        """

        self.key = key
        self._kwargs = kwargs

        #code = self.hashtext(self.key)
        message_template = self._notices[self.key]
        message = message_template.format(**self._kwargs).strip()
        self._level = self.__class__.__name__.rpartition('Notice')[0].upper()
        self._level = self._level or 'INFO'
        message = '{0} ({1}): {2}'.format(self._level, self.key, message)
        Exception.__init__(self, message)

    @staticmethod
    def hashtext(text):
        """
        Return a four-hexadecimal-digit string hash of the provided text.

        :type text: :obj:`str`
        :arg text: Text to hash.
        :rtype: :obj:`str`
        """
        hash_ = hashlib.md5(text).hexdigest()[:4]
        hash_ = '0x{0}'.format(hash_)
        return hash_

    @classmethod
    def print_notice_templates(cls):
        """Print all notice templates."""
        #notice_str = lambda k,v: '{0} ({1}): {2}'.format(cls.hashtext(k), k, v)
        notice_str = lambda k, v: '{0}: {1}'.format(k, v)
        notices = (notice_str(k,v) for k, v in
                   sorted(cls._notices.items()) if k!='Test')
        for n in notices: print(n)

    def __repr__(self):
        """
        Return a string representation.

        This string allows the class instance to be reconstructed in another
        process.
        """

        repr_ = '{0}({1}, **{2})'.format(self.__class__.__name__,
                                         repr(self.key), repr(self._kwargs))
        return repr_

    def __eq__(self, other):
        """
        Perform an equality comparison.

        :type other: :class:`Notice`
        :arg other: object to compare.
        :rtype: :obj:`bool`
        """

        return (str(self)==str(other) and self._level==other._level and
                self.key==other.key and self._kwargs==other._kwargs)

    def to_dict(self):
        """
        Return a :obj:`dict` which describes the notice.

        :rtype: :obj:`dict`

        The returned :obj:`dict` can be used to reconstruct this
        :class:`Notice` instance using the :meth:`from_dict` method.
        """

        return {self.key: {'level': self._level,
                           'args': self._kwargs},
                }

    def to_exportable_dict(self):
        """
        Return an exportable :obj:`dict` which describes the notice.

        :rtype: :obj:`dict`

        The returned :obj:`dict` cannot be used to reconstruct this
        :class:`Notice` instance using the :meth:`from_dict` method.
        """

        def flatten_dict(d):
            """
            Return a flattened version of a dict.

            This is based on http://stackoverflow.com/a/13781829/832230. Keys
            are also converted to lowercase, and spaces in a key are removed.
            """

            sep='_'
            final = {}
            def _flatten_dict(obj, parent_keys=[]):
                for k, v in obj.iteritems():
                    k = k.lower().replace(' ', '')
                    if isinstance(v, dict):
                        _flatten_dict(v, parent_keys + [k])
                    else:
                        key = sep.join(parent_keys + [k])
                        final[key] = v
            _flatten_dict(d)
            return final

        return {self.key: {'level': self._level,
                           'args': flatten_dict(self._kwargs)},
                }

    @staticmethod
    def from_dict(notice):
        """
        Return a :class:`Notice` instance constructed from the provided
        :obj:`dict`.

        :type notice: :obj:`dict`
        :arg notice: This is the object from which to construct a
            :class:`Notice`. It must have the same structure as is returned by
            :meth:`to_dict`.
        :rtype: :class:`Notice`
        """

        nkey, notice = notice.items()[0]
        nlevel = notice['level'].title()
        nclass_name = '{0}Notice'.format(nlevel)
        nclass = globals()[nclass_name]
        return nclass(nkey, **notice['args'])

    def to_json(self, indent=None):
        """
        Return a sorted JSON representation of the :obj:`dict` describing the
        notice.

        :type indent: :obj:`None` or :obj:`int` (non-negative)
        :arg indent: See :py:func:`json.dumps`.
        :rtype: :obj:`str`
        """

        return json.dumps(self.to_dict(), sort_keys=True, indent=indent)

    @classmethod
    def from_json(cls, notice):
        """
        Return a :class:`Notice` instance constructed from the provided JSON
        string.

        :type notice: :obj:`str`
        :arg notice: This is the object from which to construct a
            :class:`Notice`. It must have the same structure as is returned by
            :meth:`to_json`.
        :rtype: :class:`Notice`

        .. note:: This method can be used only with Python 2.7 or higher. It
            may raise the following :obj:`~exceptions.Exception` with Python
            2.6::

                TypeError: __init__() keywords must be strings
        """

        return cls.from_dict(json.loads(notice))


class TestNotice(Notice):
    """Test notice."""
    pass


class InfoNotice(Notice):
    """Informational notice."""
    pass


class WarningNotice(Notice):
    """Warning notice."""
    pass


class ErrorNotice(Notice):
    """Error notice."""
    pass


class CriticalNotice(Notice):
    """Critical error notice."""
    pass


class NoticeGrp(object):
    """Provide a container for :class:`Notice` objects."""

    def __init__(self, item_path, notices=None):
        """
        Return an object that is a group of :class:`Notice` objects for the
        specified item path and notices.

        :type item_path: :obj:`str`
        :arg item_path: absolute filesystem path of the item.
        :type notices: :obj:`~collections.Sequence` or :obj:`None`
        :arg notices: This can be a :obj:`~collections.Sequence` of
            :class:`Notice` objects that are added to the group. If multiple
            :class:`Notice` objects exist in the sequence with the same ``key``
            attribute, only the last such :class:`Notice` will be stored.
        """

        self.item = item_path
        self.notices = dict()

        if notices:
            for notice in notices: self.add_notice(notice)

    def __repr__(self):
        """
        Return a string representation.

        This string allows the class instance to be reconstructed in another
        process.
        """

        repr_ = '{0}({1}, {2})'.format(self.__class__.__name__,
                                       repr(self.item), repr(self.notices))
        return repr_.decode()

    def add_notice(self, notice):
        """
        Add the provided :class:`Notice`.

        If a :class:`Notice` already exists with the same ``key`` attribute, it
        will be overwritten.
        """
        self.notices[notice.key] = notice

    def __eq__(self, other):
        """
        Perform an equality comparison.

        :type other: :class:`NoticeGrp`
        :arg other: object to compare.
        :rtype: :obj:`bool`
        """

        return (self.item==other.item and self.notices==other.notices)

    def __nonzero__(self):
        """
        Return :obj:`True` if one or more notices exist in the group, otherwise
        return :obj:`False`.

        :rtype: :obj:`bool`
        """
        return bool(self.notices)

    def __str__(self):
        """
        Return a multiline string representation of the notice group.

        Example::

            /pnfs/fs/usr/mydir1/myfile1
            ERROR (ErrType1): This is error 1.
            WARNING (WarnType1): This is warning 1.
        """

        notices_strs = (str(n) for n in self.notices.values())
        return '{0}\n{1}'.format(self.item, '\n'.join(notices_strs))

    def to_dict(self):
        """
        Return a :obj:`dict` which describes the notice group.

        :rtype: :obj:`dict`

        The returned :obj:`dict` can be used to reconstruct this
        :class:`NoticeGrp` instance using the :meth:`from_dict` method.
        """

        return {'path': self.item,
                'notices': dict(v.to_dict().items()[0] for v in
                                self.notices.values()),
                }

    def to_exportable_dict(self):
        """
        Return an exportable :obj:`dict` which describes the notice group.

        :rtype: :obj:`dict`

        The returned :obj:`dict` cannot be used to reconstruct this
        :class:`NoticeGrp` instance using the :meth:`from_dict` method.
        """

        return {'path': self.item,
                'notices': dict(v.to_exportable_dict().items()[0] for v in
                                self.notices.values()),
                }

    @classmethod
    def from_dict(cls, noticegrp):
        """
        Return a :class:`NoticeGrp` instance constructed from the provided
        :obj:`dict`.

        :type notice: :obj:`dict`
        :arg notice: This is the object from which to construct a
            :class:`NoticeGrp`. It must have the same structure as is returned
            by :meth:`to_dict`.
        :rtype: :class:`NoticeGrp`
        """

        notices = noticegrp['notices'].items()
        notices = (Notice.from_dict({k:v}) for k,v in notices)
        return cls(noticegrp['path'], notices)

    def to_json(self, indent=None):
        """
        Return a sorted JSON representation of the :obj:`dict` describing the
        notice group.

        :type indent: :obj:`None` or :obj:`int` (non-negative)
        :arg indent: See :py:func:`json.dumps`.
        :rtype: :obj:`str`
        """

        return json.dumps(self.to_dict(), sort_keys=True,
                          indent=indent)


class ScanInterface:
    """
    This class is referenced and initialized by the :mod:`enstore` module.

    While this class is intended by the :mod:`enstore` module to perform
    command-line option parsing using the :class:`option.Interface` base class,
    it does not do so. Instead, the command-line option parsing for this module
    is performed by the :class:`CommandLineOptionsParser` class which is called
    independently. As such, this :class:`ScanInterface` class intentionally
    does not inherit the :class:`option.Interface` class.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the interface.

        No input arguments are used.
        """
        pass


def do_work(*args, **kwargs):
    """
    Perform a scan as specified on the command line, using the options
    described within the :meth:`CommandLineOptionsParser._add_options` method.
    """

    # Parse command line options
    options = CommandLineOptionsParser().options
    #options = {'scan_type': 'forward'} # These options are minimally required.
    settings.update(options)  # Merge provided options.

    # Select scanner
    scanners = {'forward': ScannerForward}
    scan_type = settings['scan_type']
    try: scanner = scanners[scan_type]
    except KeyError:
        msg = ('Error: "{0}" scan is not implemented. Select from: {1}'
               ).format(scan_type, ', '.join(scanners))
        exit(msg)

    # Perform specified action
    if settings.get('print') == 'checks':
        scanner.print_checks()
    elif settings.get('print') == 'notices':
        Notice.update_notices(scanner.notices)
        Notice.print_notice_templates()
    else:
        scanner().run()

if __name__ == '__main__':
    do_work()
