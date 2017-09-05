import os
import os.path
import shutil
import paramiko
import subprocess
import shlex
from stat import S_ISDIR
import resorganizer.settings as rser
from resorganizer.aux import *

paramiko.util.log_to_file("paramiko.log")

class Host(object):
    """Host is the structure storing all necessary information about the host of execution, namely:
    (1) host-relative data path which defines the path to the host-specific data (e.g. compiled programs) 
    (2) research absolute path
    """
    def __init__(self, host_relative_data_path, research_absolute_path):
        self.research_abs_path = research_absolute_path 
        self.host_relative_data_path = host_relative_data_path

    def get_path_to_host_relative_data(self, name):
        """Returns the path to the data specified by name on the given host 
        """
        return '/'.join((self.host_relative_data_path, name))

class RemoteHost(Host):
    """RemoteHost extends Host including information about ssh host and the number of cores.
    """
    def __init__(self, ssh_host, cores, host_relative_data_path, research_absolute_path):
        self.ssh_host = ssh_host
        self.cores = cores
        super(RemoteHost, self).__init__(host_relative_data_path, research_absolute_path)

# Decorator
def enable_sftp(func):
    def wrapped_func(self, *args, **kwds):
        self._init_sftp()
        return func(self, *args, **kwds)
    return wrapped_func

class BaseCommunication(object):
    """BaseCommunication is an abstract class which can be used to implement the simplest access to a machine.
    A concrete class ought to use a concrete method of communication (e.g., OS API or ssh) allowing to access 
    the filesystem (copy and remove files) and execute a command line on the machine.

    Since a machine can be, in particular, the local machine, and at the same time we must always establish the communication between
    the local machine and a machine being communicated, we have to sort the terminology out. We shall call the latter a communicated 
    machine whilst the former remain the local machine.

    Generally, two types of files exchange are possible:
    (1) between the local machine and a communicated machine,
    (2) within a communicated machine.
    Since for now only copying implies this division, we introduce so called 'modes of copying': from_local, to_local 
    and all_on_communicated
    """

    def __init__(self, host, machine_name):
        self.host = host
        self._machine_name = machine_name

    def execute(self, command):
        raise NotImplementedError('This function is not implemented')

    def copy(self, from_, to_, mode='from_local'):
        """Copies from_ to to_ which are interpreted according to mode:
        (1) from_local (default) -> from_ is local path, to_ is a path on a communicated machine
        (2) to_local -> from_ is a path on a communicated machine, to_ local path
        (3) all_on_communicated -> from_ and to_ are paths on a communicated machine

        from_ and to_ can be dirs or files according to the following combinations:
        (1) from_ is dir, to_ is dir
        (2) from_ is file, to_ is dir
        (3) from_ is file, to_ is file
        """
        raise NotImplementedError('This function is not implemented')

    def rm(self, target):
        """Removes target which can be a dir or file
        """
        raise NotImplementedError('This function is not implemented')

    def _print_copy_msg(self, from_, to_):
        print('\tCopying %s to %s' % (from_, to_))

    def _print_exec_msg(self, cmd, is_remote):
        where = '@' + self._machine_name if is_remote else ''
        print('\tExecuting %s: %s' % (where, cmd))

class LocalCommunication(BaseCommunication):
    def __init__(self, local_host, machine_name='laptop'):
        super(LocalCommunication, self).__init__(local_host, machine_name)

    def execute(self, command):
        # use PIPEs to avoid breaking the child process when the parent process finishes
        # (works on Linux, solution for Windows is to add creationflags=0x00000010 instead of stdout, stderr, stdin)
        self._print_exec_msg(command, is_remote=False)
        #pid = subprocess.Popen(args, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
        #print(pid)
        subprocess.call([command], shell=True)

    def copy(self, from_, to_, mode='from_local'):
        """Any mode is ignored since the copying shall be within a local machine anyway
        """
        cp(from_, to_)
        self._print_copy_msg(from_, to_)

    def rm(self, target):
        rm(target)

class SshCommunication(BaseCommunication):
    def __init__(self, remote_host, username, password):
        if not isinstance(remote_host, RemoteHost):
            Exception('Only RemoteHost can be used to build SshCommunication')
        self.host = remote_host
        self.ssh_client = paramiko.SSHClient()
        self.sftp_client = None
        #self.main_dir = '/nobackup/mmap/research'
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh_client.connect(self.host.ssh_host, username=username, password=password)
        super(SshCommunication, self).__init__(self.host, self.host.ssh_host)

    def execute(self, command, printing=True):
        if self.ssh_client is None:
            raise Exception('Remote host is not set')

        self._print_exec_msg(command, is_remote=True)
        stdin, stdout, stderr = self.ssh_client.exec_command(command)
        if printing:
            for line in stdout:
                print('\t\t' + line.strip('\n'))
            for line in stderr:
                print('\t\t' + line.strip('\n'))

    def copy(self, from_, to_, mode='from_local'):
        if self.ssh_client is None:
            raise Exception('Remote host is not set')
        self._init_sftp()

        if mode == 'from_local':
            self._copy_from_local(from_, to_)
        elif mode == 'from_remote':
            self._copy_from_remote(from_, to_)
        elif mode == 'all_remote':
            self._print_copy_msg(self._machine_name + ':' + from_, self._machine_name + ':' + to_)
            self._mkdirp(to_)
            self.execute('cp -r %s %s' % (from_, to_))
        else:
            raise Exception("Incorrect mode '%s'" % mode)

    def rm(self, target):
        if self.ssh_client is None:
            raise Exception('Remote host is not set')
        self._init_sftp()
        self.execute('rm -r %s' % target)

    @enable_sftp
    def listdir(self, path_on_remote):
        return self.sftp_client.listdir(path_on_remote)

    @enable_sftp
    def _chdir(self, path=None):
        self.sftp_client.chdir(path)

    @enable_sftp
    def _mkdir(self, path):
        self.sftp_client.mkdir(path)

    def _mkdirp(self, path):
        path_list = path.split('/')
        cur_dir = ''
        if (path_list[0] == '') or (path_list[0] == '~'): # path is absolute and relative to user's home dir => don't need to check obvious
            cur_dir = path_list.pop(0) + '/'
        start_creating = False # just to exclude unnecessary stat() calls when we catch non-existing dir
        for dir_ in path_list:
            if dir_ == '': # trailing slash or double slash, can skip
                continue
            cur_dir += dir_
            if start_creating or (not self._is_remote_dir(cur_dir)):
                self._mkdir(cur_dir)
                if not start_creating:
                    start_creating = True

            cur_dir += '/'

    @enable_sftp
    def _open(self, filename, mode='r'):
        return self.sftp_client.open(filename, mode)

    @enable_sftp
    def _get(self, remote_path, local_path):
        return self.sftp_client.get(remote_path, local_path)

    @enable_sftp
    def _put(self, local_path, remote_path):
        return self.sftp_client.put(local_path, remote_path)

    def _is_remote_dir(self, path):
        try:
            return S_ISDIR(self.sftp_client.stat(path).st_mode)
        except IOError:
            return False

    def _copy_from_local(self, from_, to_):
        if os.path.isfile(from_):
            self._mkdirp(to_)
            self._print_copy_msg(from_, self._machine_name + ':' + to_)
            self._put(from_, to_ + '/' + os.path.basename(from_))
        elif os.path.isdir(from_):
            new_path_on_remote = to_ + '/' + os.path.basename(from_)
            self._mkdir(new_path_on_remote)
            for dir_or_file in os.listdir(from_):
                self._copy_from_local(from_ + '/' + dir_or_file, new_path_on_remote)
        else:
            raise Exception("Path %s probably does not exist" % from_)

    def _copy_from_remote(self, from_, to_):
        if not self._is_remote_dir(from_):
            self._print_copy_msg(self._machine_name + ':' + from_, to_)
            self._get(from_, to_ + '/' + os.path.basename(from_))
        else:
            new_path_on_local = to_ + '/' + os.path.basename(from_)
            os.mkdir(new_path_on_local)
            for dir_or_file in self.sftp_client.listdir(from_):
                self._copy_from_remote(from_ + '/' + dir_or_file, new_path_on_local)

    def disconnect(self):
        if self.sftp_client is not None:
            self.sftp_client.close()
        self.ssh_client.close()

    def _init_sftp(self):
        if self.sftp_client is None:
            self.sftp_client = self.ssh_client.open_sftp()
