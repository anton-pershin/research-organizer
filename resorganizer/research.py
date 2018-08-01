import os
import pickle
import shutil
from datetime import datetime, date
import resorganizer.settings as rset
from resorganizer.aux import *
from resorganizer.communication import *
from resorganizer.distributed_storage import *

# Create RESEARCH-ID. It is a small research which should link different local directories (with reports and time-integration) and ssh directories (with continuation, for example)
# What is included in RESEARCH?
# 1) It should be multistep. So we can always continue RESEARCH if we want
# 2) Everything should be dated and timed!
# 3) For every research we should create local directory having format @date@_@name@. Inside we should have directory "report" and file research.log where all stuff is stored
# If there are some remote calculations, then there should be "remote" directory where all that stuff is organized. Another directory is "local" which contains local results
# Somehow we should save python script reproducing the results (think about it). 
# 4) 

# Local directory hierarchy:
# ROOT for us is the directory where research.py was launch (as it is a part of postproc package, postproc.research is rather used)
# ROOT/RESEARCH_REL_DIR is main directory
# ROOT/RESEARCH_REL_DIR/research.log
# ROOT/RESEARCH_REL_DIR/report
# ROOT/RESEARCH_REL_DIR/1-some_task
# ROOT/RESEARCH_REL_DIR/2-another_task
# ROOT/RESEARCH_REL_DIR/3-one_more_task

# As we can have multiple remotes, a remote root directory should be set somewhere out of Research.
# One possible solution is a factory for Task.
# Anyway, we create directory hierarchy relative to REMOTE_ROOT as follows:
# REMOTE_ROOT/RESEARCH_REL_DIR/1-some_task
# REMOTE_ROOT/RESEARCH_REL_DIR/2-another_task
# REMOTE_ROOT/RESEARCH_REL_DIR/3-one_more_task

# Some usual cases:
# 1) Remote calculation on some cluster.
#       1.1 COPY-EXECUTE TASK 
#           1.1.1 copy input files from local to remote
#           1.1.2 copy program from remote to remote
#           1.1.3 execute program
#       1.2 wait for finishing (at this moment, we just have to wait and check)
#       1.3 COPY-TASK
#           1.3.1 copy results from remote to local
#       1.4 as result, we will have results directly in the task directory

LOG_FILE = 'research.log'

class Research:
    """Research is the main class for interacting with the hierarchy of tasks.

    It allows to:
    (1) find location of a task by its number
    (2) store/load object into/from a task's dir
    (3) create new tasks by launching TaskExecution locally or on remotes
    (4) launch TaskExecution in already existing task's directory
    (5) grab task's content from remotes

    The main idea behind Research is that we collect tasks in the research's dir and make 
    them enumerated. Each task is completely identified by its number. Its content, in turn,
    is located in the task's dir which is made up by concatenation of the number and the task's
    name that must be specified by a user. Next, we distinguish two logical places where tasks
    are collected: local one and remote one. Both places are distributed, but in a different 
    manner: the local place is distributed over the locally accesible space on the current machine 
    (say, some tasks may be located in dir path/to/storage and another tasks in another_path/to/storage)
    whereas the remote place is distributed over different machines. For the local place, we use DistributedStorage
    class to access the data. The remote place is accessed by setting a necessary SshCommunication in the Research
    class constructor. To avoid the mess in directories, we assume the following rules: 
    (1) intersection between tasks' dirs of the same research dir on different location on the local machine is empty
    (2) all tasks are presented locally (in any location defined in DistributedStorage) at least as empty dirs
    (3) intersection between tasks' dirs of the same research dir on different remotes is empty
    (4) union of them

    Therefore, all tasks located remotely must map to the local machine (but it is not true for an opposite case!).
    Hence, when the task intended to be executed on a remote is created, we create both a remote directory and a local 
    directory for this task. To be consistent in creating tasks locally, we choose a main (master) directory
    in DistributedStorage in which we create by default tasks directories whereas other local directories are assumed 
    to be for storing purposes.

    The instance of Research is created by passing to it BaseCommunication object. If the instance is intended to launch
    tasks on remotes or grab tasks from remotes, then the corresponding BaseCommunication for interaction must be passed.
    By default, LocalCommunication is used and the remote interaction is thus disabled.
    """
    def __init__(self, name, comm=None, continuing=False, comment=''):
        # Always create local communication here
        # Remote communication is optional then
        self._tasks_number = 0
        self._local_comm = LocalCommunication(Host(rset.LOCAL_HOST['host_relative_data_path'], \
            rset.LOCAL_HOST['main_research_path']), rset.LOCAL_HOST['machine_name'])
        self._exec_comm = comm if comm != None else self._local_comm
        self._distr_storage = DistributedStorage((rset.LOCAL_HOST['main_research_path'], rset.LOCAL_HOST['storage_research_path']), prior_storage_index=1)
        suitable_name = self._make_suitable_name(name)
        if not continuing:
            # interpret name as name without date
            self._research_id = str(date.today()) + '_' + suitable_name
            if self._distr_storage.get_dir_path(self._research_id) is not None:
                raise ResearchAlreadyExists("Research with name '{}' already exists, choose another name".format(self._research_id))
            self.research_path = self._distr_storage.make_dir(self._research_id)
            print('Started new research at {}'.format(self.research_path))

            # Add to log
            log_lines = ['NEW RESEARCH: ' + str(self._research_id), '\n', comment]
            self.write_log(log_lines, new_research = True)
        else:
            # interpret name as the full research id
            self._research_id = suitable_name
            self.research_path = self._load_research_data()

    @classmethod
    def start_research(cls, name, comm=None, comment=''):
        return Research(name, comm, comment=comment)

    @classmethod
    def continue_research(cls, name, comm=None):
        return Research(name, comm, continuing=True)

    def _load_research_data(self):
        # find corresponding date/name
        # construct object from all data inside
        research_path = self._distr_storage.get_dir_path(self._research_id)
        if research_path is None:
            # assume date was omitted in research id
            research_path, dir_params = self._distr_storage.find_dir_by_named_regexp('', '^(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+)_{}'.format(self._research_id))
            if dir_params is None:
                raise ResearchDoesNotExist("Research '{}' does not exist".format(self._research_id))
            self._research_id = '{}-{}-{}_{}'.format(dir_params['year'], dir_params['month'], dir_params['day'], self._research_id)

        print('Loaded research at {}'.format(research_path))

        # determine maximum task number to set the number for the next possible task
        dirnames, _ = self._distr_storage.listdir(self._research_id)
        self._tasks_number = 0
        for dir_ in dirnames:
            if dir_ != 'report':
                task_number, _ = self._split_task_dir(dir_)
                if task_number > self._tasks_number:
                    self._tasks_number = task_number
        self._tasks_number += 1
        print('Number of tasks in the current research: {}'.format(self._tasks_number))
        return research_path

    def launch_task(self, task_exec, name):
        """Creates a new task, copies necessary data and executes the command line
        """
        task_number = self._get_next_task_number()
        local_task_dir = self._make_task_path(task_number, name)
        os.mkdir(local_task_dir)
        self._launch_task_impl(task_exec, task_number, task_exists=False)
        if task_exec.command is not None:
            log_lines = ['\tNEW TASK: ' + str(task_number), '\n', '\t\tCommand: ' + task_exec.command, '\n']
        else:
            log_lines = ['\tNEW TASK: ' + str(task_number), '\n']
        self.write_log(log_lines)
        return task_number

    def launch_task_on_existing(self, task_exec, task_number):
        """Copies necessary data and executes the command line in already created task
        """
        self._launch_task_impl(task_exec, task_number, task_exists=True)

    def _launch_task_impl(self, task_exec, task_number, task_exists=False):
        is_remote_execution = self._local_comm is not self._exec_comm
        local_task_dir = self.get_task_path(task_number)
        if is_remote_execution:
            working_task_dir = self.get_task_path(task_number, execution_host=self._exec_comm.host)
        else:
            working_task_dir = local_task_dir

        def copy_task_data(copies_list_):
            for copy_target in copies_list_:
                self._exec_comm.copy(copy_target['path'], working_task_dir, copy_target['mode'])
        def remove_task_data():
            if not task_exists:
                self._local_comm.rm(local_task_dir)
                if is_remote_execution:
                    self._exec_comm.rm(working_task_dir)

        copies_list = self._build_copies_list_with_modes(task_exec)
        do_atomic(partial(copy_task_data, copies_list), remove_task_data)
        if task_exec.command is None: # execute python function
            if task_exec.pyfunc is not None:
                do_atomic(partial(task_exec.pyfunc, local_task_dir), remove_task_data)
            else:
                print('Cannot execute pyfunc')
                remove_task_data()
        elif task_exec.command != '':
            # to treat arguments of command properly, we need to go to the task directory
            full_command = 'cd ' + working_task_dir + ';'
            if not task_exec.is_global_command:
                full_command += './'
            full_command += task_exec.command
            #full_command = working_task_dir + '/' + task.command if is_remote_execution else os.path.join(working_task_dir, task.command)
            #self.__communication.execute(full_command, is_remote=is_remote_execution)
            do_atomic(partial(self._exec_comm.execute, full_command), remove_task_data)
        else:
            print('Cannot execute pyfunc')
            remove_task_data()

    def _build_copies_list_with_modes(self, task_exec):
        is_remote_execution = self._local_comm is not self._exec_comm
        copies_list = []
        for copy_target in task_exec.copies_list:
            copies_list.append({
                    'path' : copy_target,
                    'mode' : 'from_local' if is_remote_execution else 'all_local',
                })
        for copy_target in task_exec.host_relative_copies_list:
            copies_list.append({
                    'path' : self._exec_comm.host.get_path_to_host_relative_data(copy_target),
                    'mode' : 'all_remote' if is_remote_execution else 'all_local',
                })
        return copies_list

    def grab_task_results(self, task_number, copies_list=[]):
        """Moves task content from the remote to the local. Locally, the task content will appear in the task
        dir located in the master research location.
        """
        task_results_local_path = self.get_task_path(task_number)
        task_results_remote_path = self.get_task_path(task_number, self._exec_comm.host)
        if len(copies_list) == 0: # copy all data
            pathes = self._exec_comm.listdir(task_results_remote_path)
            for file_or_dir in pathes:
                self._exec_comm.copy('/'.join((task_results_remote_path, file_or_dir)), task_results_local_path, 'from_remote')
        else:
            for copy_target in copies_list:
                remote_copy_target_path = '/'.join((task_results_remote_path, copy_target['path'])) # we consider copy targets as relative to task's dir
                self._exec_comm.copy(remote_copy_target_path, task_results_local_path, 'from_remote')
                if 'new_name' in copy_target:
                    os.rename(os.path.join(task_results_local_path, os.path.basename(copy_target['path'])), \
                              os.path.join(task_results_local_path, copy_target['new_name']))

    def cleanup(self, task_number, removes_list):
        for remove_target in removes_list:
            full_target_path = os.path.join(self.get_task_path(task_number), remove_target)
            print('\tRemoving ' + full_target_path)
            if os.path.isdir(full_target_path):
                shutil.rmtree(full_target_path)
            else:
                os.remove(full_target_path)

    def call_on_each_gen(self, task_number, copies_list, percopy_func):
        """For each item in copies_list, copies it from the task dir corresponding to task_number on the remote to the local dir, 
        executes percopy_func upon it and removes it. It is useful when you don't want to copy all the task content from the 
        remote at once, but still need to process some of the content in a lazy manner.
        """
        for copy_target in copies_list:
            yield self.call_on_lazy_remote_data(task_number, copy_target, percopy_func)

    def call_on_lazy_remote_data(self, task_number, copy_target, func):
        """Copies copy_target from the task dir corresponding to task_number on the remote to the local dir, executes 
        func upon it and then removes it.
        """
        self.grab_task_results(task_number, (copy_target,))
        actual_copy = copy_target['new_name'] if 'new_name' in copy_target else os.path.basename(copy_target['path'])
        print('Calling on ' + actual_copy)
        res = func(os.path.join(self.get_task_path(task_number), actual_copy))
        self.cleanup(task_number, (actual_copy,))
        return res

    def put_into_report(self, report_data):
        pass

    def write_log(self, lines, new_research = False):
        f = open(os.path.join(rset.LOCAL_HOST['main_research_path'], LOG_FILE), 'a')
        dt_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if not new_research:
            dt_str += ', RESEARCH-ID: ' + self._research_id
        f.writelines([dt_str, '\n'] + lines + ['\n\n'])

    def _make_task_path(self, task_number, task_name, execution_host=None):
        task_path = ''
        rel_task_dir = os.path.join(self._research_id, self._get_task_full_name(task_number, task_name))
        if execution_host is None:
            task_path = os.path.join(rset.LOCAL_HOST['main_research_path'], rel_task_dir)
        else:
            task_path = os.path.join(execution_host.research_abs_path, rel_task_dir)
        return task_path

    def get_task_path(self, task_number, execution_host=None):
        """Returns the task dir corresponding to task_number. By default, the local dir (from DistrubutedStorage) is returned.
        If execution_host is specified, then the remote dir will be returned.
        """
        task_path = ''
        task_name = self._get_task_name_by_number(task_number)
        rel_task_dir = os.path.join(self._research_id, self._get_task_full_name(task_number, task_name))
        if execution_host is None:
            task_path = self._distr_storage.get_dir_path(rel_task_dir)
        else:
            task_path = os.path.join(execution_host.research_abs_path, rel_task_dir)
        return task_path

    def dump_object(self, task_number, obj, obj_name):
        """Dumps obj into the file whose name is obj_name + '.pyo' and locates it into the task dir corresponding to
        task_number
        """
        print('Dumping ' + obj_name)
        f = open(os.path.join(self.get_task_path(task_number), obj_name + '.pyo'),'w')
        pickle.dump(obj, f)
        f.close()

    def load_object(self, task_number, obj_name):
        """Load an object dumped into the file whose name is obj_name + '.pyo' and which is located it into the task dir 
        corresponding to task_number
        """
        print('Loading ' + obj_name)
        f = open(os.path.join(self.get_task_path(task_number), obj_name + '.pyo'),'r')
        obj = pickle.load(f)
        f.close()
        return obj

#    def continue_task(self, task_number, task):
#        task_dir = '/'.join([RESEARCH_REL_DIR, research_id, task_name])
#        if not os.path.exists(task_dir):
#            raise Exception('Task named ' + task_name + ' does not exist')
#
#        self.__move_task_data(task.copies_list, comm, task_dir)
#        if task.command != '':
#            comm.execute(task.command)

    def _get_next_task_number(self):
        self._tasks_number += 1
        return self._tasks_number - 1

    def _get_task_full_name(self, task_number, task_name):
        return str(task_number) + '-' + self._make_suitable_name(task_name)

    def _get_task_name_by_number(self, task_number):
        find_data = self._distr_storage.find_dir_by_named_regexp(self._research_id, '^{}-(?P<task_name>\S+)'.format(task_number))
        if find_data is None:
            raise Exception("No task with number '{}' is found".format(task_number))
        return find_data[1]['task_name']

    def _split_task_dir(self, task_dir):
        parsing_params = parse_by_named_regexp('^(?P<task_number>\d+)-(?P<task_name>\S+)', task_dir)
        if parsing_params is None:
            raise Exception("No task directory '{}' is found".format(task_dir))
        return int(parsing_params['task_number']), parsing_params['task_name']

    def _get_research_path(self, execution_host=None):
        path = ''
        if execution_host:
            is_remote_execution = isinstance(task._execution_host, RemoteHost)
            rel_task_dir = os.path.join(self._research_id, str(task_number) + '-' + task)
            path = execution_host.research_abs_path if is_remote_execution else self._get_local_research_path()
        else:
            path = self._get_local_research_path()
        return path

    def _get_local_research_path(self):
        return os.path.join(rset.LOCAL_HOST['main_research_path'], self._research_id)

    def _move_task_data(self, copies_list, task_dir):
        for copy_target in copies_list:
            self._exec_comm.copy(copy_target["path"], os.path.join(comm.main_dir, task_dir), copy_target["mode"])

    def _make_suitable_name(self, name):
        return '_'.join(name.split())

class ResearchAlreadyExists(Exception):
    pass

class ResearchDoesNotExist(Exception):
    pass

def get_all_research_ids():
    return os.listdir('.' + rset.LOCAL_HOST['main_research_path'])

def retrieve_trailing_float_from_task_dir(task_dir):
    matching = re.search('^(?P<task_number>\d+)-(?P<task_name>\S+)_(?P<float_left>\d+)\.(?P<float_right>\d+)', task_dir)
    if matching is None:
        raise Exception('Incorrect task directory is given')
    return float('{}.{}'.format(matching.group('float_left'), matching.group('float_right')))
