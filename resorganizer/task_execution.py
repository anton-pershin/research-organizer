from mako.template import Template
from resorganizer.aux import append_code, get_templates_path, create_file_mkdir
import os

class TaskExecution(object):
    """TaskExecution describes how CommandTask (and PythonTask, but it is too trivial, so the case of CommandTask 
    is discussed hereafter) should be executed. In fact, there are only three steps:
    (1) copy all the data except host-relative to the dir of execution
    (2) copy all the host-relative data to the dir of execution
    (3) execute the command

    The list of paths for (1) is defined by copies_list in TaskExecution. The list of names for (2)
    is defined by host_relative_copies_list. Note that only one command must be executed whereas Task
    can contain several of them (that is called multiple task). To handle this, TaskExecution offers three
    strategies of execution:
    (1) alone task execution
    (2) plural task execution (i.e. parallel run of the commands of a multiple task)
    (3) chain task execution (i.e. serial run of the commands of a multiple task)
    and a separate case for python task:
    (4) python task execution

    It is a deal of a conrete implementation how plural task execution and chain task execution are implemented,
    but in the end, there must be a single command in self.command to be executed.
    """
    def __init__(self):
        self.pyfunc = None
        self.command = None
        self.copies_list = []
        self.host_relative_copies_list = []
        self.is_global_command = False
        append_code(self, ('set_alone_task', 'set_plural_task', 'set_chain_task'), self._add_program)

    def set_python_task(self, pytask):
        self.copies_list = pytask.inputs
        self.pyfunc = pytask.func

    def set_alone_task(self, task):
        raise NotImplementedError()

    def set_plural_task(self, task):
        raise NotImplementedError()

    def set_chain_task(self, task):
        raise NotImplementedError()

    def _add_program(self, task):
        if task.program != '':
            self.host_relative_copies_list.append(task.program)

class DirectExecution(TaskExecution):
    """DirectExecution implements an execution via OS-native API
    """
    def __init__(self):
        super(DirectExecution, self).__init__()

    def set_alone_task(self, task):
        sid, self.command = next(task.command_gen())
        self.copies_list = task.inputs

class SgeExecution(TaskExecution):
    """SgeExecution implements an execution via SGE.

    It builds sge-scripts containing the real command and creates the commands line
    via qsub. 

    Plural task execution is implemented via a separate python script which is called
    a handler. It controls the SGE queue and qsubs sge-scripts such that the queue 
    never overflows with tasks.

    Chain task execution is implemented via a sequence of sge-scripts each of which qsubs
    the next sge-script in the sequence making, therefore, a chain.
    """
    def __init__(self):
        super(SgeExecution, self).__init__()

    def set_properties(self, cores, time):
        self.cores = cores
        self.time = time

    def set_alone_task(self, task):
        sid, cmd = next(task.command_gen())
        cmd = './' + cmd
        print(cmd)
        sge_script_filename = '{}.sh'.format(sid)
        sge_script_path = os.path.join('tmp', sge_script_filename)
        sge_data = _render_sge_template(sge_script_path, self.cores, self.time, [cmd])
        self.copies_list.append(sge_script_path)
        self.copies_list += task.inputs
        self.command = 'qsub {}'.format(sge_script_filename)
        self.is_global_command = True

    def set_plural_task(self, task):
        # prepare sge scripts and add them into the list of copies
        sges = []
        for sid, cmd in task.command_gen():
            sge_script_filename = '{}.sh'.format(sid)
            sge_script_path = os.path.join('tmp', sge_script_filename)
            sge_data = _render_sge_template(sge_script_path, self.cores, self.time, ['./' + cmd])
            self.copies_list.append(sge_script_path)
            sges.append(sge_script_filename)

        # prepare py-handler
        handler_templ_file = open(os.path.join(get_templates_path(), 'plural_task_handler.py'), 'r')
        rendered_data = Template(handler_templ_file.read()).render(max_sge_tasks_in_queue=40, sleeping_time_sec=600, sges=sges)
        handler_script_filename = 'handler.py'
        handler_script_path = os.path.join('tmp', handler_script_filename)
        handler_script_file = create_file_mkdir(handler_script_path)
        handler_script_file.write(rendered_data)
        self.copies_list.append(handler_script_path)
        self.copies_list += task.inputs
        self.command = 'nohup python {} > handler.err 2>&1 &'.format(handler_script_filename)
        self.is_global_command = True

    def set_chain_task(self, task):
        cmds = []
        sids = []
        for sid, cmd in task.command_gen():
            cmds.append('./' + cmd)
            sids.append(sid)
        for i in range(len(sids)):
            sge_script_filename = '{}.sh'.format(sids[i])
            sge_script_path = os.path.join('tmp', sge_script_filename)
            sge_cmds = [cmds[i]]
            if i != len(sids) - 1:
                sge_cmds.append('qsub {}.sh'.format(sids[i + 1]))
            sge_data = _render_sge_template(sge_script_path, self.cores, self.time, sge_cmds)
            self.copies_list.append(sge_script_path)
        self.copies_list += task.inputs
        self.command = 'qsub {}.sh'.format(sids[0])
        self.is_global_command = True

def _render_sge_template(sge_script_path, cores, time, commands):
        sge_templ_file = open(os.path.join(get_templates_path(), 'sge_script.sh'), 'r')
        rendered_data = Template(sge_templ_file.read()).render(cores=cores, time=time, commands=commands)
        sge_script_file = create_file_mkdir(sge_script_path)
        sge_script_file.write(rendered_data)
