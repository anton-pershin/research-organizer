from mako.template import Template
from resorganizer.aux import append_code

class TaskExecution(object):
    """Any TaskExecution must finally produce a single command to be executed and a list of copies
    """
    def __init__(self):
        self.command = None
        self.copies_list = []
        self.host_relative_copies_list = []
        self.is_global_command = None
        append_code(self, ('set_alone_task', 'set_plural_task', 'set_chain_task'), self._add_program)

    def set_alone_task(self, task):
        raise NotImplementedError()

    def set_plural_task(self, task):
        raise NotImplementedError()

    def set_chain_task(self, task):
        raise NotImplementedError()

    def _add_program(self, task):
        if task.program != '':
            self.is_global_command = False
            self.host_relative_copies_list.append(task.program)
        else:
            self.is_global_command = True

class DirectExecution(TaskExecution):
    def __init__(self):
        super(DirectExecution, self).__init__()

    def set_alone_task(self, task):
        sid, self.command = next(task.command_gen())
        self.copies_list = task.inputs

class SgeExecution(TaskExecution):
    def __init__(self):
        super(SgeExecution, self).__init__()

    def set_properties(cores, time):
        self.cores = cores
        self.time = time

    def set_alone_task(self, task):
        sid, cmd = next(task.command_gen())
        sge_script_filename = '{}.sh'.format(sid)
        sge_script_path = os.path.join('tmp', sge_script_filename)
        sge_data = _render_sge_template(sge_script_path, self.cores, self.time, [cmd])
        self.copies_list.append(sge_script_path)
        self.copies_list += task.inputs
        self.command = 'qsub {}'.format(sge_script_filename)

    def set_plural_task(task):
        # prepare sge scripts and add them into the list of copies
        cmds = []
        sids = []
        sges = []
        for sid, cmd in task.command_gen():
            cmds.append(cmd)
            sids.append(sid)
            sge_script_filename = '{}.sh'.format(sid)
            sge_script_path = os.path.join('tmp', sge_script_filename)
            sge_data = _render_sge_template(sge_script_path, self.cores, self.time, [cmd])
            self.copies_list.append(sge_script_path)
            sges.append(sge_script_filename)

        # prepare py-handler
        handler_templ_file = open(os.path.join('templates', 'plural_task_handler.py'), 'r')
        rendered_data = Template(handler_templ_file.read()).render(max_sge_tasks_in_queue=20, sleeping_time_sec=600, sges=sges)
        handler_script_filename = 'handler.py'
        handler_script_path = os.path.join('tmp', handler_script_filename)
        handler_script_file = open(handler_script_path, 'w')
        handler_script_file.write(rendered_data)
        self.copies_list.append(handler_script_path)
        self.copies_list += task.inputs
        self.command = 'nohup python ' + self._py_script_name + ' >/dev/null 2>&1 &'

    def set_chain_task(task):
        cmds = []
        sids = []
        for sid, cmd in task.command_gen():
            cmds.append(cmd)
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

def _render_sge_template(sge_script_path, cores, time, commands):
        sge_templ_file = open(os.path.join('templates', 'sge_script.sh'), 'r')
        rendered_data = Template(sge_templ_file.read()).render(cores=cores, time=time, commands=commands)
        sge_script_file = open(sge_script_path, 'w')
        sge_script_file.write(rendered_data)
