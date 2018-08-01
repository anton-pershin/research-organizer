from resorganizer.aux import *

class Command(object):
    """Command is an abstraction for any command line able to be executed. Command line has the components: 
    program name, params (flags followed by some values, e.g. -f filename), flags (alone flags. e.g. -f)
    and trailing arguments. Some of these may be optional. We create Command by passing the program name and
    all eligible names for params and flags. When we need to produce a particular command line, we substitute
    necessary ones via substitute() which returns a complete string for execution.

    Note that params and flags are implied to be prepended by a single minus, so you need to pass only their names.
    """
    def __init__(self, program_name, params=(), flags=()):
        self._program_name = program_name
        self._params = params
        self._flags = flags

    def substitute(self, param_values={}, flags=(), trailing_args=''):
        """Produces command line from params_values (a dictionary for valued flags), flags (if they are allowed, 
        otherwise throws an exception) and trailing_args (trailing arguments) which can be either sequence or a mere string.
        """
        command_str = self._program_name + ' '
        for param, val in param_values.items():
            if not param in self._params:
                raise Exception('Command line parameter {} is not allowed'.format(param))
            command_str += '-{} {} '.format(param, val)
        for flag in flags:
            if not flag in self._flags:
                raise Exception('Command line flag {} is not allowed'.format(flag))
            command_str += '-{} '.format(flag)
        command_str += ' '.join(trailing_args) if is_sequence(trailing_args) else trailing_args
        return command_str

class CommandTask(object):
    """CommandTask consists of a Command object, substitutions for the command and inputs which are files intended
    to be moved. Each substitution consists of params, flags, trailing arguments and a substitution identifier.
    After these being set, Task acts as a generator via method command_gen which yields a sid and a command string
    for a subsequent substitution.

    Using this class, you can generate command lines of three types:  
    (1) single program, single command params/flags, single data
    (2) single program, multiple command params/flags, single data
    (3) single program, single command params/flags, multiple data
    (4) single program, multiple command params/flags, multiple data
    
    Here we call (1) an alone task and (2), (3), (4) a multiple task. Multiple tasks can be executed in various ways,
    but this is a business of TaskExecution.     
    Multiple programs tasks are, in turn, implemented as a combination of several CommandTask.
    """
    def __init__(self, cmd, prog=''):
        self.program = prog
        self.command = cmd
        self.inputs = []
        self.params_subst = []
        self.trailing_args_subst = []
        self.flags_subst = []
        self.sids = []

    def set_input(self, input_):
        """Adds one input into task.
        """
        self.inputs.append(input_)

    def set_substitution(self, sid, params={}, flags=(), trailing_args=''):
        """Adds a substitution specified by sid (substitution identifier), params, flags and trailing arguments into task.
        The latter can be either a string or a sequence.
        """
        self.sids.append(sid)
        self.params_subst.append(params)
        self.flags_subst.append(flags)
        self.trailing_args_subst.append(trailing_args)

    def command_gen(self):
        """Yields a substitution.
        """
        for sid, params_subst, flags_subst, trailing_args_subst in zip(self.sids, self.params_subst, self.flags_subst, self.trailing_args_subst):
            yield (sid, self.command.substitute(params_subst, flags_subst, trailing_args_subst))

class PythonTask(object):
    """PythonTask essentially executes a python function and specify data to be copied. It allows to automate some of the routines
    emerging while working with other heavier tasks. Even though the extension to "single function - multiple data" 
    idealogy is possible, it has not yet done due to lack of demand.
    """
    def __init__(self, func):
        self.func = func
        self.inputs = []

    def set_input(self, input_):
        """Adds one input into task.
        """
        self.inputs.append(input_)