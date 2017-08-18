from resorganizer.aux import *

# All possible combinations:
# (1) single program, single command, single data
# (2) single program, multiple command, single data
# (3) single program, single command, multiple data
# (4) single program, multiple command, multiple data
# Multiple programs imply, in general, multiple commands and multiple data, so we omit this case
# All four cases can be implemented within just one class. However, this class must provide an information about
# its inner multiplicity in order for an executor to check availability of parallel or chain execution

class Command(object):
    def __init__(self, command_name, params={}, flags=()):
        self._command_name = command_name
        self._params = params
        self._flags = flags

    def substitute(self, param_values={}, flags=(), trailing_args=''):
        """
        trailing_args can be either sequence or a mere string
        """
        command_str = self._command_name + ' '
        for param, val in param_values:
            if not param in self._params:
                raise Exception('Command line parameter {} is not allowed'.format(param))
            command_str += '-{} {} '.format(param, value)
        command_str += ' '.join(trailing_args) if is_sequence(trailing_args) else trailing_args
        return command_str

class Task(object):
    """Program is set only if it is intended to be copied from somewhere. So it is not set for global ones.
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
        self.inputs.append(input_)

    def set_substitution(self, sid, params={}, flags=(), trailing_args=''):
        self.sids.append(sid)
        self.params_subst.append(params)
        self.flags_subst.append(flags)
        self.trailing_args_subst.append(trailing_args)

    def command_gen(self):
        for sid, params_subst, flags_subst, trailing_args_subst in zip(self.sids, self.params_subst, self.flags_subst, self.trailing_args_subst):
            yield (sid, self.command.substitute(params_subst, flags_subst, trailing_args_subst))
