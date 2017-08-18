import os
import sys
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)
import resorganizer.settings as rset
from resorganizer.communication import SshCommunication, RemoteHost
from resorganizer.research import Research
from resorganizer.task import Command, Task
from resorganizer.task_execution import DirectExecution

def _dummy_task_exec():
    cmd = Command('echo 123 >')
    task = Task(cmd)
    task.set_input('dummyfile.dat')
    task.set_substitution('test_me', trailing_args='val.dat')
    task_exec = DirectExecution()
    task_exec.set_alone_task(task)
    return task_exec

### Settings must be filled beforehand in resorganizer.settings
### and host_id should be selected then
host_id = None
ssh_host = rset.REMOTE_HOSTS[host_id]['ssh_host']
username = rset.REMOTE_HOSTS[host_id]['username']
pswd = rset.REMOTE_HOSTS[host_id]['password']
host_rel_data_path = rset.REMOTE_HOSTS[host_id]['host_rel_data_path']
res_path = rset.REMOTE_HOSTS[host_id]['research_path']
cores = rset.REMOTE_HOSTS[host_id]['cores']

remote_host = RemoteHost(ssh_host, cores, host_rel_data_path, res_path)
comm = SshCommunication(remote_host, username, pswd)
res = Research.start_research('Test')
task_exec = _dummy_task_exec()
task_num = res.launch_task(task_exec, 'Test local')
local_task_outfile = open(os.path.join(res.get_task_path(task_num), 'val.dat'), 'r')
local_out = int(local_task_outfile.read())
if local_out == 123:
    print('Local task launch has gone well')
else:
    raise Exception('During local task launch expected {} but got {}'.format(123, local_out))
del(res)
res = Research.continue_research('Test', comm)
task_num = res.launch_task(task_exec, 'Test remote')
res.grab_task_results(task_num)
remote_task_outfile = open(os.path.join(res.get_task_path(task_num), 'val.dat'), 'r')
remote_out = int(remote_task_outfile.read())
if remote_out == 123:
    print('Remote task launch has gone well')
else:
    raise Exception('During remote task launch expected {} but got {}'.format(123, local_out))

print('The quick test has finished!')