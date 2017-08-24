import subprocess
import os
import time

sge_filenames = [
% for sge in sges:
    '${sge}',
% endfor
]

log = open('handler.log', 'a')
log.write(16 * '-' + '\n')
log.write('pid = {}\n'.format(os.getpid()))
log.write(16 * '-' + '\n')
log.close()
while len(sge_filenames) != 0:
    log = open('handler.log', 'a')
    p = subprocess.Popen(['qstat'], stdout=subprocess.PIPE)
    p.wait()
    lines_num = len(p.stdout.readlines())
    log.write('have ' + str(lines_num) + ' lines\n')
    max_lines_num = ${max_sge_tasks_in_queue} + 3
    if lines_num < max_lines_num:
        avail_sge_tasks = max_lines_num - lines_num
        if avail_sge_tasks > len(sge_filenames):
            avail_sge_tasks = len(sge_filenames)
        log.write('can launch ' + str(avail_sge_tasks) + ' tasks\n')
        for i in range(avail_sge_tasks):
            subprocess.call(['qsub', sge_filenames[i]])
        log.write('delete tasks ' + str(sge_filenames[:avail_sge_tasks]) + '\n')
        del sge_filenames[:avail_sge_tasks]
    log.close()
    time.sleep(${sleeping_time_sec})
log = open('handler.log', 'a')
log.write('done and exit\n')
