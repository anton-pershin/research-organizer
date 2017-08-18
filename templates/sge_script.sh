#$ -cwd -V
#$ -l h_rt=${time}
#$ -pe smp ${cores}
% for cmd in commands:
${cmd}
% endfor