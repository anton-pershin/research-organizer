# research-organizer
An organizer for a computational task-based research

## Why do I need it?
Whatever computational research you are doing, you will inevitably struggle with the same routines:
- simply organizing data produced by your code;
- backing up the same data;
- launching usually the same commands or scripts on local and remote machines;
- gathering data from the remotes;
- collecting data for post-processing

The present project offers an extremely straight-forward approach attacking all these by a single weapon. First of all, a simple hierarchy is introduced:
```
ROOT
  |
  ------ 2008-08-08_RESEARCH_1
  |             |
  |             ----------- 0_Task_1
  |             |
  |             ----------- 1_Task_2
  |             |
  |             ----------- 2_Task_3
  |
  ------ 2008-08-08_RESEARCH_2
               |
               ----------- 0_Task_1
               |
               ----------- 1_Task_2
               |
               ----------- 2_Task_3
```

Here we introduce two entities: "research" and "task". By research, we understand a set of various upon a subject which is *potentially* independent from other subjects within your scientific investigation. The division between different research is merely logical and made for your own good. By task, we understand a particular computation or, more correctly, a group of linked code runs. Note that tasks are enumerated. This structure is created via API of the class Research, so there are no worries about handling it. What you must really think of is the launch of a task. The launch can be made either locally or on some remote machine. That is where the second point kicks in. We provide a communication module enabling you to launch the code and move data in either way. The communication module is essentially a simplified filesystem and command line (it uses ssh in background when working with a remote machine). The last component is the building of a task itself (or, equivalently, the planning of the launch of the code fed by your data). On the highest level of abstraction, we consider a task as a command executed via shell and a set of instructions regarding moving of data needed for this command to be executed correctly. This concept is represented by the classes Task and TaskExecution. The trivial usage of it is a mere command fed by an input file. We call this an *alone task*. A bit less trivial case is a command running another command several times but with different inputs. Such a launch we denote a *plural task*. Similar idea yields a command running several other command which are linked by inputs/outputs of each other. It is called a *chain task*. Moreover, you can launch a sequence of tasks in one common task-directory so that the tasks will subsequently transform the input data. Proceeding this way further, you can construct a launch of any complexity. We also have different TaskExecution children allowing you to launch the code within a native command line (*direct execution*) or SGE (*sge execution*).

To make these stuff work as it should, you need to script several functions creating Task and TaskExecution objects. To help you out with command line arguments, we provide the class Command acting as a generator for commands given different command line arguments. After that, your work becomes a deal of calling your simple functions while all low-level data movement and code execution is processed by our code.

As a side effect, you *a priori* know some information which may allow you to automate work with data:
- standardized task paths enable processing by groups (you can group tasks by some patterns in their names);
- being enumerated, task paths naturally display history;
- since you are now able to script the output names for your code, you can construct non-trivial input-output paths upon the code and data (say, graph-based);

If we launch tasks on a remote machine, there will appear a (incomplete) replication of the tasks hierarchy on it. It is again created automatically. Moreover, we support a distributed storage on the local machine which can be useful if you are using an external hard drive to store your data permanently but still want to perform some tasks within your main filesystem. A distributed storage maintains a consistent state of the tasks within several research directories such that they can be distributed among them without any local duplicates.

## TODO list
It is now planned to introduce the following features:
- a system following the movement of tasks between the remotes and local storages such that it knows all locations of a task (one or more);
- a notification-based cleaning service based on the system above;
- a fully automated back-up system such that the back-up is on the remote;