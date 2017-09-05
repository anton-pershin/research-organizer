"""Settings module contains information about local host and all remotes.

We assume that the local host has two sources of data: main_research_path and storage_research_path.
They consistute a distributed storage. It is up to you whether they are overlapping somehow or not, but
it is assumed that main_research_path is used for launches of tasks and grabbing results from remotes
whereas storage_research_path is better for permanent storing of data. For instance, storage_research_path
can be an external hard drive. You can imagine an analogy with caches -- main_research_path should be quickly 
accessable whereas storage_research_path

For remotes, there is no distributed storage so only one research_path should be defined.
"""

LOCAL_HOST = {
    'machine_name' : None,
    'host_relative_data_path' : None,
    'main_research_path' : None,
    'storage_research_path' : None,
}

REMOTE_HOSTS = {
    None : {
        'ssh_host' : None,
        'username' : None,
        'password' : None,
        'host_relative_data_path' : None,
        'research_path' : None,
        'cores' : None,
    },
}
