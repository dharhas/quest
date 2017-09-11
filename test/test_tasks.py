from time import sleep
from quest.api.tasks import add_async
import pytest
import quest
from jsonrpc import dispatcher
from types import ModuleType
import sys

skip_py2 = pytest.mark.skipif(
    sys.version_info.major == 2,
    reason="async functions are not compatible with the RPC server on Python 2"
)

@pytest.fixture
def task_cleanup(api, request):
    def remove_tasks():
        pending_tasks = api.get_pending_tasks()
        api.cancel_tasks(pending_tasks)
        api.remove_tasks()

    request.addfinalizer(remove_tasks)


@dispatcher.add_method
@add_async
def long_process(delay, msg):
    sleep(delay)
    return {'delay': delay, 'msg': msg}


@dispatcher.add_method
@add_async
def long_process_with_exception(delay, msg):
    sleep(delay)
    1/0
    return {'delay': delay, 'msg': msg}


setattr(quest.api, 'long_process', long_process)
setattr(quest.api, 'long_process_with_exception', long_process_with_exception)


def wait_until_done(api):
    while len(api.get_pending_tasks()) > 0:
        sleep(0.2)
    return


@skip_py2
def test_launch_tasks(api, task_cleanup):
    test_tasks = [
        api.long_process(1, 'first', async=True),
        api.long_process(1, 'second', async=True),
        api.long_process(1, 'third', async=True),
        ]

    if isinstance(api, ModuleType):  # i.e. not using the RPC server
        tasks = api.get_tasks(as_dataframe=True)
        assert sorted(test_tasks) == sorted(tasks.index.values)
    else:
        tasks = api.get_tasks()
        assert sorted(test_tasks) == sorted(tasks)
    assert len(tasks) == 3

    wait_until_done(api)
    for task, msg in zip(test_tasks, ['first', 'second', 'third']):
        assert {'delay': 1, 'msg': msg} == api.get_task(task)['result']


@skip_py2
def test_add_remove_tasks(api, task_cleanup):
    test_tasks = [
        api.long_process(1, 'first', async=True),
        api.long_process(1, 'second', async=True),
        api.long_process(1, 'third', async=True),
        ]
    assert len(api.get_tasks()) == 3
    test_tasks.append(api.long_process(10, 'fourth', async=True))
    assert len(api.get_tasks()) == 4
    api.cancel_tasks(test_tasks[3])
    assert len(api.get_tasks(filters={'status': 'cancelled'})) == 1
    api.remove_tasks(status='cancelled')
    assert len(api.get_tasks(filters={'status': 'cancelled'})) == 0
    assert len(api.get_tasks()) == 3
    wait_until_done(api)
    api.remove_tasks()
    assert len(api.get_tasks()) == 0


@skip_py2
def test_task_with_exception(api, task_cleanup):
    test_tasks = [
        api.long_process(1, 'first', async=True),
        long_process_with_exception(1, 'second', async=True),
        api.long_process(1, 'third', async=True),
        ]

    tasks = api.get_tasks(as_dataframe=False)
    assert len(tasks) == 3
    wait_until_done(api)
    assert len(api.get_tasks(filters={'status': 'error'})) == 1
