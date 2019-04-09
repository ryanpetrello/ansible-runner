import os
import pytest
import tempfile
from distutils.version import LooseVersion
from distutils.spawn import find_executable
import pkg_resources
import json

from ansible_runner import run
from ansible_runner.exceptions import AnsibleRunnerException


def test_basic_events():
    tdir = tempfile.mkdtemp()
    r = run(private_data_dir=tdir,
            inventory="localhost ansible_connection=local",
            playbook=[{'hosts': 'all', 'gather_facts': False, 'tasks': [{'debug': {'msg': "test"}}]}])
    event_types = [x['event'] for x in r.events]
    okay_events = [x for x in filter(lambda x: 'event' in x and x['event'] == 'runner_on_ok',
                                     r.events)]
    assert event_types[0] == 'playbook_on_start'
    assert "playbook_on_play_start" in event_types
    assert "runner_on_ok" in event_types
    assert "playbook_on_stats" in event_types
    assert r.rc == 0
    assert len(okay_events) == 1
    okay_event = okay_events[0]
    assert "uuid" in okay_event and len(okay_event['uuid']) == 36
    assert "stdout" in okay_event and len(okay_event['stdout']) > 0
    assert "event_data" in okay_event and len(okay_event['event_data']) > 0


def test_basic_serializeable():
    tdir = tempfile.mkdtemp()
    r = run(private_data_dir=tdir,
            inventory="localhost ansible_connection=local",
            playbook=[{'hosts': 'all', 'gather_facts': False, 'tasks': [{'debug': {'msg': "test"}}]}])
    events = [x for x in r.events]
    json.dumps(events)


@pytest.mark.skipif(LooseVersion(pkg_resources.get_distribution('ansible').version) < LooseVersion('2.8'),
                    reason="Valid only on Ansible 2.8+")
def test_runner_on_start(rc):
    tdir = tempfile.mkdtemp()
    r = run(private_data_dir=tdir,
            inventory="localhost ansible_connection=local",
            playbook=[{'hosts': 'all', 'gather_facts': False, 'tasks': [{'debug': {'msg': "test"}}]}])
    start_events = [x for x in filter(lambda x: 'event' in x and x['event'] == 'runner_on_start',
                                      r.events)]
    assert len(start_events) == 1


def test_playbook_on_stats_summary_fields(rc):
    tdir = tempfile.mkdtemp()
    r = run(private_data_dir=tdir,
            inventory="localhost ansible_connection=local",
            playbook=[{'hosts': 'all', 'gather_facts': False, 'tasks': [{'debug': {'msg': "test"}}]}])
    stats_events = [x for x in filter(lambda x: 'event' in x and x['event'] == 'playbook_on_stats',
                                      r.events)]
    assert len(stats_events) == 1

    EXPECTED_SUMMARY_FIELDS = ('changed', 'dark', 'failures', 'ignored', 'ok', 'rescued', 'skipped')
    fields = stats_events[0]['event_data'].keys()
    assert set(fields) >= set(EXPECTED_SUMMARY_FIELDS)


@pytest.mark.skipif(find_executable('cgexec') is None,
                    reason="cgexec not available")
@pytest.mark.skipif(LooseVersion(pkg_resources.get_distribution('ansible').version) < LooseVersion('2.8'),
                    reason="Valid only on Ansible 2.8+")
def test_profile_data():
    tdir = tempfile.mkdtemp()
    r = run(private_data_dir=tdir,
            inventory="localhost ansible_connection=local",
            resource_profiling=True,
            resource_profiling_base_cgroup='ansible-runner',
            playbook=[{'hosts': 'all', 'gather_facts': False, 'tasks': [{'debug': {'msg': "test"}}]}])
    try:
        assert r.config.env['CGROUP_CONTROL_GROUP'].startswith('ansible-runner/')
        assert r.config.env['CGROUP_CPU_POLL_INTERVAL'] == '0.25'
        assert r.config.env['CGROUP_MEMORY_POLL_INTERVAL'] == '0.25'
        assert r.config.env['CGROUP_PID_POLL_INTERVAL'] == '0.25'
        assert r.config.env['CGROUP_OUTPUT_DIR'] == os.path.join(r.config.private_data_dir, 'profiling_data')
        assert r.config.env['ANSIBLE_CALLBACK_WHITELIST'] == 'cgroup_perf_recap'
        for event in r.events:
            if event['event'] == 'runner_on_ok':
                assert 'profiling_data' in event
                assert 'cpu' in event['profiling_data']
                assert 'memory' in event['profiling_data']
                assert 'pids' in event['profiling_data']
    except AnsibleRunnerException:
        pytest.skip(
            'this test requires a cgroup to run e.g., '
            'sudo cgcreate -a `whoami` -t `whoami` -g cpuacct,memory,pids:ansible-runner'
        )  # noqa
