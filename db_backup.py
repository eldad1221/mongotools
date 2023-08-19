"""

Dumping DB and upload files into S3 bucket.
Collections for incremental backup (i):
    sessions, personal_data, tokens
Collections for full backup (f):
    agent_analytics, billing_accounts, clients, questionnaires, surveys, surveys_counters, system_settings, users
Collections to NOT backup (0 or ''):
    assets, analytics, monitored_sessions, volatile_session_params, answers, assets, support_assistance_bot

"""

from quickbelog import Log
from backup_n_restore import backup, get_conf, check_missing_collections


if __name__ == '__main__':
    sw_id = Log.start_stopwatch(msg='DB Backup', print_it=False)
    status = 'OK'

    try:
        conf = get_conf()
        try:
            check_missing_collections(conf=conf)
        except NotImplementedError as ex:
            status = 'WARNING'
            Log.warning(str(ex))
        backup(conf=conf)
    except Exception as e:
        status = 'ERROR'
        Log.exception(f'Backup failed ({e.__class__.__name__}: {e})')
