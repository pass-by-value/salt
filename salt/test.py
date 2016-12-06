import time


from salt.config import master_config
from salt.runner import RunnerClient
from salt.loader import runner
from salt.request_queuing.salt_job_manager import SaltJobManager

import logging

# create console handler and set level to info
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
#
# # create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#
# # add formatter to ch
ch.setFormatter(formatter)

log = logging.getLogger('salt')
log.setLevel(logging.DEBUG)
# add ch to logger
log.addHandler(ch)


def main():
    opts = master_config('/etc/salt/master')
    runners = runner(opts)
    runner_client = RunnerClient(opts)
    log.debug('Instantiating Salt Job manager')
    sjm = SaltJobManager(
        runner_client=runner_client,
        opts=opts,
        runners=runners)

    log.debug("starting main loop")
    while True:
        sjm.poll()
        sjm.update()
        time.sleep(5)


if __name__ == '__main__':
    main()
