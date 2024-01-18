import argcomplete

import os

from airflow.bin.cli import CLIFactory
from airflow.configuration import conf

if __name__ == '__main__':
    if conf.get("core", "security") == 'kerberos':
        os.environ['KRB5CCNAME'] = conf.get('kerberos', 'ccache')
        os.environ['KRB5_KTNAME'] = conf.get('kerberos', 'keytab')

    cmd = ["dags","list"]
    # cmd=["dags", "trigger", "tutorial"]
    parser = CLIFactory.get_parser()
    argcomplete.autocomplete(parser)
    args = parser.parse_args(cmd)
    out = args.func(args)   # 运行命令
