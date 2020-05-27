import argparse


class ArgparseUtil:
    @classmethod
    def invoke(cls):
        p = argparse.ArgumentParser()
        p.add_argument('-debug', type=bool, default=False, help='if use debug mode')
        p.add_argument('-sTime', type=int, default=1, help='schedule task random start time')
        p.add_argument('-eTime', type=int, default=59, help='schedule task random end time')
        p.add_argument('-uSec', type=bool, default=True, help='schedule task unit if use second unit')
        return p.parse_args()
