import sys
import psycopg2
from utils.db_connect import conn
from utils.prossecing import process
import time


class filter_school():
    def __init__(self, task_no):
        self.data = {
            'task_id': task_no,
            'pg': {'database': 'postgres',
                   'user': 'postgres',
                   'password': '123',
                   'host': 'localhost',
                   'port': '5432'},
            'meta': {},
            'query': {},
            'table': {'school_capacity': 'school_capacity',
                      'agg_table': 'agg_table'},
            'final_query': [],
            'export_loc': r'C:\Users\namit\Documents\Arbor_Exports\\'
        }


    def main(self):
        conn.est_conn(self)
        process.get_meta(self)
        process.create_query(self)
        process.execution(self)


if __name__ == '__main__':
    start_time=time.time()
    print("-" * 8, "starting process", "-" * 8, sep='')
    school = filter_school(sys.argv[1])
    school.main()
    print("-" * 8, "Process Completed", "-" * 8, sep='')
    print(f"Exicution Time : {time.time()-start_time}")
