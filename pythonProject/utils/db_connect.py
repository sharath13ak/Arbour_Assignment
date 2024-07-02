import psycopg2


class conn():
    def est_conn(self, *args, **kwrgs):
        try:
            print("establishing connection with postgress")
            pg_connection = psycopg2.connect(database=self.data['pg']['database'],
                                             user=self.data['pg']['user'],
                                             password=self.data['pg']['password'],
                                             host=self.data['pg']['host'],
                                             port=self.data['pg']['port'],
                                             )
            pg_connection.autocommit = True
            self.data['pg_conn'] = pg_connection.cursor()
            print("connection established")
        except Exception as err:
            print(f"Error in establishing connection \nReason: {err}")
