class process():
    def create_query(self, *args, **kwargs):
        try:
            print("createing dynamic query")

            if self.data['meta']['max_hier'] == 'nation code':
                self.data['query']['filter_col'] = 'country_code'
            elif self.data['meta']['max_hier'] == 'region code':
                self.data['query']['filter_col'] = 'region_code'
            elif self.data['meta']['max_hier'] == 'new la code':
                self.data['query']['filter_col'] = 'new_la_code'
            elif self.data['meta']['max_hier'] == 'school code':
                self.data['query']['filter_col'] = 'school_urn'

            if self.data['meta']['create_table'] == 'True':
                #create schema if not exist
                query = "CREATE SCHEMA IF NOT EXISTS export;"
                self.data['pg_conn'].execute(query)
                # drop table if already exist
                query = f"DROP TABLE IF EXISTS export.{self.data['meta']['table_name']}_{self.data['task_id']}"
                self.data['pg_conn'].execute(query)
                self.data['query']['new_tab'] = f"into export.{self.data['meta']['table_name']}_{self.data['task_id']}"
            else:
                self.data['query']['new_tab'] = ''

            query = f"select * {self.data['query']['new_tab']} from {self.data['table']['school_capacity']} where {self.data['query']['filter_col']} = '{self.data['meta']['choice']}' ;"
            self.data['final_query'].append(query)

            #agg dynamic query creation
            if self.data['meta']['agg'] == 'True' and self.data['meta']['max_hier'] != 'school code':
                hier_col_ord = ['nation code', 'region code', 'new la code']
                hier_col_ord_dict = {'nation code': 'country_code',
                                     'region code': 'region_code',
                                     'new la code': 'new_la_code'}
                cond_check = False
                q_list = []

                for col in hier_col_ord:
                    if col == self.data['meta']['max_hier']:
                        cond_check = True
                        cond = f"{hier_col_ord_dict[col]}='{self.data['meta']['choice']}'"
                        q_list.append(cond)
                    else:
                        if cond_check:
                            cond = f"{hier_col_ord_dict[col]} is null "
                            q_list.append(cond)

                if self.data['meta']['create_table'] == 'True':
                    query = f"DROP TABLE IF EXISTS export.{self.data['meta']['table_name']}_aggregate_{self.data['task_id']}"
                    self.data['pg_conn'].execute(query)
                    self.data['query']['new_tab'] = f"into export.{self.data['meta']['table_name']}_aggregate_{self.data['task_id']}"
                else:
                    self.data['query']['new_tab'] = ''

                query = f"select * {self.data['query']['new_tab']} from {self.data['table']['agg_table']} where {' and '.join(q_list)};"
                self.data['final_query'].append(query)

            print("Query created successfully")
        except Exception as err:
            print(f"Error in process \n Reason: {err}")

    def execution(self,*args, **kwrgs):
        try:
            if self.data['meta']['create_table'] == 'True':
                for que in self.data['final_query']:
                    print(que)
                    self.data['pg_conn'].execute(que)
            else:
                with open(self.data['export_loc'] + f'task_id_{self.data["task_id"]}.txt', "w") as file:
                    for que in self.data['final_query']:
                        file.write('-' * 8 + 'querry' + '-' * 8 + '\n')
                        file.write(que)
                        file.write('\n\n')
        except Exception as err:
            print(f"Error in execution \n Reason: {err}")

    def get_meta(self, *args, **kwrgs):
        try:
            print("Getting section criteria")
            query = f"select * from public.meta_query_data where id={self.data['task_id']};"
            self.data['pg_conn'].execute(query)
            key_val = self.data['pg_conn'].fetchall()
            d = {}
            for i in key_val:
                self.data['meta'][i[1]] = i[2]
            print("Metadata fetched successfully")
        except Exception as err:
            print(f"Error in get_meta \n Reason: {err}")
