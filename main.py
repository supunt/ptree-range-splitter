import pandas as pd
from datetime import datetime as dt, timedelta

gs_data_obj = {
    'start': ['2022-01-01', '2022-01-06', '2022-01-12'],
    'end': ['2022-01-05', '2022-01-11', '2022-01-20'],
}

pearl_data_obj = {
    'start': ['2022-01-03', '2022-01-11'],
    'end': ['2022-01-08', '2022-01-15']
}


class EventQueueGenerator:
    def __init__(self):
        self.gs_event_queue = []
        self.pearl_event_queue = []
        self.op_event_queue = None
        self.event_queue_obj = {
            'Effective_From': [],
            'Effective_To': [],
            'GS_Row_Id': [],
            'Pearl_Row_Id': []
        }

    def generate_queue(self, gs_data, pearl_data):
        gs_frame = pd.DataFrame(gs_data)
        pearl_frame = pd.DataFrame(pearl_data)

        melted_gs_frame = self.__prepare_frame__(gs_frame, 'BNP_Start_Date', 'BNP_End_Date')
        melted_pearl_frame = self.__prepare_frame__(pearl_frame, 'Pearl_Start_Date', 'Pearl_End_Date')

        event_sequence = melted_gs_frame.append(melted_pearl_frame).sort_values(by='event_date').reset_index(drop=True)

        event_dates = list(event_sequence['event_date'].unique())

        for item in event_dates:
            events_of_date = event_sequence[event_sequence['event_date'] == item]

            if len(events_of_date) == 1:
                self.__handle_single_event__(events_of_date.iloc[0])
            else:
                self.__handle_dual_events__(events_of_date)

        self.op_event_queue = pd.DataFrame(self.event_queue_obj)

        return self.op_event_queue

    def __handle_single_event__(self, event_info):
        if event_info['event_name'] == 'gs_start':
            if len(self.pearl_event_queue) > 0:
                pearl_record = self.pearl_event_queue[0]
                if pearl_record['event_date'] < event_info['event_date']:
                    self.__add_event_to_queue__(effective_from=pearl_record['event_date'],
                                                effective_to=event_info['event_date'] - timedelta(days=1),
                                                gs_row_id=None,
                                                pearl_row_id=pearl_record['row_id'])

                    print(f"gs_start::Pearl Only Record : {pearl_record['row_id']}, "
                          f"Start : {pearl_record['event_date']}, End : {event_info['event_date'] - timedelta(days=1)}")

                    pearl_record['event_date'] = event_info['event_date']

            self.gs_event_queue.append(event_info)
        elif event_info['event_name'] == 'pearl_start':
            if len(self.gs_event_queue) > 0:
                gs_record = self.gs_event_queue[0]
                if gs_record['event_date'] < event_info['event_date']:
                    self.__add_event_to_queue__(effective_from=gs_record['event_date'],
                                                effective_to=event_info['event_date'] - timedelta(days=1),
                                                gs_row_id=gs_record['row_id'],
                                                pearl_row_id=None)

                    print(
                        f"pearl_start::GS Only Record : {gs_record['row_id']}, "
                        f"Start : {gs_record['event_date']}, End : {event_info['event_date'] - timedelta(days=1)}")

                    gs_record['event_date'] = event_info['event_date']

            self.pearl_event_queue.append(event_info)
        elif event_info['event_name'] == 'gs_end':
            if len(self.pearl_event_queue) > 0:
                pearl_record = self.pearl_event_queue[0]
                self.__add_event_to_queue__(effective_from=pearl_record['event_date'],
                                            effective_to=event_info['event_date'],
                                            gs_row_id=event_info['row_id'],
                                            pearl_row_id=pearl_record['row_id'])
                print(
                    f"gs_end::Common Record : Pearl - {pearl_record['row_id']}, GS - {event_info['row_id']}, "
                    f"Start : {pearl_record['event_date']}, End : {event_info['event_date']}")

                pearl_record['event_date'] = event_info['event_date'] + timedelta(days=1)
            else:
                gs_record = self.gs_event_queue[0]
                self.__add_event_to_queue__(effective_from=gs_record['event_date'],
                                            effective_to=event_info['event_date'],
                                            gs_row_id=gs_record['row_id'],
                                            pearl_row_id=None)
                print(
                    f"gs_end::GS ONLY Record : Pearl - {gs_record['row_id']}, GS - {event_info['row_id']}, "
                    f"Start : {gs_record['event_date']}, End : {event_info['event_date']}")

            self.gs_event_queue.pop()
        else:
            if len(self.gs_event_queue) > 0:
                gs_record = self.gs_event_queue[0]
                self.__add_event_to_queue__(effective_from=gs_record['event_date'],
                                            effective_to=event_info['event_date'],
                                            gs_row_id=gs_record['row_id'],
                                            pearl_row_id=event_info['row_id'])
                print(
                    f"pearl_end::Common Record : Pearl - {event_info['row_id']}, GS - {gs_record['row_id']}, "
                    f"Start : {gs_record['event_date']}, End : {event_info['event_date']}")

                gs_record['event_date'] = event_info['event_date'] + timedelta(days=1)
            else:
                pearl_record = self.pearl_event_queue[0]
                self.__add_event_to_queue__(effective_from=pearl_record['event_date'],
                                            effective_to=event_info['event_date'],
                                            gs_row_id=None,
                                            pearl_row_id=pearl_record['row_id'])

                print(
                    f"gs_end::Pearl ONLY Record : Pearl - {pearl_record['row_id']}, GS - {event_info['row_id']}, "
                    f"Start : {pearl_record['event_date']}, End : {event_info['event_date']}")

            self.pearl_event_queue.pop()

    def __handle_dual_events__(self, event_info_list):
        gs_event = event_info_list[event_info_list['event_name'].str.startswith('gs_')].iloc[0]
        pearl_event = event_info_list[event_info_list['event_name'].str.startswith('pearl_')].iloc[0]

        if gs_event['event_name'] == 'gs_start' and pearl_event['event_name'] == 'pearl_start':
            print('gs_start -> pearl_start')
            self.gs_event_queue.append(gs_event)
            self.pearl_event_queue.append(pearl_event)
        elif gs_event['event_name'] == 'gs_start' and pearl_event['event_name'] == 'pearl_end':
            print(f'gs_start -> pearl_end - Common One Day Record  {gs_event["event_date"]}')
            
            pearl_record = self.pearl_event_queue[0]
            self.__add_event_to_queue__(effective_from=pearl_record['event_date'],
                                        effective_to=pearl_event['event_date'] - timedelta(days=1),
                                        gs_row_id=None,
                                        pearl_row_id=pearl_event['row_id'])

            self.__add_event_to_queue__(effective_from=gs_event['event_date'],
                                        effective_to=gs_event['event_date'],
                                        gs_row_id=gs_event['row_id'],
                                        pearl_row_id=pearl_event['row_id'])
            gs_event['event_date'] = gs_event['event_date'] + timedelta(days=1)
            self.gs_event_queue.append(gs_event)
            self.pearl_event_queue.pop()
        elif gs_event['event_name'] == 'gs_end' and pearl_event['event_name'] == 'pearl_start':
            print(f'pearl_start -> gs_end - Common Record {pearl_event["event_date"]}')
            gs_record = self.gs_event_queue[0]

            self.__add_event_to_queue__(effective_from=gs_record['event_date'],
                                        effective_to=gs_event['event_date'] - timedelta(days=1),
                                        gs_row_id=gs_event['row_id'],
                                        pearl_row_id=None)

            self.__add_event_to_queue__(effective_from=pearl_event['event_date'],
                                        effective_to=pearl_event['event_date'],
                                        gs_row_id=gs_event['row_id'],
                                        pearl_row_id=pearl_event['row_id'])

            pearl_event['event_date'] = pearl_event['event_date'] + timedelta(days=1)
            self.pearl_event_queue.append(pearl_event)
            self.gs_event_queue.pop()
        elif gs_event['event_name'] == 'gs_end' and pearl_event['event_name'] == 'pearl_end':
            print('pearl_end -> gs_end')
            self.__add_event_to_queue__(effective_from=pearl_event['event_date'],
                                        effective_to=pearl_event['event_date'],
                                        gs_row_id=gs_event['row_id'],
                                        pearl_row_id=pearl_event['row_id'])
            self.gs_event_queue.pop()
            self.pearl_event_queue.pop()
        else:
            raise Exception("Cannot Handle this scenario. Ranges have issues")

    def __add_event_to_queue__(self, effective_from, effective_to, gs_row_id, pearl_row_id):
        self.event_queue_obj['Effective_From'].append(effective_from)
        self.event_queue_obj['Effective_To'].append(effective_to)
        self.event_queue_obj['GS_Row_Id'].append(gs_row_id)
        self.event_queue_obj['Pearl_Row_Id'].append(pearl_row_id)

    @staticmethod
    def __prepare_frame__(frame: pd.DataFrame, start_date_col_name, end_date_col_name):
        frame = frame.rename(columns={
            start_date_col_name: 'start',
            end_date_col_name: 'end',
        })
        melted_frame = frame.melt(id_vars=['row_id', 'system'], value_vars=['start', 'end'],
                                  value_name='event_date', var_name='event').sort_values(by='event_date').reset_index(
            drop=True)

        melted_frame['event_name'] = melted_frame['system'] + '_' + melted_frame['event']
        melted_frame = melted_frame.drop(columns=['event', 'system'])

        return melted_frame

eqg = EventQueueGenerator()
eqg.generate_queue(gs_data_obj, pearl_data_obj)




