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


def prepare_frame(frame: pd.DataFrame, system):
    frame['system'] = system
    frame['start'] = frame['start'].apply(lambda x: dt.strptime(x, '%Y-%m-%d').date())
    frame['end'] = frame['end'].apply(lambda x: dt.strptime(x, '%Y-%m-%d').date())

    frame['row_id'] = None
    frame['row_id'] = f'{system}_row_' + frame['row_id'].index.astype(str)
    melted_frame = frame.melt(id_vars=['row_id', 'system'], value_vars=['start', 'end'],
                              value_name='event_date', var_name='event').sort_values(by='event_date').reset_index(
        drop=True)

    melted_frame['event_name'] = melted_frame['system'] + '_' + melted_frame['event']
    melted_frame = melted_frame.drop(columns=['event', 'system'])

    return melted_frame


class EventQueueGenerator:
    def __init__(self):
        self.gs_event_queue = []
        self.pearl_event_queue = []

    def generate_queue(self, gs_data, pearl_data):
        gs_frame = pd.DataFrame(gs_data)
        pearl_frame = pd.DataFrame(pearl_data)

        melted_gs_frame = prepare_frame(gs_frame, 'gs')
        melted_pearl_frame = prepare_frame(pearl_frame, 'pearl')

        event_sequence = melted_gs_frame.append(melted_pearl_frame).sort_values(by='event_date').reset_index(drop=True)

        event_dates = list(event_sequence['event_date'].unique())

        for item in event_dates:
            events_of_date = event_sequence[event_sequence['event_date'] == item]

            if len(events_of_date) == 1:
                self.handle_single_event(events_of_date.iloc[0])
            else:
                self.handle_dual_events(events_of_date)

    def handle_single_event(self, event_info):
        if event_info['event_name'] == 'gs_start':
            if len(self.pearl_event_queue) > 0:
                pearl_record = self.pearl_event_queue[0]
                if pearl_record['event_date'] < event_info['event_date']:
                    print(f"gs_start::Pearl Only Record : {pearl_record['row_id']}, "
                          f"Start : {pearl_record['event_date']}, End : {event_info['event_date'] - timedelta(days=1)}")

                    pearl_record['event_date'] = event_info['event_date']

            self.gs_event_queue.append(event_info)
        elif event_info['event_name'] == 'pearl_start':
            if len(self.gs_event_queue) > 0:
                gs_record = self.gs_event_queue[0]
                if gs_record['event_date'] < event_info['event_date']:
                    print(
                        f"pearl_start::GS Only Record : {gs_record['row_id']}, "
                        f"Start : {gs_record['event_date']}, End : {event_info['event_date'] - timedelta(days=1)}")

                    gs_record['event_date'] = event_info['event_date']

            self.pearl_event_queue.append(event_info)
        elif event_info['event_name'] == 'gs_end':
            if len(self.pearl_event_queue) > 0:
                pearl_record = self.pearl_event_queue[0]
                print(
                    f"gs_end::Common Record : Pearl - {pearl_record['row_id']}, GS - {event_info['row_id']}, "
                    f"Start : {pearl_record['event_date']}, End : {event_info['event_date']}")

                pearl_record['event_date'] = event_info['event_date'] + timedelta(days=1)
            else:
                gs_record = self.gs_event_queue[0]
                print(
                    f"gs_end::GS ONLY Record : Pearl - {gs_record['row_id']}, GS - {event_info['row_id']}, "
                    f"Start : {gs_record['event_date']}, End : {event_info['event_date']}")

            self.gs_event_queue.pop()
        else:
            if len(self.gs_event_queue) > 0:
                gs_record = self.gs_event_queue[0]
                print(
                    f"pearl_end::Common Record : Pearl - {event_info['row_id']}, GS - {gs_record['row_id']}, "
                    f"Start : {gs_record['event_date']}, End : {event_info['event_date']}")

                gs_record['event_date'] = event_info['event_date'] + timedelta(days=1)
            else:
                pearl_record = self.pearl_event_queue[0]
                print(
                    f"gs_end::Pearl ONLY Record : Pearl - {pearl_record['row_id']}, GS - {event_info['row_id']}, "
                    f"Start : {pearl_record['event_date']}, End : {event_info['event_date']}")

            self.pearl_event_queue.pop()

    def handle_dual_events(self, event_info_list):
        gs_event = event_info_list[event_info_list['event_name'].str.startswith('gs_')].iloc[0]
        pearl_event = event_info_list[event_info_list['event_name'].str.startswith('pearl_')].iloc[0]

        if gs_event['event_name'] == 'gs_start' and pearl_event['event_name'] == 'pearl_start':
            print('gs_start -> pearl_start')
        elif gs_event['event_name'] == 'gs_start' and pearl_event['event_name'] == 'pearl_end':
            print('gs_start -> pearl_end')
        elif gs_event['event_name'] == 'gs_end' and pearl_event['event_name'] == 'pearl_start' :
            print('pearl_start -> gs_end')
        elif gs_event['event_name'] == 'gs_end' and pearl_event['event_name'] == 'pearl_end':
            print('pearl_end -> gs_end')
        else:
            raise Exception("Cannot Handle this scenario. Ranges have issues")


eqg = EventQueueGenerator()
eqg.generate_queue(gs_data_obj, pearl_data_obj)




