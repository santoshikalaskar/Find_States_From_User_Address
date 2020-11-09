import pandas as pd
import os
from datetime import datetime
import logger_hander
from google_sheet_handler import Google_sheet_handler

class Find_State_Location:

    def __init__(self):
        self.Global_Dict = {}
        self.Global_Dict_keys_lower = {}
        self.Global_Dict_path = 'dataset/Area_Location.csv'
        self.Generate_Gobal_Dict()

    def Generate_Gobal_Dict(self):
        df = pd.read_csv(self.Global_Dict_path)
        for col in df.columns:
            State_Key_Name, State_Key_Value = self.preprocess_columns(df, col)
            self.Global_Dict[State_Key_Name] = State_Key_Value
        Global_Dict_keys = self.Global_Dict.keys()
        self.Global_Dict_keys_lower = [key.lower() for key in Global_Dict_keys]
        self.Global_Dict_keys_lower = [key.replace('_', ' ') for key in self.Global_Dict_keys_lower]
        print("Global Dict get Created")

    def fetch_data(self, google_sheet, start_date, end_date):
        list_of_records = google_sheet.get_all_records()
        Email_id_list = []
        Name_list = []
        MobileNumber_list = []
        University_list = []
        FinalUniversity_list = []
        CurrentLocation_list = []
        PermanentLocation_list = []
        Date_List = []
        Timestamp_list = []
        for records in list_of_records:
            if (records.get('Timestamp').split(' ')[0] >= start_date) & (records.get('Timestamp').split(' ')[0] <= end_date):
                Timestamp_list.append(records.get('Timestamp'))
                Date_List.append(records.get('Timestamp').split(' ')[0])
                Email_id_list.append(records.get('Email Address'))
                Name_list.append(records.get('Name'))
                MobileNumber_list.append(records.get('Mobile Number'))
                University_list.append(records.get('University'))
                FinalUniversity_list.append(records.get('College and University'))
                CurrentLocation_list.append(records.get('Your current location?'))
                PermanentLocation_list.append(records.get('Permanent Residence Location'))
        logger.info("Data fetched from existing sheet Successfully..!")
        return Timestamp_list, Date_List, Email_id_list, Name_list, MobileNumber_list, University_list, FinalUniversity_list, CurrentLocation_list, PermanentLocation_list

    def check_cell_name_valid_or_not(self, sheet, List_cell_name):
        return Google_sheet_handler.find_cell(self, sheet, List_cell_name)

    def extract_data_and_return_dataframe(self, sheet,start_date,end_date):
        List_of_cell_name = ['Timestamp', 'Email Address', 'Name', 'Mobile Number', 'University','College and University','Your current location?','Permanent Residence Location']

        flag = self.check_cell_name_valid_or_not(sheet, List_of_cell_name)
        if flag:
            Timestamp_list, Date_List, Email_id_list, Name_list, MobileNumber_list, University_list, FinalUniversity_list, CurrentLocation_list, PermanentLocation_list = self.fetch_data(sheet, start_date, end_date)
            for itr, final_uni in enumerate(FinalUniversity_list):
                final_uni = final_uni.lower()
                if ('na' in final_uni) or ('no' in final_uni) or ('nill' in final_uni) or ('n/a' in final_uni):
                    FinalUniversity_list[itr] = ''
                if len(FinalUniversity_list[itr]) != 0:
                    University_list[itr] = final_uni

            # dict = {'Timestamp':Timestamp_list, 'Date': Date_List, 'Email Address': Email_id_list, 'Name': Name_list, 'Mobile Number': MobileNumber_list,
            #         'University': University_list, 'Final University Name': FinalUniversity_list,
            #         'Current Location': CurrentLocation_list,
            #         'Permanent Address': PermanentLocation_list}
            # State_dataframe = pd.DataFrame(dict)
            return Timestamp_list, Date_List, Email_id_list, Name_list, MobileNumber_list, University_list, CurrentLocation_list, PermanentLocation_list
            # return State_dataframe

    def preprocess_columns(self, Dataframe, column_name):
        Dataframe[column_name] = Dataframe[column_name].str.lower()
        State_Key_Name = column_name.split('_SUB-DISTRICT')[0]
        State_Key_Value = Dataframe[column_name].dropna().unique().tolist()
        return State_Key_Name, State_Key_Value

    def get_state_name_from_dict(self,Search_State_name):
        for State_name_key, State_name_values in self.Global_Dict.items():
            for State_list_value in State_name_values:
                if Search_State_name == State_list_value:
                    return State_name_key
        return "State_name_key doesn't exist"

    def preprocessTestLocation(self,test_df,column_name):
        test_df[column_name] = test_df[column_name].str.lower()
        test_df[column_name] = test_df[column_name].str.replace('[#,@,&,(,),.,/,;]', ' ').dropna()
        test_df[column_name] = test_df[column_name].str.replace(',', ' ')
        # return test_location

    def get_State_Name_From_Test_Location(self,test_location):
        state_loc_list = []
        test_location = str(test_location).strip()
        if test_location in self.Global_Dict_keys_lower:
            state_name = self.get_state_name_from_dict(test_location)
            return state_name
        for token in test_location.split():
            token = ''.join(e.lower() for e in token if e.isalnum())
            token = token.strip()
            state = self.get_state_name_from_dict(token)
            state_loc_list.append(state)
        return ' '.join(state_loc_list)

    def final_output(self,test_location_df, test_column_name):
        count = 0
        state_name_list = []
        State_df = []

        for _, item in test_location_df.iterrows():
            state_names = self.get_State_Name_From_Test_Location(item[test_column_name])
            if "State_name_key doesn't exist" in state_names:
                state_names = state_names.replace("State_name_key doesn't exist", '').strip()
                for state_name in state_names.split():
                    if state_name not in state_name_list:
                        state_name_list.append(state_name)
                if len(state_name_list) > 0:
                    state_name = state_name_list[-1]
                    State_df.append(state_name.replace('_',' '))
                    # print(item[test_column_name], '*********', state_name)
                else:
                    # print("no state found...", item[test_column_name])
                    State_df.append("State_name_key doesn't exist")
                    count = count + 1
                state_name_list = []
            else:
                if len(state_names) > 1:
                    # print(state_names," ********")
                    state_name_value = state_names.split()[-1]
                    State_df.append(state_name_value.replace('_',' '))
                    # print(item[test_column_name], '*********', state_name_value)
                else:
                    State_df.append(state_names.replace('_',' '))
        test_column_name = test_column_name + ' State'
        # test_location_df[test_column_name] = State_df
        print("---------Count of State Not found Records...", count)
        return State_df, test_column_name

    def get_date(self):
        start_date = '10/30/2020'
        end_date = '10/31/2020'
        Timestamp_list, Date_List, Email_id_list, Name_list, MobileNumber_list, University_list, CurrentLocation_list, PermanentLocation_list = Find_State_Obj.extract_data_and_return_dataframe(
            sheet, start_date, end_date)
        dict = {'Timestamp': Timestamp_list, 'Date': Date_List, 'Email Address': Email_id_list, 'Name': Name_list,
                'Mobile Number': MobileNumber_list,
                'Final University Name': University_list,
                'Current Location': CurrentLocation_list,
                'Permanent Address': PermanentLocation_list}
        State_dataframe = pd.DataFrame(dict)
        Find_State_Obj.preprocessTestLocation(State_dataframe, 'Current Location')
        Current_State_df, Current_test_column_name = Find_State_Obj.final_output(State_dataframe, 'Current Location')
        Find_State_Obj.preprocessTestLocation(State_dataframe, 'Permanent Address')
        Permanent_State_df, Permanent_test_column_name = Find_State_Obj.final_output(State_dataframe,
                                                                                     'Permanent Address')
        dict = {'Timestamp': Timestamp_list, 'Email Address': Email_id_list, 'Name': Name_list,
                'Mobile Number': MobileNumber_list,
                'Final University Name': University_list,
                'University State': '',
                'Current Location': CurrentLocation_list,
                Current_test_column_name: Current_State_df,
                'Permanent Address': PermanentLocation_list,
                Permanent_test_column_name: Permanent_State_df}
        final_df = pd.DataFrame(dict)
        return final_df


if __name__ == "__main__":
    Find_State_Obj = Find_State_Location()
    sheet_handler = Google_sheet_handler()
    logger = logger_hander.set_logger()
    sheet = sheet_handler.call_sheet("CodInClub Student Information_Till_Oct_2020", "Learners_Data")
    if sheet != 'WorksheetNotFound':
        final_df = Find_State_Obj.get_date()
        df_list_value = final_df.values.tolist()
        Output_sheet = sheet_handler.call_sheet("CodInClub Student Information_Till_Oct_2020", "Copy of Processed_Data")
        if Output_sheet != 'WorksheetNotFound':
            output = sheet_handler.save_output_into_sheet(Output_sheet, df_list_value)
            logger.info(" Sheet Updated Successfully...!!!") if (output == True) else logger.error(
                " Something went wrong while Updating sheet ")