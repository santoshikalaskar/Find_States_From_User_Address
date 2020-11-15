import pandas as pd
import requests
from datetime import datetime
from lxml import html
import re
import logger_hander
from google_sheet_handler import Google_sheet_handler

class Find_State_Location:

    def __init__(self):
        """
            initialize Global Location Dictionary
        """
        self.Global_Dict = {}
        self.Global_Dict_keys_lower = {}
        self.Global_Dict_path = 'dataset/Area_Location.csv'
        self.generate_Gobal_Dict()

    def preProcess_Columns(self, Dataframe, column_name):
        """
            This function will preprocess dataframe to generate global Dictionary
            :param: Dataframe: Dictionary value Dataframe, column_name: column_name of Dataframe
            :return: state column name & their value
        """
        Dataframe[column_name] = Dataframe[column_name].str.lower()
        State_Key_Name = column_name.split('_SUB-DISTRICT')[0]
        State_Key_Value = Dataframe[column_name].dropna().unique().tolist()
        return State_Key_Name, State_Key_Value

    def generate_Gobal_Dict(self):
        """
            This function will Generate Global Dictionary required for finding State
        """
        df = pd.read_csv(self.Global_Dict_path)
        for col in df.columns:
            State_Key_Name, State_Key_Value = self.preProcess_Columns(df, col)
            self.Global_Dict[State_Key_Name] = State_Key_Value
        Global_Dict_keys = self.Global_Dict.keys()
        self.Global_Dict_keys_lower = [key.lower() for key in Global_Dict_keys]
        self.Global_Dict_keys_lower = [key.replace('_', ' ') for key in self.Global_Dict_keys_lower]
        logger.info(" Global Dictionary got Created...!")

    def fetch_Data_From_Sheet(self, google_sheet, start_date, end_date):
        """
            This function will Fetch data of specific date from google sheet & return into list.
            :param: google_sheet: Original google_sheet, start_date: date, end_date: date
            :return: fetched data columns in list
        """
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
        start_date = datetime.strptime(start_date, '%m/%d/%Y').date()
        end_date = datetime.strptime(end_date,'%m/%d/%Y').date()
        for records in list_of_records:
            timestamp_date = datetime.strptime(records.get('Timestamp').split(' ')[0],'%m/%d/%Y').date()
            if (timestamp_date >= start_date) & (timestamp_date <= end_date):
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

    def check_Cell_Name_Valid_or_Not(self, sheet, List_cell_name):
        """
            This function will find column name is right or wrong
            :param: sheet: google sheet instance, List_cell_name: list of cell name
            :return: True or False
        """
        return Google_sheet_handler.find_cell(self, sheet, List_cell_name)

    def convert_Final_University(self, sheet,start_date,end_date):
        """
            This function will call Fetch data() and convert UG & PG to final university list
            :param: sheet: google sheet instance, start_date: date, end_date: date
            :return: list of fetched records
        """
        List_of_cell_name = ['Timestamp', 'Email Address', 'Name', 'Mobile Number', 'University','College and University','Your current location?','Permanent Residence Location']

        flag = self.check_Cell_Name_Valid_or_Not(sheet, List_of_cell_name)
        if flag:
            Timestamp_list, Date_List, Email_id_list, Name_list, MobileNumber_list, University_list, FinalUniversity_list, CurrentLocation_list, PermanentLocation_list = self.fetch_Data_From_Sheet(sheet, start_date, end_date)
            for itr, final_uni in enumerate(FinalUniversity_list):
                final_uni = final_uni.lower()
                if ('na' in final_uni) or ('no' in final_uni) or ('nill' in final_uni) or ('n/a' in final_uni) or ('yes' in final_uni):
                    FinalUniversity_list[itr] = ''
                if len(FinalUniversity_list[itr]) != 0:
                    University_list[itr] = final_uni

            return Timestamp_list, Date_List, Email_id_list, Name_list, MobileNumber_list, University_list, CurrentLocation_list, PermanentLocation_list

    def get_State_Name_From_Dict(self,search_State_name):
        """
            This function will compare search value with Dictionary value & return State name
            :param: search_State_name: search value
            :return: State name key or "State_not_found"
        """
        for state_name_key, state_name_values in self.Global_Dict.items():
            for state_list_value in state_name_values:
                if search_State_name == state_list_value:
                    return state_name_key
        return "State_not_found"

    def preProcess_Test_Location(self,test_df,column_name):
        """
            This function will remove special symbols & convert test df column into lower_Case
            :param: test_df: dataframe name of testing, column_name: column_name of that testing dataframe
            :return: cleaned dataframe
        """
        if len(test_df[column_name])>0:
            test_df[column_name] = test_df[column_name].str.lower()
            test_df[column_name] = test_df[column_name].str.replace('[#,@,&,(,),.,/,;,-]', ' ').dropna()
            test_df[column_name] = test_df[column_name].str.replace(',', ' ')

    def get_State_Name_From_Test_Location(self,test_location):
        """
            This function will split address into each word and find their respective state name using Dict
            :param: test_location: test Current Addr/ Permanant Addr
            :return: State name for each word
        """
        state_loc_list = []
        test_location = str(test_location).strip()
        if test_location in self.Global_Dict_keys_lower:
            state_name = self.get_State_Name_From_Dict(test_location)
            return state_name
        for token in test_location.split():
            token = ''.join(e.lower() for e in token if e.isalnum())
            token = token.strip()
            state = self.get_State_Name_From_Dict(token)
            state_loc_list.append(state)
        return ' '.join(state_loc_list)

    def find_State_From_Address(self,test_location_df, test_column_name):
        """
            This function is main controller to find State names from current/ Permenent addr
            :param: test_location_df: dataframe of location, test_column_name: column name of df (test Current Addr/ Permanant Addr)
            :return: dataframe , column_name
        """
        count = 0
        state_name_list = []
        State_df = []
        for _, item in test_location_df.iterrows():
            state_names = self.get_State_Name_From_Test_Location(item[test_column_name])
            if "State_not_found" in state_names:
                state_names = state_names.replace("State_not_found", '').strip()
                for state_name in state_names.split():
                    if state_name not in state_name_list:
                        state_name_list.append(state_name)
                if len(state_name_list) > 0:
                    state_name = state_name_list[-1]
                    State_df.append(state_name.replace('_',' '))
                else:
                    State_df.append("State_not_found")
                    count = count + 1
                state_name_list = []
            else:
                if len(state_names) > 1:
                    state_name_value = state_names.split()[-1]
                    State_df.append(state_name_value.replace('_',' '))
                else:
                    State_df.append(state_names.replace('_',' '))
        test_column_name = test_column_name + ' State'
        print("From ",test_column_name,"--",count," Records Not Found..!")
        return State_df, test_column_name

    def find_Univerity_State(self,final_university_name_list):
        """
            This function is main controller to find State names from University name (Scrapper code using buetifulsoup)
            :param: final_university_name_list: Test University name in list
            :return: list of state name of tested university
        """
        final_list =[]
        for uni_vals in final_university_name_list:
            if str(uni_vals) != 'nan':
                page = requests.get("https://www.google.com/search?q=" + str(uni_vals))
                tree = html.fromstring(page.content)
                state_location = tree.xpath("//div[contains(@class,'BNeawe tAd8D AP7Wnd')]/text()")
                try:
                    state_location = state_location.replace("\n", "")
                except:
                    state_location = tree.xpath("//span[contains(@class,'BNeawe tAd8D AP7Wnd')]/text()")
                try:
                    state_location = state_location[0]
                except:
                    state_location = state_location
                try:
                    string_check = re.compile('[,]')
                    if (string_check.search(state_location) == None):
                        if re.search('in', state_location):
                            State = state_location.split('in')[-1]
                            State = State.rsplit(' ', 1)[0]
                        else:
                            State = state_location.rsplit(' ', 1)[0]
                    else:
                        State = state_location.split(',')[-1]
                        State = State.rsplit(' ', 1)[0]

                    final_list.append(State.strip())
                except:
                    State = "State_not_found"
                    final_list.append(State)
            else:
                final_list.append('')
        return final_list

    def Main_Controller(self):
        """
            This function is main controller to find State names, it calls Location logic as well as University logic
            :param:
            :return: final_dataframe which save back to google sheet
        """
        start_date = '11/1/2020'
        end_date = '11/2/2020'
        Timestamp_list, Date_List, Email_id_list, Name_list, MobileNumber_list, University_list, CurrentLocation_list, PermanentLocation_list = \
            Find_State_Obj.convert_Final_University(sheet, start_date, end_date)
        dict = {'Timestamp': Timestamp_list,
                'Date': Date_List,
                'Email Address': Email_id_list,
                'Name': Name_list,
                'Mobile Number': MobileNumber_list,
                'Final University Name': University_list,
                'Current Location': CurrentLocation_list,
                'Permanent Address': PermanentLocation_list }
        State_dataframe = pd.DataFrame(dict)
        print("Total No of fetched Records: ",State_dataframe.shape)

        Find_State_Obj.preProcess_Test_Location(State_dataframe, 'Current Location')
        Current_State_df, Current_test_column_name = Find_State_Obj.find_State_From_Address(State_dataframe, 'Current Location')
        Find_State_Obj.preProcess_Test_Location(State_dataframe, 'Permanent Address')
        Permanent_State_df, Permanent_test_column_name = Find_State_Obj.find_State_From_Address(State_dataframe,'Permanent Address')
        logger.info("From Address find States Successfully..!")
        University_state_list = self.find_Univerity_State(University_list)
        logger.info("From University find States Successfully..!")
        dict = {'Timestamp': Timestamp_list,
                'Email Address': Email_id_list,
                'Name': Name_list,
                'Mobile Number': MobileNumber_list,
                'Final University Name': University_list,
                'University State': University_state_list,
                'Current Location': CurrentLocation_list,
                Current_test_column_name: Current_State_df,
                'Permanent Address': PermanentLocation_list,
                Permanent_test_column_name: Permanent_State_df}
        final_dataframe = pd.DataFrame(dict)
        return final_dataframe

if __name__ == "__main__":

    # Initialize instances
    sheet_handler = Google_sheet_handler()
    logger = logger_hander.set_logger()
    Find_State_Obj = Find_State_Location()

    # call google sheet which raw data present
    sheet = sheet_handler.call_sheet("CodInClub Student Information_Till_Oct_2020", "Learners_Data")
    if sheet != 'WorksheetNotFound':
        final_df = Find_State_Obj.Main_Controller()
        df_list_value = final_df.values.tolist()

        # call output sheet which result will be stored back
        Output_sheet = sheet_handler.call_sheet("CodInClub Student Information_Till_Oct_2020", "Copy_of_Processed_Data_1")
        if Output_sheet != 'WorksheetNotFound':
            output = sheet_handler.save_output_into_sheet(Output_sheet, df_list_value)
            logger.info(" Sheet Updated Successfully...!!!") if (output == True) else logger.error(" Something went wrong while Updating sheet ")


