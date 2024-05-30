### **function to filter the seven last days**
def file_list():
    """"create the list of source file"""
    import os
    file_list = os.listdir("france/data")
    return file_list

def seven_last_file_name(ourlist: list) -> list:
    """args : list of files containing our datas
    return : list of seven last days file name"""
    new_list = []
    for i in ourlist:
        i = i.split("_")[2].split("-")[0] # "_" : take the third position an with "-"" separated character take the firs position
        i = int(i)
        new_list.append(i)

    # tidy new_list elements
    new_list = sorted(new_list) # or list.sort()
    seven_last_days = new_list[(len(new_list))-7:] # filter the seven last days
    seven_last_file_list = [] # define list of the seven files we need
    for i in seven_last_days:
        for j in ourlist:
            if j.__contains__(f"{i}"):
                seven_last_file_list.append(j)
    del new_list, seven_last_days

    return seven_last_file_list


def concat_seven_last_days_function():
    """concatenate only the seven days datas
    """
    import os
    import pandas as pd
    #from days_filter import file_list, seven_last_file_name
    
    data_list = seven_last_file_name(file_list())
    directory_path = "france/data"
    dataframe = pd.DataFrame([])
    for element in data_list:
        path_to_file = os.path.join(directory_path, element)
        path_to_file.replace("\\", "/")
        data = pd.read_csv(path_to_file)
        dataframe = pd.concat([dataframe, data], ignore_index=True)
    
    return dataframe
